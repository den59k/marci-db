//! Delete + cascade for relations expressed through composite primary keys.
//! Covers: composite single-prefix join keys, composite autoincrement-tail keys, multi-level
//! cascade, cross-reference preservation, and the schema/migration guards for the two cases that
//! genuinely cannot be applied (binding change, @index on a relation).

use marcidb::{MarciDB, MigrateError, parse_schema, try_parse_schema};
use marcidb_schema::{diff, reconcile};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{delete_data, get_data, insert_data, try_delete};

fn count(db: &MarciDB, model: &str) -> u64 {
    db.count(db.get_model(model).unwrap()).unwrap()
}

// ─────────────────────────── Symptom 1: composite single-prefix join key ───────────────────────────

const JOIN_SCHEMA: &str = "
model User {
    login String @unique
    chats ChatUser[] @bind(ChatUser.user)
}
model Chat {
    name String?
    users ChatUser[] @bind(ChatUser.chat)
}
model ChatUser {
    chat Chat @id @onDelete(Cascade)
    user User @id @onDelete(Cascade)
}
";

/// Deleting the model that is the @current_id prefix of a join table (Chat) must cascade its owned
/// ChatUser rows — even though each ChatUser carries its own dependency (the User.chats index) — while
/// the referenced User rows are left untouched. An empty parent must also be deletable.
#[test]
fn composite_join_key_cascade_delete() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(JOIN_SCHEMA, dir.path().to_str().unwrap());

    let alice = insert_data(&db, "User", json!({ "login": "alice" }));
    let bob = insert_data(&db, "User", json!({ "login": "bob" }));
    let empty = insert_data(&db, "Chat", json!({ "name": "empty" }));
    let full = insert_data(&db, "Chat", json!({ "name": "full" }));
    insert_data(&db, "ChatUser", json!({ "chat": full, "user": alice }));
    insert_data(&db, "ChatUser", json!({ "chat": full, "user": bob }));

    assert_eq!(count(&db, "ChatUser"), 2);

    // Empty chat: deletable (the failure was structural — it errored even with zero members)
    delete_data(&db, "Chat", empty);
    assert_eq!(count(&db, "Chat"), 1);
    assert_eq!(count(&db, "ChatUser"), 2);

    // Full chat: owned ChatUser rows removed, Users preserved
    delete_data(&db, "Chat", full);
    assert_eq!(count(&db, "ChatUser"), 0);
    assert_eq!(count(&db, "User"), 2, "referenced users must NOT be cascade-deleted");

    // Regression: deleting from the index_tree side still works
    delete_data(&db, "User", alice);
    assert_eq!(count(&db, "User"), 1);
}

/// Deleting one Chat removes only that chat's owned memberships; another chat's memberships and all
/// referenced Users survive (cascade must not over-reach across the join table or into User).
#[test]
fn composite_join_key_cross_reference_preserved() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(JOIN_SCHEMA, dir.path().to_str().unwrap());

    let alice = insert_data(&db, "User", json!({ "login": "alice" }));
    let bob = insert_data(&db, "User", json!({ "login": "bob" }));
    let chat_a = insert_data(&db, "Chat", json!({ "name": "A" }));
    let chat_b = insert_data(&db, "Chat", json!({ "name": "B" }));
    insert_data(&db, "ChatUser", json!({ "chat": chat_a, "user": alice }));
    insert_data(&db, "ChatUser", json!({ "chat": chat_a, "user": bob }));
    insert_data(&db, "ChatUser", json!({ "chat": chat_b, "user": alice }));

    assert_eq!(count(&db, "ChatUser"), 3);

    // Delete chat A: only A's two memberships go; chat B's membership survives; no users deleted
    delete_data(&db, "Chat", chat_a);
    assert_eq!(count(&db, "ChatUser"), 1);
    assert_eq!(count(&db, "User"), 2);
    assert_eq!(count(&db, "Chat"), 1);

    // the surviving membership is alice's in chat B (the owned collection is read by key prefix)
    let chats = get_data(&db, "Chat", json!({ "name": true, "users": { "user": { "login": true } } }));
    assert_eq!(chats, json!([{ "name": "B", "users": [{ "user": { "login": "alice" } }] }]));
}

// ─────────────────────────── Symptom 2: composite autoincrement-tail key ───────────────────────────

const MESSAGE_SCHEMA: &str = "
model Chat {
    messages Message[]
}
model Message {
    chat   Chat @id @onDelete(Cascade)
    id     UInt @id @default(autoincrement())
    author User
    text   String
}
model User {
    name String
}
";

/// A child keyed by `parent + autoincrement` (reached via index_tree with no forward binding) must be
/// deletable directly and via parent cascade; the author Users it merely references must survive.
#[test]
fn composite_tail_key_cascade_delete() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(MESSAGE_SCHEMA, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "alice" }));
    insert_data(&db, "Chat", json!({}));
    insert_data(&db, "Message", json!({ "chat": { "id": 1 }, "author": { "id": 1 }, "text": "one" }));
    insert_data(&db, "Message", json!({ "chat": { "id": 1 }, "author": { "id": 1 }, "text": "two" }));
    insert_data(&db, "Message", json!({ "chat": { "id": 1 }, "author": { "id": 1 }, "text": "three" }));
    assert_eq!(count(&db, "Message"), 3);

    // delete a single child by its composite id
    delete_data(&db, "Message", json!({ "chat": { "id": 1 }, "id": 1 }));
    assert_eq!(count(&db, "Message"), 2);

    // delete the parent: cascades to the remaining messages, authors remain
    delete_data(&db, "Chat", json!({ "id": 1 }));
    assert_eq!(count(&db, "Message"), 0);
    assert_eq!(count(&db, "User"), 1, "author users must remain");
}

// ─────────────────────────── multi-level cascade (nested composite keys) ───────────────────────────

const MULTI_SCHEMA: &str = "
model Org {
    name String
    channels Channel[] @bind(Channel.org)
}
model Channel {
    org Org @id @onDelete(Cascade)
    id  UInt @id @default(autoincrement())
    posts Post[] @bind(Post.channel)
}
model Post {
    channel Channel @id @onDelete(Cascade)
    id UInt @id @default(autoincrement())
    text String
}
";

/// Deleting the root cascades through a chain of composite-key children (Org → Channel → Post).
/// A sibling Org and its subtree are untouched.
#[test]
fn multilevel_composite_cascade_delete() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(MULTI_SCHEMA, dir.path().to_str().unwrap());

    insert_data(&db, "Org", json!({ "name": "Acme" }));
    insert_data(&db, "Org", json!({ "name": "Globex" }));
    insert_data(&db, "Channel", json!({ "org": { "id": 1 } }));
    insert_data(&db, "Channel", json!({ "org": { "id": 2 } }));
    insert_data(&db, "Post", json!({ "channel": { "org": { "id": 1 }, "id": 1 }, "text": "a" }));
    insert_data(&db, "Post", json!({ "channel": { "org": { "id": 1 }, "id": 1 }, "text": "b" }));
    insert_data(&db, "Post", json!({ "channel": { "org": { "id": 2 }, "id": 2 }, "text": "c" }));

    assert_eq!(count(&db, "Channel"), 2);
    assert_eq!(count(&db, "Post"), 3);

    delete_data(&db, "Org", json!({ "id": 1 }));

    // Acme's whole subtree is gone; Globex's subtree survives
    assert_eq!(count(&db, "Org"), 1);
    assert_eq!(count(&db, "Channel"), 1);
    assert_eq!(count(&db, "Post"), 1);
    let posts = get_data(&db, "Post", json!({ "text": true }));
    assert_eq!(posts, json!([{ "text": "c" }]));
}

// ─────────────────────────── Symptom 4: @index on a relation field ───────────────────────────

/// A plain @index on a relation field is rejected at parse time with an actionable message — not a
/// confusing failure at migrate-apply time. (@unique on a relation, the one-to-one constraint, still works.)
#[test]
fn index_on_relation_field_is_rejected() {
    let schema = "
model Chat {
    messages Message[] @bind(Message.chat)
}
model Message {
    id   UInt @id @default(autoincrement())
    chat Chat @index @onDelete(Cascade)
    text String
}
";
    let err = try_parse_schema(schema).unwrap_err();
    assert!(err.0.contains("@index on relation"), "unexpected error: {}", err.0);

    // @unique on a relation (one-to-one) is still accepted
    assert!(try_parse_schema("model A {\n  b B @unique\n}\nmodel B {\n  a A @bind(A.b)\n}").is_ok());
}

// ─────────────────────────── Symptom 3: @bind flips the storage binding ───────────────────────────

/// Adding @bind to a composite-key relation flips its binding (index_tree → current_id), which moves
/// where the relation is stored. The migration generator must surface that explicitly instead of
/// silently producing no migration.
#[test]
fn relation_binding_change_is_rejected() {
    let old = parse_schema(MESSAGE_SCHEMA);
    let new_text = MESSAGE_SCHEMA.replace("messages Message[]", "messages Message[] @bind(Message.chat)");
    let mut new = parse_schema(&new_text);
    reconcile(&mut new, &old);

    match diff(&old, &new) {
        Err(MigrateError::UnsupportedBindingChange { entity, field, .. }) => {
            assert_eq!(entity, "Chat");
            assert_eq!(field, "messages");
        }
        other => panic!("expected UnsupportedBindingChange, got {:?}", other),
    }
}

/// A composite-key relation without `@onDelete(Cascade)` used to reach `SetNull` (the old blanket default
/// for a `field_value` relation) and panic with "Cannot set null on non-body values" — a key cannot be
/// nulled. A required relation now defaults to `Restrict`, so the same schema reports what is holding the
/// row instead of aborting the process.
#[test]
fn composite_key_without_cascade_restricts_instead_of_panicking() {
    const SCHEMA: &str = "
model User {
    login String @unique
    chats ChatUser[] @bind(ChatUser.user)
}
model Chat {
    name String?
    users ChatUser[] @bind(ChatUser.chat)
}
model ChatUser {
    chat Chat @id
    user User @id
}
";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());

    let alice = insert_data(&db, "User", json!({ "login": "alice" }));
    let chat = insert_data(&db, "Chat", json!({ "name": "full" }));
    insert_data(&db, "ChatUser", json!({ "chat": chat, "user": alice.clone() }));

    match try_delete(&db, "User", alice) {
        Err(marcidb::DeleteError::RestrictConstraints(field, _)) => assert_eq!(field, "ChatUser.user"),
        other => panic!("expected a Restrict rejection, got {:?}", other),
    }
    assert_eq!(count(&db, "User"), 1);
    assert_eq!(count(&db, "ChatUser"), 1);
}




/// The declared `@onDelete(Cascade)` on a key-located relation must actually fire from the referenced
/// side. It never did: a relation stored in the key wrote no reverse-index entry on insert, so
/// `User.chats->ChatUser` stayed empty and every policy that reads it found nothing to do. Deleting the
/// User reported success and left the membership row behind with a key pointing at a row that is gone.
#[test]
fn key_located_relation_cascades_from_the_referenced_side() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(JOIN_SCHEMA, dir.path().to_str().unwrap());

    let alice = insert_data(&db, "User", json!({ "login": "alice" }));
    let bob = insert_data(&db, "User", json!({ "login": "bob" }));
    let chat = insert_data(&db, "Chat", json!({ "name": "A" }));
    insert_data(&db, "ChatUser", json!({ "chat": chat.clone(), "user": alice.clone() }));
    insert_data(&db, "ChatUser", json!({ "chat": chat, "user": bob }));

    // The reverse index of a key-located relation is maintained like any other back-reference
    assert_eq!(db.count_dev("User.chats->ChatUser"), 2);
    assert_eq!(count(&db, "ChatUser"), 2);

    delete_data(&db, "User", alice);

    // Alice's membership is gone — not orphaned — and Bob's is untouched
    assert_eq!(count(&db, "ChatUser"), 1, "the cascade must remove the membership, not orphan it");
    assert_eq!(db.count_dev("User.chats->ChatUser"), 1);
    assert_eq!(
        get_data(&db, "ChatUser", json!({ "user": { "login": true }, "chat": { "name": true } })),
        json!([{ "user": { "login": "bob" }, "chat": { "name": "A" } }])
    );
}
