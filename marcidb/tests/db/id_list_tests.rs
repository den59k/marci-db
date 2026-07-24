use marcidb::{EncodeError, MarciDB, parse_insert, parse_update, try_parse_schema};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{delete_data, get_data, get_data_one, insert_data, update_data};

/// `@list` with a declared back-reference: `Gallery.images` owns the ordered id array,
/// `Image.galleries` reads through the reverse tree (`Image.galleries->Gallery`)
const GALLERY_SCHEMA: &str = "
    model Gallery {
        name      String
        images    Image[]    @list
    }

    model Image {
        url        String
        galleries  Gallery[]  @bind(Gallery.images)
    }
";

/// `@list` without a back-reference: the reverse tree is hidden (`Playlist.tracks<-Track`),
/// maintained only for delete integrity
const PLAYLIST_SCHEMA: &str = "
    model Playlist {
        name    String
        tracks  Track[]  @list
    }

    model Track {
        title   String
    }
";

#[test]
fn id_list_insert_and_query() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(GALLERY_SCHEMA, dir.path().to_str().unwrap());

    let img_a = insert_data(&db, "Image", json!({ "url": "a.png" }));
    let img_b = insert_data(&db, "Image", json!({ "url": "b.png" }));
    let img_c = insert_data(&db, "Image", json!({ "url": "c.png" }));

    // The order is deliberately NOT id order — that's the point of @list
    insert_data(&db, "Gallery", json!({ "name": "First", "images": [ img_c, img_a ] }));
    insert_data(&db, "Gallery", json!({ "name": "Second", "images": [ img_a, img_b, img_c ] }));
    insert_data(&db, "Gallery", json!({ "name": "Empty", "images": [] }));

    // The include preserves the array order
    {
        let resp = get_data(&db, "Gallery", json!({ "name": true, "images": { "url": true } }));
        assert_eq!(resp, json!([
            { "name": "First", "images": [{ "url": "c.png" }, { "url": "a.png" }] },
            { "name": "Second", "images": [{ "url": "a.png" }, { "url": "b.png" }, { "url": "c.png" }] },
            { "name": "Empty", "images": [] },
        ]));
    }

    // The declared back-reference reads through the reverse tree (result in owner-id order)
    {
        let resp = get_data(&db, "Image", json!({
            "url": true,
            "galleries": { "name": true },
            "$where": { "url": "b.png" }
        }));
        assert_eq!(resp, json!([
            { "url": "b.png", "galleries": [{ "name": "Second" }] },
        ]));
    }

    // $count reads the array header — no rows are touched
    {
        let resp = get_data(&db, "Gallery", json!({ "name": true, "images": { "$count": true } }));
        assert_eq!(resp, json!([
            { "name": "First", "images": { "count": 2 } },
            { "name": "Second", "images": { "count": 3 } },
            { "name": "Empty", "images": { "count": 0 } },
        ]));
    }

    // One reverse-tree entry per membership
    assert_eq!(db.count_dev("Image.galleries->Gallery"), 5);

    // $some / $none filters walk the array
    {
        let resp = get_data(&db, "Gallery", json!({
            "name": true,
            "$where": { "images": { "$some": { "url": "b.png" } } }
        }));
        assert_eq!(resp, json!([ { "name": "Second" } ]));

        let resp = get_data(&db, "Gallery", json!({
            "name": true,
            "$where": { "images": { "$none": { "url": "b.png" } } }
        }));
        assert_eq!(resp, json!([ { "name": "First" }, { "name": "Empty" } ]));
    }

    // $limit / $skip apply in array order
    {
        let resp = get_data_one(&db, "Gallery", json!({
            "images": { "url": true, "$limit": 1, "$skip": 1 },
            "$where": { "name": "Second" }
        }));
        assert_eq!(resp, json!({ "images": [{ "url": "b.png" }] }));
    }
}

#[test]
fn id_list_update_ops() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(GALLERY_SCHEMA, dir.path().to_str().unwrap());

    let img_a = insert_data(&db, "Image", json!({ "url": "a.png" }));
    let img_b = insert_data(&db, "Image", json!({ "url": "b.png" }));
    let img_c = insert_data(&db, "Image", json!({ "url": "c.png" }));

    let gallery = insert_data(&db, "Gallery", json!({ "name": "Main", "images": [ img_a, img_b ] }));
    let images_of = |db: &MarciDB| get_data_one(db, "Gallery", json!({ "images": { "url": true } }));

    // $set with the same members is a pure reorder: body rewrite, reverse tree untouched
    {
        update_data(&db, "Gallery", &gallery, json!({ "images": { "$set": [ img_b, img_a ] } }));
        assert_eq!(images_of(&db), json!({ "images": [{ "url": "b.png" }, { "url": "a.png" }] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 2);
    }

    // $connect appends at the end; connecting an already-present id is a no-op
    {
        update_data(&db, "Gallery", &gallery, json!({ "images": { "$connect": img_c } }));
        assert_eq!(images_of(&db), json!({ "images": [{ "url": "b.png" }, { "url": "a.png" }, { "url": "c.png" }] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 3);

        update_data(&db, "Gallery", &gallery, json!({ "images": { "$connect": img_c } }));
        assert_eq!(images_of(&db), json!({ "images": [{ "url": "b.png" }, { "url": "a.png" }, { "url": "c.png" }] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 3);
    }

    // $remove splices out and cleans the reverse tree
    {
        update_data(&db, "Gallery", &gallery, json!({ "images": { "$remove": img_a } }));
        assert_eq!(images_of(&db), json!({ "images": [{ "url": "b.png" }, { "url": "c.png" }] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 2);
    }

    // $set with different membership syncs the reverse tree both ways
    {
        update_data(&db, "Gallery", &gallery, json!({ "images": { "$set": [ img_a ] } }));
        assert_eq!(images_of(&db), json!({ "images": [{ "url": "a.png" }] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 1);

        let resp = get_data(&db, "Image", json!({ "url": true, "galleries": { "$count": true } }));
        assert_eq!(resp, json!([
            { "url": "a.png", "galleries": { "count": 1 } },
            { "url": "b.png", "galleries": { "count": 0 } },
            { "url": "c.png", "galleries": { "count": 0 } },
        ]));
    }

    // $set to empty clears the field entirely
    {
        update_data(&db, "Gallery", &gallery, json!({ "images": { "$set": [] } }));
        assert_eq!(images_of(&db), json!({ "images": [] }));
        assert_eq!(db.count_dev("Image.galleries->Gallery"), 0);
    }
}

#[test]
fn id_list_delete_target_with_declared_reverse() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(GALLERY_SCHEMA, dir.path().to_str().unwrap());

    let img_a = insert_data(&db, "Image", json!({ "url": "a.png" }));
    let img_b = insert_data(&db, "Image", json!({ "url": "b.png" }));
    let img_c = insert_data(&db, "Image", json!({ "url": "c.png" }));

    insert_data(&db, "Gallery", json!({ "name": "First", "images": [ img_c, img_a, img_b ] }));
    insert_data(&db, "Gallery", json!({ "name": "Second", "images": [ img_a ] }));

    // Deleting a referenced row splices it out of every owner's array, preserving the rest of the order
    delete_data(&db, "Image", img_a);

    let resp = get_data(&db, "Gallery", json!({ "name": true, "images": { "url": true } }));
    assert_eq!(resp, json!([
        { "name": "First", "images": [{ "url": "c.png" }, { "url": "b.png" }] },
        { "name": "Second", "images": [] },
    ]));
    assert_eq!(db.count_dev("Image.galleries->Gallery"), 2);

    // $count agrees with the spliced arrays (it reads the stored header, not the index)
    let resp = get_data(&db, "Gallery", json!({ "name": true, "images": { "$count": true } }));
    assert_eq!(resp, json!([
        { "name": "First", "images": { "count": 2 } },
        { "name": "Second", "images": { "count": 0 } },
    ]));
}

#[test]
fn id_list_delete_owner_with_declared_reverse() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(GALLERY_SCHEMA, dir.path().to_str().unwrap());

    let img_a = insert_data(&db, "Image", json!({ "url": "a.png" }));
    let img_b = insert_data(&db, "Image", json!({ "url": "b.png" }));

    let first = insert_data(&db, "Gallery", json!({ "name": "First", "images": [ img_a, img_b ] }));
    insert_data(&db, "Gallery", json!({ "name": "Second", "images": [ img_a ] }));

    // Deleting an owner cleans its reverse-tree entries — the back-reference must not see a ghost
    delete_data(&db, "Gallery", first);

    let resp = get_data(&db, "Image", json!({ "url": true, "galleries": { "name": true } }));
    assert_eq!(resp, json!([
        { "url": "a.png", "galleries": [{ "name": "Second" }] },
        { "url": "b.png", "galleries": [] },
    ]));
    assert_eq!(db.count_dev("Image.galleries->Gallery"), 1);
}

#[test]
fn id_list_hidden_reverse_tree() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db: MarciDB = MarciDB::new(PLAYLIST_SCHEMA, &path);

        let t1 = insert_data(&db, "Track", json!({ "title": "One" }));
        let t2 = insert_data(&db, "Track", json!({ "title": "Two" }));
        let t3 = insert_data(&db, "Track", json!({ "title": "Three" }));

        let morning = insert_data(&db, "Playlist", json!({ "name": "Morning", "tracks": [ t3, t1 ] }));
        insert_data(&db, "Playlist", json!({ "name": "Evening", "tracks": [ t1, t2 ] }));

        assert_eq!(db.count_dev("Playlist.tracks<-Track"), 4);

        // Deleting a track splices it from every playlist via the hidden reverse tree
        delete_data(&db, "Track", t1);
        let resp = get_data(&db, "Playlist", json!({ "name": true, "tracks": { "title": true } }));
        assert_eq!(resp, json!([
            { "name": "Morning", "tracks": [{ "title": "Three" }] },
            { "name": "Evening", "tracks": [{ "title": "Two" }] },
        ]));
        assert_eq!(db.count_dev("Playlist.tracks<-Track"), 2);

        // Deleting an owner cleans its hidden reverse-tree entries
        delete_data(&db, "Playlist", morning);
        assert_eq!(db.count_dev("Playlist.tracks<-Track"), 1);
    }

    // Reopen from disk: the @list binding is reconstructed from the stored snapshot
    {
        let db = MarciDB::open(&path);
        let resp = get_data(&db, "Playlist", json!({ "name": true, "tracks": { "title": true } }));
        assert_eq!(resp, json!([
            { "name": "Evening", "tracks": [{ "title": "Two" }] },
        ]));
    }
}

#[test]
fn id_list_rejects_invalid_input() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(GALLERY_SCHEMA, dir.path().to_str().unwrap());

    let img_a = insert_data(&db, "Image", json!({ "url": "a.png" }));
    let gallery_model = db.get_model("Gallery").unwrap();
    let image_model = db.get_model("Image").unwrap();

    // The same id twice in one array
    let dup = parse_insert(&db.schema, gallery_model, &json!({ "name": "Dup", "images": [ img_a, img_a ] }));
    assert!(matches!(dup, Err(EncodeError::DuplicateListId(_))), "got {:?}", dup);

    let dup = parse_update(&db.schema, gallery_model, &json!({ "images": { "$set": [ img_a, img_a ] } }));
    assert!(matches!(dup, Err(EncodeError::DuplicateListId(_))), "got {:?}", dup);

    // Membership can only be changed through the @list side — not from the back-reference
    let ins = parse_insert(&db.schema, image_model, &json!({ "url": "x.png", "galleries": [ { "id": 0 } ] }));
    assert!(matches!(ins, Err(EncodeError::MutateViaListSide(_, _))), "got {:?}", ins);

    let upd = parse_update(&db.schema, image_model, &json!({ "galleries": { "$connect": { "id": 0 } } }));
    assert!(matches!(upd, Err(EncodeError::MutateViaListSide(_, _))), "got {:?}", upd);
}

#[test]
fn id_list_schema_validation() {
    // @list needs a fixed-size target id — a String @id can't be split out of the array
    let string_id = try_parse_schema("
        model Gallery {
            name    String
            images  Image[]  @list
        }
        model Image {
            id      String   @id
        }
    ");
    assert!(string_id.is_err(), "string-id target must be rejected");

    // A fixed-size non-numeric id (uuid) is fine
    let uuid_id = try_parse_schema("
        model Gallery {
            name    String
            images  Image[]  @list
        }
        model Image {
            id      Byte[16]  @id  @format(uuid)
        }
    ");
    assert!(uuid_id.is_ok(), "fixed-size uuid target must be accepted: {:?}", uuid_id.err());

    // @list on a struct (owned) list — the children have no standalone ids to store
    let on_struct = try_parse_schema("
        model Project {
            name   String
            users  UserRole[]  @list
        }
        struct UserRole {
            role   String
        }
    ");
    assert!(on_struct.is_err(), "@list on a struct list must be rejected");

    // @list on a non-relation field
    let on_primitive = try_parse_schema("
        model User {
            tags  String[]  @list
        }
    ");
    assert!(on_primitive.is_err(), "@list on a primitive list must be rejected");

    // The bound partner of a @list field must be a plain (virtual) relation list
    let both_sides = try_parse_schema("
        model Gallery {
            name    String
            images  Image[]  @list
        }
        model Image {
            url        String
            galleries  Gallery[]  @list  @bind(Gallery.images)
        }
    ");
    assert!(both_sides.is_err(), "@list on both sides must be rejected");
}
