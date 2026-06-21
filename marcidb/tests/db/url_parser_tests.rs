// tests/url_parser_tests.rs
//
// Покрывает url_parser/url_parsers.rs (14.58% → целевое 80%+)
// и url_parser/mod.rs (88.94% → закрываем оставшиеся ветки)
//
// Запускать вместе с остальными тестами: cargo test url_parser

use marcidb::{parse_id, parse_id_from_url, parse_schema};
use serde_json::json;

// ─────────────────────────────────────────────────────────────────────────────
// Вспомогательная схема, которую используют большинство тестов
// ─────────────────────────────────────────────────────────────────────────────
fn make_schema() -> marcidb::Schema {
    parse_schema("
        model User {
            name        String
            age         UInt
        }

        model Chat {
            uuid        String      @id
        }

        model Product {
            sku         Int         @id
        }

        model Metric {
            score       Float       @id
        }

        model Measurement {
            value       Double      @id
        }

        model Flag {
            active      Boolean     @id
        }

        model ChatUser {
            chat        Chat        @id
            user        User        @id
        }
    ")
}

// ─────────────────────────────────────────────────────────────────────────────
// UInt (User) — базовое числовое поле
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_uint_bare() {
    let schema = make_schema();
    let user = &schema.models[0];
    let from_url = parse_id_from_url(&schema, user, "7").unwrap();
    let from_json = parse_id(&schema, user, &json!({ "id": 7 })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_uint_key_value() {
    let schema = make_schema();
    let user = &schema.models[0];
    let from_url = parse_id_from_url(&schema, user, "id=42").unwrap();
    let from_json = parse_id(&schema, user, &json!({ "id": 42 })).unwrap();
    assert_eq!(from_url, from_json);
}

// ─────────────────────────────────────────────────────────────────────────────
// String (Chat) — переменная длина
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_string_bare() {
    let schema = make_schema();
    let chat = &schema.models[1];
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let from_url = parse_id_from_url(&schema, chat, uuid).unwrap();
    let from_json = parse_id(&schema, chat, &json!({ "uuid": uuid })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_string_key_value() {
    let schema = make_schema();
    let chat = &schema.models[1];
    let uuid = "hello-world";
    let from_url = parse_id_from_url(&schema, chat, &format!("uuid={uuid}")).unwrap();
    let from_json = parse_id(&schema, chat, &json!({ "uuid": uuid })).unwrap();
    assert_eq!(from_url, from_json);
}

// ─────────────────────────────────────────────────────────────────────────────
// Int (Product) — encode_primitive_value для Int64
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_int_bare() {
    let schema = make_schema();
    let product = &schema.models[2];
    let from_url = parse_id_from_url(&schema, product, "-10").unwrap();
    let from_json = parse_id(&schema, product, &json!({ "sku": -10 })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_int_key_value() {
    let schema = make_schema();
    let product = &schema.models[2];
    let from_url = parse_id_from_url(&schema, product, "sku=999").unwrap();
    let from_json = parse_id(&schema, product, &json!({ "sku": 999 })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_int_invalid_returns_error() {
    let schema = make_schema();
    let product = &schema.models[2];
    let result = parse_id_from_url(&schema, product, "not_a_number");
    assert!(result.is_err(), "Ожидалась ошибка при невалидном int64");
}

// ─────────────────────────────────────────────────────────────────────────────
// Float (Metric)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_float_bare() {
    let schema = make_schema();
    let metric = &schema.models[3];
    let from_url = parse_id_from_url(&schema, metric, "3.14").unwrap();
    let from_json = parse_id(&schema, metric, &json!({ "score": 3.14 })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_float_invalid_returns_error() {
    let schema = make_schema();
    let metric = &schema.models[3];
    let result = parse_id_from_url(&schema, metric, "abc");
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// Double (Measurement)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_double_bare() {
    let schema = make_schema();
    let meas = &schema.models[4];
    let from_url = parse_id_from_url(&schema, meas, "2.718281828").unwrap();
    let from_json = parse_id(&schema, meas, &json!({ "value": 2.718281828 })).unwrap();
    assert_eq!(from_url, from_json);
}

// ─────────────────────────────────────────────────────────────────────────────
// Bool (Flag) — true/false/1/0
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_bool_true() {
    let schema = make_schema();
    let flag = &schema.models[5];
    let from_url = parse_id_from_url(&schema, flag, "true").unwrap();
    let from_json = parse_id(&schema, flag, &json!({ "active": true })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_bool_false() {
    let schema = make_schema();
    let flag = &schema.models[5];
    let from_url = parse_id_from_url(&schema, flag, "false").unwrap();
    let from_json = parse_id(&schema, flag, &json!({ "active": false })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_bool_one() {
    let schema = make_schema();
    let flag = &schema.models[5];
    let from_url_1 = parse_id_from_url(&schema, flag, "1").unwrap();
    let from_url_t = parse_id_from_url(&schema, flag, "true").unwrap();
    assert_eq!(from_url_1, from_url_t);
}

#[test]
fn url_parse_bool_zero() {
    let schema = make_schema();
    let flag = &schema.models[5];
    let from_url_0 = parse_id_from_url(&schema, flag, "0").unwrap();
    let from_url_f = parse_id_from_url(&schema, flag, "false").unwrap();
    assert_eq!(from_url_0, from_url_f);
}

#[test]
fn url_parse_bool_invalid_returns_error() {
    let schema = make_schema();
    let flag = &schema.models[5];
    let result = parse_id_from_url(&schema, flag, "maybe");
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// Составной ключ (ChatUser) — несколько полей через &
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_composite_key() {
    let schema = make_schema();
    let chat_user = &schema.models[6];
    let uuid = "36116d39-8376-4430-bb2e-d725923ab645";

    let from_url = parse_id_from_url(
        &schema,
        chat_user,
        &format!("chat.uuid={uuid}&user.id=5"),
    )
    .unwrap();

    let from_json = parse_id(
        &schema,
        chat_user,
        &json!({
            "chat": { "uuid": uuid },
            "user": { "id": 5 }
        }),
    )
    .unwrap();

    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_composite_missing_field_returns_error() {
    let schema = make_schema();
    let chat_user = &schema.models[6];
    // Передаём только chat, пропускаем user.id
    let result = parse_id_from_url(&schema, chat_user, "chat.uuid=some-uuid");
    assert!(result.is_err(), "Ожидалась ошибка MissingIdField");
}

// ─────────────────────────────────────────────────────────────────────────────
// Ошибка WrongSyntax — часть без знака '='
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_wrong_syntax_returns_error() {
    let schema = make_schema();
    let chat_user = &schema.models[6];
    // Нет знака '=' в одной из пар
    let result = parse_id_from_url(&schema, chat_user, "chat.uuid=abc&user_id_no_eq");
    assert!(result.is_err(), "Ожидалась ошибка WrongSyntax");
}

// ─────────────────────────────────────────────────────────────────────────────
// FullIdExpected — модель с несколькими @id полями, но передан одиночный bare
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_full_id_expected_error() {
    let schema = make_schema();
    let chat_user = &schema.models[6]; // ChatUser: 2 ключа
    // Передаём bare-значение (без '=') — для составного ключа это недопустимо
    let result = parse_id_from_url(&schema, chat_user, "single_value");
    assert!(result.is_err(), "Ожидалась ошибка FullIdExpected");
}

// ─────────────────────────────────────────────────────────────────────────────
// DateTime через URL
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_datetime_epoch() {
    let schema = parse_schema("
        model Event {
            ts   DateTime   @id
        }
    ");
    let event = &schema.models[0];

    // Передаём как epoch (i64)
    let from_url = parse_id_from_url(&schema, event, "1700000000000").unwrap();
    let from_json = parse_id(&schema, event, &json!({ "ts": 1700000000000_i64 })).unwrap();
    assert_eq!(from_url, from_json);
}

#[test]
fn url_parse_datetime_iso8601() {
    let schema = parse_schema("
        model Event {
            ts   DateTime   @id
        }
    ");
    let event = &schema.models[0];

    // Передаём как ISO-8601 строку
    let iso = "2023-11-14T22:13:20+00:00";
    let from_url = parse_id_from_url(&schema, event, iso).unwrap();
    // Должна получиться непустая последовательность байт
    assert!(!from_url.is_empty());
}

#[test]
fn url_parse_datetime_invalid_returns_error() {
    let schema = parse_schema("
        model Event {
            ts   DateTime   @id
        }
    ");
    let event = &schema.models[0];
    let result = parse_id_from_url(&schema, event, "not-a-date");
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// UInt — недопустимое значение
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn url_parse_uint_invalid_returns_error() {
    let schema = make_schema();
    let user = &schema.models[0];
    let result = parse_id_from_url(&schema, user, "not_uint");
    assert!(result.is_err(), "Ожидалась ошибка TypeMismatch для UInt");
}
