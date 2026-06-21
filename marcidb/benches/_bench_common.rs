// =============================================================================
//  benchmark.rs — AS-IS (Prisma + PostgreSQL) vs TO-BE (MarciDB) — ЧЕСТНОЕ СРАВНЕНИЕ
//
//  АРХИТЕКТУРА СРАВНЕНИЯ:
//  ─────────────────────────────────────────────────────────────────────────────
//  AS-IS: бенчмарк → PrismaEngineClient → IPC → Prisma QE → TCP → PostgreSQL
//  TO-BE: бенчмарк → MarciTcpClient → TCP/MDWP → MarciDB-сервер → CanopyDB (диск)
//
//  Обе СУБД работают как серверные процессы с сетевым транспортом.
//  Бенчмарк подключается к каждой через TCP и отправляет запросы по протоколу:
//    Prisma: Prisma Query Engine IPC → PostgreSQL wire protocol
//    MarciDB: MDWP бинарный протокол (op_code + u32 len + payload)
//
//  Для каждой операции MarciDB полный путь включает:
//    1. JSON-сериализацию запроса (serde_json::to_vec)
//    2. Упаковку в MDWP-фрейм
//    3. Отправку по TCP (keep-alive соединение, без переустановки)
//    4. Разбор бинарного ответа от сервера
//    5. Возврат JSON-байт клиенту
//
//  АРХИТЕКТУРНЫЕ ПРЕИМУЩЕСТВА TO-BE (теперь видны без транспортного шума):
//    [P1] Разрежённые таблицы: Place в PostgreSQL имеет 9 NULL-столбцов на запись.
//         MarciDB хранит только поля актуального варианта enum.
//    [P2] Int[] без FK: MarciDB использует типизированные RefList с целостностью.
//    [P3] events Json: MarciDB хранит типизированный Events[] с валидацией.
//
//  ПОДГОТОВКА К ЗАПУСКУ:
//  ─────────────────────────────────────────────────────────────────────────────
//  1. Обновите generator в schema_trimmed.prisma:
//       generator client {
//         provider = "prisma-client-rust"
//         output   = "../src/prisma.rs"
//       }
//  2. cargo prisma generate
//  3. docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16
//  4. cargo prisma db push
//  5. Запустите MarciDB-сервер (в отдельном терминале):
//       cargo run --release --bin marcidb-server
//     (сервер слушает на 127.0.0.1:3000, данные в ./data)
//  6. cargo bench --bench benchmark
//
//  ВАЖНО: перед каждым запуском бенчмарка удаляйте ./data и перезапускайте
//  MarciDB-сервер, чтобы данные не накапливались между запусками.
//
//  ПЕРЕМЕННЫЕ СРЕДЫ:
//    DATABASE_URL = postgresql://postgres:postgres@localhost:5432/bench_db
//    MARCIDB_ADDR = 127.0.0.1:3000  (опционально; по умолчанию 127.0.0.1:3000)
//
//  ИЗМЕНЕНИЯ В Cargo.toml:
//  ─────────────────────────────────────────────────────────────────────────────
//  [dev-dependencies]
//  criterion   = { version = "0.5", features = ["html_reports"] }
//  tokio       = { version = "1",   features = ["full"] }
//  serde       = { version = "1",   features = ["derive"] }
//  serde_json  = "1"
//
//  [[bench]]
//  name    = "benchmark"
//  harness = false
// =============================================================================

#[allow(warnings, unused)]
mod prisma;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prisma::PrismaClient;
use prisma_client_rust::{chrono, raw, PrismaValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

include!("_resource_monitor.rs");

// =============================================================================
//  Вспомогательные типы
// =============================================================================
const FAVOURITES_PER_USER: usize = 5;
#[derive(serde::Deserialize)]
struct RawIdI32 { id: i32 }

#[derive(serde::Deserialize)]
struct RawIdI64 { id: i64 }

fn parse_dt(s: &str) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(s).unwrap()
}

/// Извлекает i32 id из JSON: число → cast, {"id": N} → extract.
fn extract_i32_id(v: &Value) -> i32 {
    if let Some(n) = v.as_i64()                            { return n as i32; }
    if let Some(n) = v.get("id").and_then(|n| n.as_i64()) { return n as i32; }
    panic!("extract_i32_id: не удалось извлечь id из {:?}", v)
}

// =============================================================================
//  ЧАСТЬ 1: MarciTcpClient — TCP-клиент к MarciDB-серверу (MDWP-протокол)
//
//  Реализует тот же интерфейс, что и бывший MarciEngineClient, но вместо
//  прямых fn-call идёт по сети:
//
//    insert    → MDWP фрейм OP_INSERT   → сервер → parse_insert + insert_item
//    find_many → MDWP фрейм OP_FIND_MANY → сервер → parse_query + find_many
//    find_first→ MDWP фрейм OP_FIND_FIRST→ сервер → parse_query + find_first
//    update    → MDWP фрейм OP_UPDATE   → сервер → parse_update + update_item
//    delete    → MDWP фрейм OP_DELETE   → сервер → delete_item
//
//  Использует одно постоянное TCP-соединение (keep-alive из коробки —
//  сервер обслуживает неограниченное число запросов без переустановки).
//  Алгоритм Нейгла отключён (TCP_NODELAY), чтобы каждый flush уходил
//  немедленно — аналогично libpq при работе с PostgreSQL.
//
//  Протокол MDWP (MarciDB Wire Protocol):
//    Запрос:  [1B: op_code] [4B: u32 payload_len, BE] [payload]
//    Ответ:   [1B: status]  [4B: u32 data_len, BE]   [data]
//    Payload (Insert/FindMany/FindFirst):
//      [2B: u16 model_len] [model_name] [json_bytes]
//    Payload (Update):
//      [2B: u16 model_len] [model_name] [2B: u16 id_len] [id] [json_bytes]
//    Payload (Delete):
//      [2B: u16 model_len] [model_name] [2B: u16 id_len] [id]
// =============================================================================

// ─── MDWP op-коды (инлайн — без импорта из server-крейта) ───────────────────

const OP_INSERT:     u8 = 0x01;
const OP_FIND_MANY:  u8 = 0x02;
const OP_FIND_FIRST: u8 = 0x03;
const OP_UPDATE:     u8 = 0x04;
const OP_DELETE:     u8 = 0x05;
const STATUS_OK:     u8 = 0x00;

// ─── Сборщик MDWP-фреймов ────────────────────────────────────────────────────

fn mdwp_model_prefix(model: &str) -> Vec<u8> {
    let name = model.as_bytes();
    let mut buf = Vec::with_capacity(2 + name.len());
    buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
    buf.extend_from_slice(name);
    buf
}

fn restart_postgres() {
    // Windows
    std::process::Command::new("net")
        .args(["stop", "postgresql-x64-17"])
        .status().unwrap();
    std::process::Command::new("net")
        .args(["start", "postgresql-x64-17"])
        .status().unwrap();
    // Ждём пока сервер поднимется
    std::thread::sleep(Duration::from_secs(3));
}

fn mdwp_frame(op: u8, parts: &[&[u8]]) -> Vec<u8> {
    let payload: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(op);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn mdwp_insert(model: &str, json: &[u8]) -> Vec<u8> {
    mdwp_frame(OP_INSERT, &[&mdwp_model_prefix(model), json])
}

fn mdwp_find_many(model: &str, json: &[u8]) -> Vec<u8> {
    mdwp_frame(OP_FIND_MANY, &[&mdwp_model_prefix(model), json])
}

fn mdwp_find_first(model: &str, json: &[u8]) -> Vec<u8> {
    mdwp_frame(OP_FIND_FIRST, &[&mdwp_model_prefix(model), json])
}

fn mdwp_update(model: &str, item_id: &str, json: &[u8]) -> Vec<u8> {
    let id_bytes = item_id.as_bytes();
    let id_prefix: Vec<u8> = [(id_bytes.len() as u16).to_be_bytes().as_slice(), id_bytes].concat();
    mdwp_frame(OP_UPDATE, &[&mdwp_model_prefix(model), &id_prefix, json])
}

fn mdwp_delete(model: &str, item_id: &str) -> Vec<u8> {
    let id_bytes = item_id.as_bytes();
    let id_prefix: Vec<u8> = [(id_bytes.len() as u16).to_be_bytes().as_slice(), id_bytes].concat();
    mdwp_frame(OP_DELETE, &[&mdwp_model_prefix(model), &id_prefix])
}

// ─── Конвертер Value → строка id для URL-подобного формата ──────────────────

fn marci_value_to_id_url(id: &Value) -> String {
    if let Value::Object(map) = id {
        if map.len() == 1 {
            let v = map.values().next().unwrap();
            return match v {
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
        }
        return map.iter()
            .map(|(k, v)| {
                let raw = match v {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                format!("{}={}", k, raw)
            })
            .collect::<Vec<_>>()
            .join("&");
    }
    match id {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ─── Состояние TCP-соединения (живёт внутри tokio-рантайма) ─────────────────

struct MarciConn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl MarciConn {
    async fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).await
            .unwrap_or_else(|e| panic!(
                "MarciTcpClient: не удалось подключиться к MarciDB-серверу ({}).\n\
                 Убедитесь, что сервер запущен: cargo run --release --bin marcidb-server\n\
                 Ошибка: {}",
                addr, e
            ));
        stream.set_nodelay(true).unwrap();
        let (r, w) = stream.into_split();
        Self {
            reader: BufReader::new(r),
            writer: BufWriter::with_capacity(8 * 1024, w),
        }
    }

    /// Отправляет готовый MDWP-фрейм и возвращает байты ответа.
    /// Статус STATUS_ERR превращается в панику с текстом ошибки от сервера.
    async fn roundtrip(&mut self, frame: Vec<u8>) -> Vec<u8> {
        // Запрос
        self.writer.write_all(&frame).await
            .expect("MarciTcpClient: ошибка записи в сокет");
        self.writer.flush().await
            .expect("MarciTcpClient: ошибка flush сокета");

        // Ответ
        let status = self.reader.read_u8().await
            .expect("MarciTcpClient: ошибка чтения статуса");
        let data_len = self.reader.read_u32().await
            .expect("MarciTcpClient: ошибка чтения длины ответа") as usize;
        let mut data = vec![0u8; data_len];
        self.reader.read_exact(&mut data).await
            .expect("MarciTcpClient: ошибка чтения тела ответа");

        if status != STATUS_OK {
            panic!(
                "MarciTcpClient: сервер вернул ошибку: {}",
                String::from_utf8_lossy(&data)
            );
        }
        data
    }
}

// ─── Публичный клиент (sync-обёртка через block_on) ─────────────────────────

struct MarciTcpClient {
    rt:   tokio::runtime::Runtime,
    conn: Arc<tokio::sync::Mutex<MarciConn>>,
}

impl MarciTcpClient {
    fn new() -> Self {
        let addr = std::env::var("MARCIDB_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let conn = rt.block_on(async {
            Arc::new(tokio::sync::Mutex::new(MarciConn::connect(&addr).await))
        });
        Self { rt, conn }
    }

    fn roundtrip(&self, frame: Vec<u8>) -> Vec<u8> {
        let conn = self.conn.clone();
        self.rt.block_on(async move {
            conn.lock().await.roundtrip(frame).await
        })
    }

    // ── CRUD-операции ─────────────────────────────────────────────────────────

    fn insert(&self, model: &str, data: &Value) -> Value {
        let json = serde_json::to_vec(data)
            .unwrap_or_else(|e| panic!("insert '{}': сериализация: {}", model, e));
        let response = self.roundtrip(mdwp_insert(model, &json));
        let id_str = String::from_utf8(response)
            .unwrap_or_else(|e| panic!("insert '{}': невалидный UTF-8 в ответе: {}", model, e));
        serde_json::from_str(&id_str).unwrap_or_else(|_| Value::String(id_str))
    }

    fn find_many(&self, model: &str, query: &Value) -> Vec<String> {
        let json = serde_json::to_vec(query)
            .unwrap_or_else(|e| panic!("find_many '{}': сериализация: {}", model, e));
        let response = self.roundtrip(mdwp_find_many(model, &json));
        let arr_str = String::from_utf8(response)
            .unwrap_or_else(|e| panic!("find_many '{}': невалидный UTF-8 в ответе: {}", model, e));
        // Сервер возвращает JSON-массив строк-документов: ["{ ... }", "{ ... }"]
        // Разбираем как Value и конвертируем каждый элемент обратно в строку.
        let arr: Vec<Value> = serde_json::from_str(&arr_str)
            .unwrap_or_else(|e| panic!("find_many '{}': невалидный JSON-массив: {}", model, e));
        arr.into_iter()
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .collect()
    }

    fn find_first(&self, model: &str, query: &Value) -> Option<String> {
        let json = serde_json::to_vec(query)
            .unwrap_or_else(|e| panic!("find_first '{}': сериализация: {}", model, e));
        let response = self.roundtrip(mdwp_find_first(model, &json));
        let s = String::from_utf8(response)
            .unwrap_or_else(|e| panic!("find_first '{}': невалидный UTF-8 в ответе: {}", model, e));
        if s == "null" { None } else { Some(s) }
    }

    fn update(&self, model: &str, id: &Value, data: &Value) {
        let id_url = marci_value_to_id_url(id);
        let json = serde_json::to_vec(data)
            .unwrap_or_else(|e| panic!("update '{}': сериализация: {}", model, e));
        self.roundtrip(mdwp_update(model, &id_url, &json));
    }

    fn delete(&self, model: &str, id: &Value) {
        let id_url = marci_value_to_id_url(id);
        self.roundtrip(mdwp_delete(model, &id_url));
    }
}

// =============================================================================
//  ЧАСТЬ 2: Async Prisma-хелперы
//
//  Переиспользуются PrismaEngineClient'ом через rt.block_on().
//  Полностью идентичны оригинальным asis_* из HTTP-версии бенчмарка.
// =============================================================================

async fn prisma_insert_place(client: &PrismaClient, body: Value) -> Value {
    let name        = body["name"].as_str().unwrap_or("").to_string();
    let description = body["description"].as_str().unwrap_or("").to_string();
    let address     = body["address"].as_str().unwrap_or("").to_string();
    let place_type  = body["type"].as_str().unwrap_or("food").to_string();

    let mut opts: Vec<prisma::place::SetParam> = vec![
        prisma::place::tags::set(vec![]),
        prisma::place::photo_ids::set(vec![]),
        prisma::place::simillar_places::set(vec![]),
    ];
    if let Some(v) = body["averageBill"].as_i64()        { opts.push(prisma::place::average_bill::set(Some(v as i32))); }
    if let Some(v) = body["deliveryAvailable"].as_bool() { opts.push(prisma::place::delivery_available::set(Some(v))); }
    if let Some(v) = body["openingHours"].as_str()       { opts.push(prisma::place::opening_hours::set(Some(v.to_string()))); }
    if let Some(v) = body["price"].as_i64()              { opts.push(prisma::place::price::set(Some(v as i32))); }
    if let Some(v) = body["childZone"].as_bool()         { opts.push(prisma::place::child_zone::set(Some(v))); }
    if let Some(v) = body["hasParking"].as_bool()        { opts.push(prisma::place::has_parking::set(Some(v))); }
    if let Some(v) = body["isPayEntrance"].as_bool()     { opts.push(prisma::place::is_pay_entrance::set(Some(v))); }
    if let Some(v) = body["hasWifi"].as_bool()           { opts.push(prisma::place::has_wifi::set(Some(v))); }
    if let Some(v) = body["starRating"].as_i64()         { opts.push(prisma::place::star_rating::set(Some(v as i32))); }
    if let Some(v) = body["pricePerNight"].as_i64()      { opts.push(prisma::place::price_per_night::set(Some(v as i32))); }

    let p = client.place().create(name, description, address, place_type, opts)
        .exec().await.unwrap();
    json!({ "id": p.id })
}

async fn prisma_insert_landmark(client: &PrismaClient, body: Value, photo_ids: Vec<i32>,) -> Value {
    let name        = body["name"].as_str().unwrap_or("").to_string();
    let description = body["description"].as_str().unwrap_or("").to_string();
    let location    = body["location"].as_str().unwrap_or("").to_string();

    let mut opts: Vec<prisma::landmark::SetParam> = vec![
        prisma::landmark::photo_ids::set(photo_ids),
        prisma::landmark::simillar_landmarks::set(vec![]),
    ];
    if let Some(v) = body["indexOnLine"].as_i64() {
        opts.push(prisma::landmark::index_on_line::set(v as i32));
    }
    let group_id = body.get("groupId").and_then(|v| {
        v.as_i64().or_else(|| v.get("id").and_then(|n| n.as_i64()))
    });
    if let Some(gid) = group_id {
        opts.push(prisma::landmark::group_id::set(Some(gid as i32)));
    }
    let l = client.landmark().create(name, description, location, opts).exec().await.unwrap();
    json!({ "id": l.id })
}

async fn prisma_insert_landmark_group(client: &PrismaClient, body: Value) -> Value {
    let name        = body["name"].as_str().unwrap_or("").to_string();
    let description = body["description"].as_str().unwrap_or("").to_string();
    let g = client.landmark_group().create(
        name,
        vec![
            prisma::landmark_group::description::set(description),
            prisma::landmark_group::photo_ids::set(vec![]),
        ],
    ).exec().await.unwrap();
    json!({ "id": g.id })
}

async fn prisma_insert_app_user(client: &PrismaClient, body: Value) -> Value {
    let name      = body["name"].as_str().unwrap_or("").to_string();
    let auth_type = body["authentication"].as_str().unwrap_or("email").to_string();
    let mut opts: Vec<prisma::app_user::SetParam> = vec![];
    if let Some(v) = body["emailVerified"].as_bool() { opts.push(prisma::app_user::email_verified::set(Some(v))); }
    if let Some(v) = body["accessToken"].as_str()    { opts.push(prisma::app_user::access_token::set(Some(v.to_string()))); }
    if let Some(v) = body["address"].as_str()        { opts.push(prisma::app_user::address::set(Some(v.to_string()))); }
    let u = client.app_user().create(name, auth_type, opts).exec().await.unwrap();
    json!({ "id": u.id })
}

async fn prisma_insert_app_tour(client: &PrismaClient, body: Value) -> Value {
    let title  = body["title"].as_str().unwrap_or("").to_string();
    let text   = body["text"].as_str().unwrap_or("").to_string();
    let start  = parse_dt(body["start"].as_str().unwrap_or("2026-01-01T00:00:00Z"));
    let end_dt = parse_dt(body["end"].as_str().unwrap_or("2026-01-02T00:00:00Z"));
    let events = body["events"].clone();
    let t = client.app_tour().create(title, text, start, end_dt, events, vec![])
        .exec().await.unwrap();
    json!({ "id": t.id })
}

async fn prisma_insert_file(client: &PrismaClient, body: Value) -> Value {
    let file_size = body["fileSize"].as_i64().unwrap_or(0) as i32;
    let src       = body["src"].as_str().unwrap_or("").to_string();
    let mut opts  = vec![];
    if let Some(v) = body["name"].as_str() { opts.push(prisma::file::name::set(Some(v.to_string()))); }
    let f = client.file().create(file_size, src, opts).exec().await.unwrap();
    json!({ "id": f.id })
}

async fn prisma_find_many_place(client: &PrismaClient, where_val: Option<&Value>) -> Vec<Value> {
    let Some(w) = where_val else {
        return client.place().find_many(vec![])
            .select(prisma::place::select!({ id })).exec().await.unwrap()
            .iter().map(|p| json!({"id": p.id})).collect();
    };

    if w.get("simillarPlaces").is_some() {
        return client._query_raw::<RawIdI32>(raw!(
            r#"SELECT DISTINCT p.id FROM "Place" p, unnest(p."simillarPlaces") AS sp_id WHERE sp_id IS NOT NULL"#
        )).exec().await.unwrap()
            .iter().map(|r| json!({"id": r.id})).collect();
    }

    let mut filters: Vec<prisma::place::WhereParam> = vec![];
    if let Some(obj) = w.as_object() {
        for (key, val) in obj {
            match key.as_str() {
                "type" => {
                    if let Some(s) = val.as_str() {
                        filters.push(prisma::place::place_type::equals(s.to_string()));
                    }
                }
                "averageBill" => {
                    if let Some(gt) = val.get("$gt").and_then(|v| v.as_i64()) {
                        filters.push(prisma::place::average_bill::gt(gt as i32));
                    }
                }
                "starRating" => {
                    if let Some(n) = val.as_i64() {
                        filters.push(prisma::place::star_rating::equals(Some(n as i32)));
                    }
                }
                "name" => {
                    if let Some(s) = val.get("$includes").and_then(|v| v.as_str()) {
                        filters.push(prisma::place::name::contains(s.to_string()));
                    }
                }
                "openingHours" => {
                    if let Some(s) = val.as_str() {
                        filters.push(
                            prisma::place::opening_hours::equals(Some(s.to_string()))
                        );
                    }
                }
                _ => {}
            }
        }
    }
    client.place().find_many(filters)
        .select(prisma::place::select!({ id })).exec().await.unwrap()
        .iter().map(|p| json!({"id": p.id})).collect()
}

async fn prisma_find_many_landmark(client: &PrismaClient, where_val: Option<&Value>) -> Vec<Value> {
    let mut filters: Vec<prisma::landmark::WhereParam> = vec![];
    if let Some(w) = where_val {
        if let Some(obj) = w.as_object() {
            for (key, val) in obj {
                match key.as_str() {
                    "indexOnLine" => {
                        if let Some(gt) = val.get("$gt").and_then(|v| v.as_i64()) {
                            filters.push(prisma::landmark::index_on_line::gt(gt as i32));
                        }
                    }
                    "groupId" => {
                        filters.push(prisma::landmark::group_id::equals(Some(extract_i32_id(val))));
                    }
                    "name" => {
                        if let Some(s) = val.get("$includes").and_then(|v| v.as_str()) {
                            filters.push(prisma::landmark::name::contains(s.to_string()));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    client.landmark().find_many(filters)
        .select(prisma::landmark::select!({ id })).exec().await.unwrap()
        .iter().map(|l| json!({"id": l.id})).collect()
}

async fn prisma_find_many_app_tour(client: &PrismaClient, where_val: Option<&Value>) -> Vec<Value> {
    let Some(w) = where_val else {
        return client.app_tour().find_many(vec![])
            .select(prisma::app_tour::select!({ id })).exec().await.unwrap()
            .iter().map(|t| json!({"id": t.id})).collect();
    };

    if let Some(events_clause) = w.get("events") {
        if let Some(some_clause) = events_clause.get("$some") {
            let info = some_clause.get("info").and_then(|v| v.as_str()).unwrap_or("");
            return match info {
                "visit_landmark" => {
                    let lm_id = some_clause
                        .get("landmarkId")
                        .and_then(|v| v.get("id")).and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    {
                        let sql = format!(
                            r#"SELECT id FROM "AppTour" WHERE events @> '[{{"type":"visit_landmark","landmarkId":{}}}]'::jsonb"#,
                            lm_id
                        );
                        client._query_raw::<RawIdI32>(raw!(&sql)).exec().await.unwrap()
                    }
                        .iter().map(|r| json!({"id": r.id})).collect()
                }
                "eating" => {
                    client._query_raw::<RawIdI32>(raw!(
                        r#"SELECT id FROM "AppTour" WHERE events @> '[{"type":"eating"}]'::jsonb"#
                    )).exec().await.unwrap()
                        .iter().map(|r| json!({"id": r.id})).collect()
                }
                _ => vec![],
            };
        }
    }

    let mut filters: Vec<prisma::app_tour::WhereParam> = vec![];
    if let Some(obj) = w.as_object() {
        if let Some(start_clause) = obj.get("start") {
            if let Some(gte) = start_clause.get("$gte").and_then(|v| v.as_str()) {
                filters.push(prisma::app_tour::start::gte(parse_dt(gte)));
            }
        }
    }
    client.app_tour().find_many(filters)
        .select(prisma::app_tour::select!({ id })).exec().await.unwrap()
        .iter().map(|t| json!({"id": t.id})).collect()
}

async fn prisma_find_many_favourite(client: &PrismaClient, where_val: Option<&Value>) -> Vec<Value> {
    let mut filters: Vec<prisma::favourite::WhereParam> = vec![];
    if let Some(w) = where_val {
        if let Some(obj) = w.as_object() {
            if let Some(uid_val) = obj.get("userId") {
                filters.push(prisma::favourite::user_id::equals(extract_i32_id(uid_val)));
            }
        }
    }
    client.favourite().find_many(filters)
        .select(prisma::favourite::select!({ id })).exec().await.unwrap()
        .iter().map(|f| json!({"id": f.id})).collect()
}

// ── B5. Полный JOIN: AppUser → Favourites → Place/Landmark ───────────────────

#[derive(serde::Deserialize)]
struct FavJoinRow {
    user_name:     String,
    fav_id:        i64,
    fav_type:      String,
    created_at:    String,
    place_id:      Option<i32>,
    place_name:    Option<String>,
    landmark_id:   Option<i32>,
    landmark_name: Option<String>,
}

async fn prisma_find_user_with_favourites(
    client:  &PrismaClient,
    user_id: i32,
) -> Option<Value> {
    let sql = format!(r#"
        SELECT
            u.name             AS user_name,
            f.id               AS fav_id,
            f.type             AS fav_type,
            f."createdAt"      AS created_at,
            p.id               AS place_id,
            p.name             AS place_name,
            l.id               AS landmark_id,
            l.name             AS landmark_name
        FROM   "AppUser"   u
        JOIN   "Favourite" f  ON f."userId"     = u.id
        LEFT   JOIN "Place"    p  ON f."placeId"    = p.id
        LEFT   JOIN "Landmark" l  ON f."landmarkId" = l.id
        WHERE  u.id = {}
    "#, user_id);

    let rows = client
        ._query_raw::<FavJoinRow>(raw!(&sql))
        .exec().await.unwrap();

    if rows.is_empty() { return None; }

    let user_name  = rows[0].user_name.clone();
    let favourites = rows.iter().map(|r| json!({
        "id":        r.fav_id,
        "type":      r.fav_type,
        "createdAt": r.created_at,
        "placeId":    r.place_id.map(|id| json!({ "id": id, "name": r.place_name })),
        "landmarkId": r.landmark_id.map(|id| json!({ "id": id, "name": r.landmark_name })),
    })).collect::<Vec<_>>();

    Some(json!({ "name": user_name, "favourites": favourites }))
}

async fn prisma_find_first_place(client: &PrismaClient, id: i32) -> Option<Value> {
    client.place()
        .find_unique(prisma::place::id::equals(id))
        .select(prisma::place::select!({ id }))
        .exec().await.unwrap()
        .map(|p| json!({"id": p.id}))
}

async fn prisma_find_first_landmark(client: &PrismaClient, id: i32) -> Option<Value> {
    client.landmark()
        .find_unique(prisma::landmark::id::equals(id))
        .select(prisma::landmark::select!({ id }))
        .exec().await.unwrap()
        .map(|l| json!({"id": l.id}))
}

async fn prisma_update_place(client: &PrismaClient, id: i32, body: Value) {
    let mut params: Vec<prisma::place::SetParam> = vec![];
    if let Some(v) = body["description"].as_str() {
        params.push(prisma::place::description::set(v.to_string()));
    }
    if !params.is_empty() {
        client.place().update(prisma::place::id::equals(id), params).exec().await.unwrap();
    }
}

async fn prisma_update_app_user(client: &PrismaClient, id: i32, body: Value) {
    let mut params: Vec<prisma::app_user::SetParam> = vec![];
    if let Some(v) = body["authentication"].as_str() {
        params.push(prisma::app_user::authentication_type::set(v.to_string()));
        if v == "appleId" {
            params.push(prisma::app_user::access_token::set(None));
        }
    }
    if !params.is_empty() {
        client.app_user().update(prisma::app_user::id::equals(id), params).exec().await.unwrap();
    }
}

async fn prisma_update_landmark(client: &PrismaClient, id: i32, body: Value) {
    let mut params: Vec<prisma::landmark::SetParam> = vec![];
    if let Some(v) = body["description"].as_str() {
        params.push(prisma::landmark::description::set(v.to_string()));
    }
    if !params.is_empty() {
        client.landmark().update(prisma::landmark::id::equals(id), params).exec().await.unwrap();
    }
}

async fn prisma_delete_landmark_group(client: &PrismaClient, id: i32) {
    client.landmark_group()
        .delete(prisma::landmark_group::id::equals(id))
        .exec().await.unwrap();
}

async fn prisma_insert_favourite(client: &PrismaClient, body: Value) -> Value {
    let user_id  = body["userId"].as_i64().expect("insert_favourite: userId required") as i32;
    let fav_type = body["type"].as_str().unwrap_or("place");

    // Хотя бы один из placeId / landmarkId должен быть задан для валидной записи.
    let sql = if let Some(pid) = body["placeId"].as_i64() {
        format!(
            r#"INSERT INTO "Favourite" ("userId", "type", "placeId") VALUES ({}, '{}', {}) RETURNING id"#,
            user_id, fav_type, pid as i32
        )
    } else if let Some(lid) = body["landmarkId"].as_i64() {
        format!(
            r#"INSERT INTO "Favourite" ("userId", "type", "landmarkId") VALUES ({}, '{}', {}) RETURNING id"#,
            user_id, fav_type, lid as i32
        )
    } else {
        panic!("prisma_insert_favourite: требуется placeId или landmarkId");
    };

    let rows = client._query_raw::<RawIdI32>(raw!(&sql))
        .exec().await.unwrap();
    json!({ "id": rows[0].id })
}

async fn prisma_delete_app_user(client: &PrismaClient, id: i32) {
    client.app_user()
        .delete(prisma::app_user::id::equals(id))
        .exec().await.unwrap();
}

async fn prisma_create_many_landmark(client: &PrismaClient, records: Vec<Value>) -> usize {
    let count = records.len();
    let prisma_records: Vec<(String, String, String, Vec<prisma::landmark::SetParam>)> = records
        .iter()
        .map(|r| (
            r["name"].as_str().unwrap_or("").to_string(),
            r["description"].as_str().unwrap_or("desc").to_string(),
            r["location"].as_str().unwrap_or("55.7,37.6").to_string(),
            vec![
                prisma::landmark::index_on_line::set(
                    r["indexOnLine"].as_i64().unwrap_or(0) as i32
                ),
            ],
        ))
        .collect();
    client.landmark().create_many(prisma_records).exec().await.unwrap();
    count
}

// После prisma_create_many_landmark:

async fn prisma_create_many_place(client: &PrismaClient, records: &[Value]) -> usize {
    let prisma_records = records.iter().map(|r| {
        let mut opts = vec![
            prisma::place::tags::set(vec![]),
            prisma::place::photo_ids::set(vec![]),
            prisma::place::simillar_places::set(vec![]),
        ];
        if let Some(v) = r["averageBill"].as_i64()        { opts.push(prisma::place::average_bill::set(Some(v as i32))); }
        if let Some(v) = r["deliveryAvailable"].as_bool() { opts.push(prisma::place::delivery_available::set(Some(v))); }
        if let Some(v) = r["openingHours"].as_str()       { opts.push(prisma::place::opening_hours::set(Some(v.to_string()))); }
        (
            r["name"].as_str().unwrap_or("").to_string(),
            r["description"].as_str().unwrap_or("").to_string(),
            r["address"].as_str().unwrap_or("").to_string(),
            r["type"].as_str().unwrap_or("food").to_string(),
            opts,
        )
    }).collect();
    client.place().create_many(prisma_records).exec().await.unwrap();
    records.len()
}

async fn prisma_create_many_app_user(client: &PrismaClient, records: &[Value]) -> usize {
    let prisma_records = records.iter().map(|r| {
        let mut opts = vec![];
        if let Some(v) = r["accessToken"].as_str() { opts.push(prisma::app_user::access_token::set(Some(v.to_string()))); }
        if let Some(v) = r["address"].as_str()     { opts.push(prisma::app_user::address::set(Some(v.to_string()))); }
        (
            r["name"].as_str().unwrap_or("").to_string(),
            r["authentication"].as_str().unwrap_or("email").to_string(),
            opts,
        )
    }).collect();
    client.app_user().create_many(prisma_records).exec().await.unwrap();
    records.len()
}

// =============================================================================
//  ЧАСТЬ 3: PrismaEngineClient — прямой вызов Prisma Client Rust
//
//  Оборачивает PrismaClient в блокирующий Runtime.
//  Нет HTTP. Транспорт: IPC (Prisma QE) → TCP → PostgreSQL.
// =============================================================================

struct PrismaEngineClient {
    rt:     tokio::runtime::Runtime,
    client: Arc<PrismaClient>,
}

impl PrismaEngineClient {
    fn new() -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(PrismaClient::_builder().build())
            .expect(
                "PrismaEngineClient: не удалось подключиться к PostgreSQL.\n\
                 Убедитесь что DATABASE_URL задан и PostgreSQL запущен."
            );
        Self { rt, client: Arc::new(client) }
    }

    fn reset_data(&self) {
        self.rt.block_on(async {
            self.client.favourite().delete_many(vec![]).exec().await.unwrap();
            self.client.app_tour().delete_many(vec![]).exec().await.unwrap();
            self.client.landmark().delete_many(vec![]).exec().await.unwrap();
            self.client.landmark_group().delete_many(vec![]).exec().await.unwrap();
            self.client.place().delete_many(vec![]).exec().await.unwrap();
            self.client.app_user().delete_many(vec![]).exec().await.unwrap();
            self.client.file().delete_many(vec![]).exec().await.unwrap();
            self.client._query_raw::<serde_json::Value>(
                prisma_client_rust::Raw::new("VACUUM ANALYZE", vec![])
            ).exec().await.unwrap();
        });
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    fn insert_place(&self, body: Value) -> i32 {
        self.rt.block_on(prisma_insert_place(&self.client, body))
            ["id"].as_i64().unwrap() as i32
    }

    fn insert_landmark(&self, body: Value, photo_ids: Vec<i32>) -> i32 {
        self.rt.block_on(prisma_insert_landmark(&self.client, body, photo_ids))
            ["id"].as_i64().unwrap() as i32
    }

    fn insert_landmark_group(&self, body: Value) -> i32 {
        self.rt.block_on(prisma_insert_landmark_group(&self.client, body))
            ["id"].as_i64().unwrap() as i32
    }

    fn insert_app_user(&self, body: Value) -> i32 {
        self.rt.block_on(prisma_insert_app_user(&self.client, body))
            ["id"].as_i64().unwrap() as i32
    }

    fn insert_app_tour(&self, body: Value) -> i32 {
        self.rt.block_on(prisma_insert_app_tour(&self.client, body))
            ["id"].as_i64().unwrap() as i32
    }

    fn insert_file(&self, body: Value) -> i32 {
        self.rt.block_on(prisma_insert_file(&self.client, body))
            ["id"].as_i64().unwrap() as i32
    }

    pub fn create_many_app_tour(&self, records: &[Value]) -> i64 {
        self.rt.block_on(async {
            let data = records
                .iter()
                .map(|r| {
                    prisma::app_tour::create_unchecked(
                        r["title"].as_str().unwrap_or("").to_string(),
                        r["text"].as_str().unwrap_or("").to_string(),
                        parse_dt(r["start"].as_str().unwrap_or("2026-01-01T00:00:00Z")),
                        parse_dt(r["end"].as_str().unwrap_or("2026-01-02T00:00:00Z")),
                        r["events"].clone(),
                        vec![],
                    )
                })
                .collect();

            self.client
                .app_tour()
                .create_many(data)
                .exec()
                .await
                .unwrap()
        })
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    fn find_many_place(&self, query: &Value) -> Vec<Value> {
        self.rt.block_on(prisma_find_many_place(&self.client, query.get("$where")))
    }

    fn find_many_landmark(&self, query: &Value) -> Vec<Value> {
        self.rt.block_on(prisma_find_many_landmark(&self.client, query.get("$where")))
    }

    fn find_many_app_tour(&self, query: &Value) -> Vec<Value> {
        self.rt.block_on(prisma_find_many_app_tour(&self.client, query.get("$where")))
    }

    fn find_many_favourite(&self, query: &Value) -> Vec<Value> {
        self.rt.block_on(prisma_find_many_favourite(&self.client, query.get("$where")))
    }

    fn find_user_with_favourites(&self, user_id: i32) -> Option<Value> {
        self.rt.block_on(prisma_find_user_with_favourites(&self.client, user_id))
    }

    fn find_first_place(&self, id: i32) -> Option<Value> {
        self.rt.block_on(prisma_find_first_place(&self.client, id))
    }

    fn find_first_landmark(&self, id: i32) -> Option<Value> {
        self.rt.block_on(prisma_find_first_landmark(&self.client, id))
    }
    fn find_first_landmark_photo_ids(&self, lm_id: i32) -> Option<Vec<i32>> {
        self.rt.block_on(async {
            self.client
                .landmark()
                .find_unique(prisma::landmark::id::equals(lm_id))
                .select(prisma::landmark::select!({ photo_ids }))
                .exec()
                .await
                .unwrap()
                .map(|lm| lm.photo_ids)
        })
    }

    // ── Update / Delete ───────────────────────────────────────────────────────

    fn update_place(&self, id: i32, body: Value) {
        self.rt.block_on(prisma_update_place(&self.client, id, body));
    }

    fn update_app_user(&self, id: i32, body: Value) {
        self.rt.block_on(prisma_update_app_user(&self.client, id, body));
    }

    fn update_landmark(&self, id: i32, body: Value) {
        self.rt.block_on(prisma_update_landmark(&self.client, id, body));
    }

    fn delete_landmark_group(&self, id: i32) {
        self.rt.block_on(prisma_delete_landmark_group(&self.client, id));
    }

     fn insert_favourite(&self, body: Value) -> i32 {
         self.rt.block_on(prisma_insert_favourite(&self.client, body))
             ["id"].as_i64().unwrap() as i32
     }

     fn delete_app_user(&self, id: i32) {
         self.rt.block_on(prisma_delete_app_user(&self.client, id));
     }

    fn create_many_landmark(&self, records: Vec<Value>) -> usize {
        self.rt.block_on(prisma_create_many_landmark(&self.client, records))
    }

    fn create_many_place(&self, records: &[Value]) -> usize {
        self.rt.block_on(prisma_create_many_place(&self.client, records))
    }

    fn create_many_app_user(&self, records: &[Value]) -> usize {
        self.rt.block_on(prisma_create_many_app_user(&self.client, records))
    }

    fn raw_execute(&self, sql: &str) {
        self.rt.block_on(async {
            self.client
                ._execute_raw(prisma_client_rust::Raw::new(sql, vec![]))
                .exec()
                .await
                .unwrap();
        });
    }
}

// =============================================================================
//  ЧАСТЬ 4: Тестовые данные
// =============================================================================

struct Scenario {
    files:     usize,
    groups:    usize,
    landmarks: usize,
    places:    usize,
    users:     usize,
    tours:     usize,
}


impl Scenario {
    fn scale() -> usize {
        std::env::var("BENCH_SCALE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1)
    }

    fn medium() -> Self {
        let k = Self::scale();
        Self {
            files:     20  * k,
            groups:    5   * k,
            landmarks: 100  * k,
            places:    100 * k,
            users:     50  * k,
            tours:     100  * k,
        }
    }
}

// ── MarciEngineFixture ────────────────────────────────────────────────────────

struct MarciEngineFixture {
    engine:       MarciTcpClient,
    file_ids:     Vec<Value>,
    group_ids:    Vec<Value>,
    landmark_ids: Vec<Value>,
    place_ids:    Vec<Value>,
    user_ids:     Vec<Value>,
    tour_ids:     Vec<Value>,
}

impl MarciEngineFixture {
    fn new(s: &Scenario) -> Self {
        let engine = MarciTcpClient::new();

        let file_ids: Vec<Value> = (0..s.files).map(|i| {
            engine.insert("File", &json!({
                "fileSize": (i + 1) * 1024,
                "src": format!("/img/{}.jpg", i)
            }))
        }).collect();

        let group_ids: Vec<Value> = (0..s.groups).map(|i| {
            engine.insert("LandmarkGroup", &json!({
                "name": format!("Group {}", i),
                "description": "Test group"
            }))
        }).collect();

        let file_refs: Vec<Value> = file_ids.iter().take(2).cloned().collect();
        let landmark_types = ["historical", "natural", "cultural", "architectural", "religious"];
        let landmark_ids: Vec<Value> = (0..s.landmarks).map(|i| {
            let t = landmark_types[i % 5];
            engine.insert("Landmark", &json!({
                "name":        format!("Landmark {} ({})", i, t),
                "description": format!("Description of landmark {}", i),
                "location":    format!("55.{},37.{}", 70 + i % 10, 60 + i % 10),
                "indexOnLine": i as u64,
                "groupId":     group_ids[i % s.groups],
                "photoIds":    file_refs
            }))
        }).collect();

        let place_types = ["food", "museum", "square", "park", "hotel"];
        let place_ids: Vec<Value> = (0..s.places).map(|i| {
            let t = place_types[i % 5];
            let mut body = json!({
                "name":        format!("Place {} ({})", i, t),
                "description": format!("Desc {}", i),
                "address":     format!("{} Main St", i),
                "type":        t
            });
            match t {
                "food"   => { body["averageBill"] = json!(500 + i as u64 * 10); body["deliveryAvailable"] = json!(i % 2 == 0); body["openingHours"] = json!("10:00-22:00"); }
                "museum" => { body["price"] = json!(300u64); body["childZone"] = json!(true); body["openingHours"] = json!("09:00-18:00"); }
                "square" => { body["hasParking"] = json!(true); body["childZone"] = json!(false); }
                "park"   => { body["isPayEntrance"] = json!(false); body["hasWifi"] = json!(true); }
                "hotel"  => { body["starRating"] = json!(4u64); body["pricePerNight"] = json!(3000u64 + i as u64); }
                _ => {}
            }
            engine.insert("Place", &body)
        }).collect();

        let auth_variants = ["email", "vkId", "appleId", "yandexId"];
        let user_ids: Vec<Value> = (0..s.users).map(|i| {
            let auth = auth_variants[i % 4];
            let mut body = json!({ "name": format!("User {}", i), "authentication": auth });
            match auth {
                "email"    => { body["emailVerified"] = json!(true); body["address"] = json!(format!("addr_{}", i)); }
                "vkId"     => { body["accessToken"] = json!(format!("tok_vk_{}", i)); body["address"] = json!(format!("addr_{}", i)); }
                "yandexId" => { body["accessToken"] = json!(format!("tok_ya_{}", i)); body["address"] = json!(format!("addr_{}", i)); }
                "appleId"  => { body["address"] = json!(format!("addr_{}", i)); }
                _ => {}
            }
            engine.insert("AppUser", &body)
        }).collect();

        let tour_ids: Vec<Value> = (0..s.tours).map(|i| {
            let lm_id = landmark_ids[i % s.landmarks].clone();
            let pl_id = place_ids[i % s.places].clone();
            engine.insert("AppTour", &json!({
                "title": format!("Tour {}", i),
                "text":  format!("Description of tour {}", i),
                "start": format!("2026-0{}-01T10:00:00Z", (i % 9) + 1),
                "end":   format!("2026-0{}-02T18:00:00Z", (i % 9) + 1),
                "events": [
                    { "order": 1, "info": "visit_landmark", "landmarkId": lm_id, "time": "2026-05-01T10:00:00Z" },
                    { "order": 2, "info": "eating",          "placeId":   pl_id,  "time": "2026-05-01T13:00:00Z" },
                    { "order": 3, "info": "visit_place",     "placeId":   pl_id,  "time": "2026-05-01T16:00:00Z" }
                ]
            }))
        }).collect();

        for u in 0..s.users {
            for j in 0..FAVOURITES_PER_USER {
                if j % 2 == 0 {
                    engine.insert("Favourite", &json!({
                        "userId":  user_ids[u],
                        "object":  "place",
                        "placeId": place_ids[j % s.places]
                    }));
                } else {
                    engine.insert("Favourite", &json!({
                        "userId":     user_ids[u],
                        "object":     "landmark",
                        "landmarkId": landmark_ids[j % s.landmarks]
                    }));
                }
            }
        }

        Self { engine, file_ids, group_ids, landmark_ids, place_ids, user_ids, tour_ids }
    }
}

// ── PrismaEngineFixture ───────────────────────────────────────────────────────

struct PrismaEngineFixture {
    engine:       PrismaEngineClient,
    file_ids:     Vec<i32>,
    group_ids:    Vec<i32>,
    landmark_ids: Vec<i32>,
    place_ids:    Vec<i32>,
    user_ids:     Vec<i32>,
    tour_ids:     Vec<i32>,
}

impl PrismaEngineFixture {
    fn new(s: &Scenario) -> Self {
        let engine = PrismaEngineClient::new();
        engine.reset_data();

        let file_ids: Vec<i32> = (0..s.files).map(|i| {
            engine.insert_file(json!({
                "fileSize": (i + 1) * 1024,
                "src": format!("/img/{}.jpg", i),
                "name": format!("img_{}.jpg", i)
            }))
        }).collect();

        let group_ids: Vec<i32> = (0..s.groups).map(|i| {
            engine.insert_landmark_group(json!({
                "name": format!("Group {}", i),
                "description": "Test group"
            }))
        }).collect();

        let photo_ids_for_landmark: Vec<i32> = vec![file_ids[0], file_ids[1]];
        let landmark_types = ["historical", "natural", "cultural", "architectural", "religious"];
        let landmark_ids: Vec<i32> = (0..s.landmarks).map(|i| {
            let t = landmark_types[i % 5];
            engine.insert_landmark(json!({
                "name":        format!("Landmark {} ({})", i, t),
                "description": format!("Description of landmark {}", i),
                "location":    format!("55.{},37.{}", 70 + i % 10, 60 + i % 10),
                "indexOnLine": i as u64,
                "groupId":     group_ids[i % s.groups]
            }), photo_ids_for_landmark.clone())
        }).collect();

        let place_types = ["food", "museum", "square", "park", "hotel"];
        let place_ids: Vec<i32> = (0..s.places).map(|i| {
            let t = place_types[i % 5];
            let mut body = json!({
                "name":        format!("Place {} ({})", i, t),
                "description": format!("Desc {}", i),
                "address":     format!("{} Main St", i),
                "type":        t
            });
            match t {
                "food"   => { body["averageBill"] = json!(500 + i as i64 * 10); body["deliveryAvailable"] = json!(i % 2 == 0); body["openingHours"] = json!("10:00-22:00"); }
                "museum" => { body["price"] = json!(300i64); body["childZone"] = json!(true); body["openingHours"] = json!("09:00-18:00"); }
                "square" => { body["hasParking"] = json!(true); body["childZone"] = json!(false); }
                "park"   => { body["isPayEntrance"] = json!(false); body["hasWifi"] = json!(true); }
                "hotel"  => { body["starRating"] = json!(4i64); body["pricePerNight"] = json!(3000i64 + i as i64); }
                _ => {}
            }
            engine.insert_place(body)
        }).collect();

        let auth_variants = ["email", "vkId", "appleId", "yandexId"];
        let user_ids: Vec<i32> = (0..s.users).map(|i| {
            let auth = auth_variants[i % 4];
            let mut body = json!({ "name": format!("User {}", i), "authentication": auth });
            match auth {
                "email"    => { body["emailVerified"] = json!(true); body["address"] = json!(format!("addr_{}", i)); }
                "vkId"     => { body["accessToken"] = json!(format!("tok_vk_{}", i)); body["address"] = json!(format!("addr_{}", i)); }
                "yandexId" => { body["accessToken"] = json!(format!("tok_ya_{}", i)); body["address"] = json!(format!("addr_{}", i)); }
                "appleId"  => { body["address"] = json!(format!("addr_{}", i)); }
                _ => {}
            }
            engine.insert_app_user(body)
        }).collect();

        let tour_ids: Vec<i32> = (0..s.tours).map(|i| {
            let lm_id = landmark_ids[i % s.landmarks];
            let pl_id = place_ids[i % s.places];
            engine.insert_app_tour(json!({
                "title": format!("Tour {}", i),
                "text":  format!("Description of tour {}", i),
                "start": format!("2026-0{}-01T10:00:00Z", (i % 9) + 1),
                "end":   format!("2026-0{}-02T18:00:00Z", (i % 9) + 1),
                "events": [
                    { "order": 1, "type": "visit_landmark", "landmarkId": lm_id, "time": "2026-05-01T10:00:00Z" },
                    { "order": 2, "type": "eating",         "placeId":   pl_id,  "time": "2026-05-01T13:00:00Z" },
                    { "order": 3, "type": "visit_place",    "placeId":   pl_id,  "time": "2026-05-01T16:00:00Z" }
                ]
            }))
        }).collect();

        for u in 0..s.users {
            for j in 0..FAVOURITES_PER_USER {
                if j % 2 == 0 {
                    engine.insert_favourite(json!({
                        "userId":  user_ids[u],
                        "type":    "place",
                        "placeId": place_ids[j % s.places]
                    }));
                } else {
                    engine.insert_favourite(json!({
                        "userId":     user_ids[u],
                        "type":       "landmark",
                        "landmarkId": landmark_ids[j % s.landmarks]
                    }));
                }
            }
        }

        Self { engine, file_ids, group_ids, landmark_ids, place_ids, user_ids, tour_ids }
    }
}

fn report_pg_db_size() {
    let url = std::env::var("PG_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/bench".into());

    let query = "\
        SELECT \
            pg_size_pretty(pg_database_size(current_database())) AS pretty, \
            pg_database_size(current_database())                 AS bytes;";

    match std::process::Command::new("psql")
        .arg(&url)
        .args(["-t", "-A", "-F", "\t", "-c", query])
        .output()
    {
        Ok(out) if out.status.success() => {
            let raw = String::from_utf8_lossy(&out.stdout);
            // Формат вывода при -t -A -F'\t': "<pretty>\t<bytes>"
            let mut parts = raw.trim().splitn(2, '\t');
            let pretty = parts.next().unwrap_or("?");
            let bytes  = parts.next().unwrap_or("?");
            eprintln!(
                "\n[db-size] PostgreSQL database size after inserts: {} ({} bytes)",
                pretty, bytes
            );
        }
        Ok(out) => {
            eprintln!(
                "[db-size] psql returned non-zero: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Err(e) => eprintln!("[db-size] could not run psql: {e}"),
    }
}



