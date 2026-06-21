use std::sync::Arc;
use marcidb::MarciDB;
use crate::protocol::{RequestBuilder, STATUS_OK};
use crate::ServerContext;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

// ─── Вспомогательные схемы ──────────────────────────────────────────────────

const SCHEMA: &str = r#"
model User {
    name String
    age Int
}
"#;

const SCHEMA_RESTRICT: &str = r#"
model Author {
    name String
}
model Post {
    title   String
    author  Author  @onDelete(Restrict)
}
"#;

const SCHEMA_REF: &str = r#"
model User {
    name    String
    profile Profile?
}
model Profile {
    bio String
}
"#;

// ─── Запуск TCP-сервера ─────────────────────────────────────────────────────

async fn run_server(
    ctx: Arc<ServerContext>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            let ctx = ctx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = stream.set_nodelay(true) {
                                    eprintln!("set_nodelay failed: {}", e);
                                    return;
                                }
                                let (mut reader, writer) = stream.into_split();
                                let mut writer = crate::protocol::ResponseWriter::new(writer);

                                loop {
                                    let msg = match crate::protocol::ClientMessage::read_from(&mut reader).await {
                                        Ok(Some(m)) => m,
                                        Ok(None) => break,
                                        Err(e) => {
                                            let _ = writer.write_err(&e.to_string()).await;
                                            break;
                                        }
                                    };

                                    use crate::protocol::ClientMessage;
                                    let result = match msg {
                                        ClientMessage::Insert { model, json } =>
                                            crate::tcp_handlers::handle_insert(ctx.clone(), model, json).await,
                                        ClientMessage::FindMany { model, json } =>
                                            crate::tcp_handlers::handle_find_many(ctx.clone(), model, json).await,
                                        ClientMessage::FindFirst { model, json } =>
                                            crate::tcp_handlers::handle_find_first(ctx.clone(), model, json).await,
                                        ClientMessage::Update { model, item_id, json } =>
                                            crate::tcp_handlers::handle_update(ctx.clone(), model, item_id, json).await,
                                        ClientMessage::Delete { model, item_id } =>
                                            crate::tcp_handlers::handle_delete(ctx.clone(), model, item_id).await,
                                    };

                                    let send_result = match result {
                                        Ok(data) => writer.write_ok(&data).await,
                                        Err(e) => writer.write_err(e.message()).await,
                                    };

                                    if let Err(e) = send_result {
                                        eprintln!("Write error: {}", e);
                                        break;
                                    }
                                }
                            });
                        }
                        Err(e) => eprintln!("accept error: {:?}", e),
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

    addr
}

// ─── Окружение для тестов ───────────────────────────────────────────────────

struct TestEnv {
    _temp_dir: TempDir,
    client: TcpStream,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TestEnv {
    async fn new(schema_content: &str) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.marci");
        tokio::fs::write(&schema_path, schema_content).await.unwrap();

        let data_dir = temp_dir.path().join("data");
        tokio::fs::create_dir_all(&data_dir).await.unwrap();

        let schema_str = tokio::fs::read_to_string(&schema_path).await.unwrap();
        let db = MarciDB::new(&schema_str, data_dir.to_str().unwrap());
        let ctx = Arc::new(ServerContext { db });

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let addr = run_server(ctx, shutdown_rx).await;

        let client = TcpStream::connect(addr).await.unwrap();
        client.set_nodelay(true).unwrap();

        TestEnv {
            _temp_dir: temp_dir,
            client,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    async fn send(&mut self, request: &[u8]) -> Result<Vec<u8>, String> {
        self.client.write_all(request).await.map_err(|e| e.to_string())?;
        self.client.flush().await.map_err(|e| e.to_string())?;

        let status = self.client.read_u8().await.map_err(|e| e.to_string())?;
        let len = self.client.read_u32().await.map_err(|e| e.to_string())? as usize;
        let mut data = vec![0u8; len];
        self.client.read_exact(&mut data).await.map_err(|e| e.to_string())?;

        if status == STATUS_OK {
            Ok(data)
        } else {
            let msg = String::from_utf8_lossy(&data).into_owned();
            Err(msg)
        }
    }

    async fn insert(&mut self, model: &str, json: &Value) -> Result<u64, String> {
        let bytes = serde_json::to_vec(json).map_err(|e| e.to_string())?;
        let req = RequestBuilder::insert(model, &bytes);
        let data = self.send(&req).await?;
        let response: Value = serde_json::from_slice(&data)
            .map_err(|e| format!("Invalid JSON response: {}", e))?;
        response["id"].as_u64()
            .ok_or_else(|| format!("Response missing 'id' field: {}", String::from_utf8_lossy(&data)))
    }

    async fn find_many(&mut self, model: &str, json: &Value) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(json).map_err(|e| e.to_string())?;
        let req = RequestBuilder::find_many(model, &bytes);
        self.send(&req).await
    }

    async fn find_first(&mut self, model: &str, json: &Value) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(json).map_err(|e| e.to_string())?;
        let req = RequestBuilder::find_first(model, &bytes);
        self.send(&req).await
    }

    async fn update(&mut self, model: &str, item_id: &str, json: &Value) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(json).map_err(|e| e.to_string())?;
        let req = RequestBuilder::update(model, item_id, &bytes);
        self.send(&req).await
    }

    async fn delete(&mut self, model: &str, item_id: &str) -> Result<Vec<u8>, String> {
        let req = RequestBuilder::delete(model, item_id);
        self.send(&req).await
    }
}

// ─── Тесты ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_and_find_many() {
    let mut env = TestEnv::new(SCHEMA).await;

    let insert_body = json!({"name": "Alice", "age": 30});
    let item_id = env.insert("User", &insert_body).await.unwrap(); // u64

    let find_body = json!({
        "id": true, "name": true, "age": true,
        "$where": { "id": item_id }   // число
    });
    let raw = env.find_many("User", &find_body).await.unwrap();
    let array: Value = serde_json::from_slice(&raw).unwrap();
    let arr = array.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Alice");
    assert_eq!(arr[0]["age"], 30);
    assert_eq!(arr[0]["id"].as_u64(), Some(item_id));

    env.shutdown().await;
}

#[tokio::test]
async fn test_find_first_not_found() {
    let mut env = TestEnv::new(SCHEMA).await;
    let find_body = json!({
        "name": true,
        "$where": { "name": { "$eq": "Nobody" } }
    });
    let raw = env.find_first("User", &find_body).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&raw).trim(), "null");
    env.shutdown().await;
}

#[tokio::test]
async fn test_update() {
    let mut env = TestEnv::new(SCHEMA).await;
    let insert_body = json!({"name": "Bob", "age": 25});
    let item_id = env.insert("User", &insert_body).await.unwrap();

    let _ = env.update("User", &item_id.to_string(), &json!({"age": 26})).await.unwrap();
    let raw = env.find_first("User", &json!({"age": true, "$where": {"id": item_id}})).await.unwrap();
    let user: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(user["age"], 26);
    env.shutdown().await;
}

#[tokio::test]
async fn test_delete() {
    let mut env = TestEnv::new(SCHEMA).await;
    let item_id = env.insert("User", &json!({"name": "Charlie", "age": 40})).await.unwrap();
    let _ = env.delete("User", &item_id.to_string()).await.unwrap();
    let raw = env.find_first("User", &json!({"name": true, "$where": {"id": item_id}})).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&raw).trim(), "null");
    env.shutdown().await;
}

#[tokio::test]
async fn test_model_not_found() {
    let mut env = TestEnv::new(SCHEMA).await;
    let err = env.insert("NonExistent", &json!({"name": "x"})).await.unwrap_err();
    assert!(err.to_lowercase().contains("not found"), "unexpected error: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_bad_json() {
    let mut env = TestEnv::new(SCHEMA).await;
    use crate::protocol::OP_INSERT;
    let model = "User";
    let mut payload = Vec::new();
    let name_bytes = model.as_bytes();
    payload.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(name_bytes);
    payload.extend_from_slice(b"{ invalid json }");
    let mut frame = Vec::new();
    frame.push(OP_INSERT);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);

    let err = env.send(&frame).await.unwrap_err();
    assert!(err.contains("Invalid JSON"), "expected Invalid JSON, got: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_missing_id_for_update() {
    let mut env = TestEnv::new(SCHEMA).await;
    let err = env.update("User", "", &json!({"age": 99})).await.unwrap_err();
    assert!(err.contains("parse") || err.to_lowercase().contains("id"), "unexpected error: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_missing_id_for_delete() {
    let mut env = TestEnv::new(SCHEMA).await;
    let err = env.delete("User", "").await.unwrap_err();
    assert!(err.contains("parse") || err.to_lowercase().contains("id"), "unexpected error: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_insert_null_not_allowed() {
    let mut env = TestEnv::new(SCHEMA).await;
    let err = env.insert("User", &json!({"name": null, "age": 30})).await.unwrap_err();
    assert!(err.contains("NullNotAllowed") || err.to_lowercase().contains("null"), "expected null error, got: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_update_null_not_allowed() {
    let schema = r#"
        model User {
            name String
            age Int?
            email String
        }
    "#;
    let mut env = TestEnv::new(schema).await;
    let item_id = env.insert("User", &json!({"name": "Bob", "age": 25, "email": "bob@test.com"})).await.unwrap();
    let err = env.update("User", &item_id.to_string(), &json!({"name": null})).await.unwrap_err();
    assert!(err.contains("NullNotAllowed") || err.to_lowercase().contains("null"), "expected null error, got: {err}");
    env.shutdown().await;
}

#[tokio::test]
async fn test_delete_restrict_error() {
    let mut env = TestEnv::new(SCHEMA_RESTRICT).await;
    let author_id = env.insert("Author", &json!({"name": "Alice"})).await.unwrap();
    let _ = env.insert("Post", &json!({"title": "Hello", "author": {"id": author_id}})).await.unwrap();
    let err = env.delete("Author", &author_id.to_string()).await;
    assert!(err.is_err(), "expected Restrict error");
    env.shutdown().await;
}

#[tokio::test]
async fn test_update_connect_nonexistent_ref_error() {
    let mut env = TestEnv::new(SCHEMA_REF).await;
    let user_id = env.insert("User", &json!({"name": "Bob"})).await.unwrap();
    let err = env.update("User", &user_id.to_string(), &json!({"profile": {"connect": {"id": 99999}}})).await;
    assert!(err.is_err(), "expected connect not found error");
    env.shutdown().await;
}