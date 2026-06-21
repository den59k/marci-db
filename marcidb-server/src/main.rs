mod tcp_handlers;
pub mod protocol;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::{fs, net::TcpListener};

use protocol::{ClientMessage, ResponseWriter};
use tcp_handlers::{
    handle_delete, handle_find_first, handle_find_many,
    handle_insert, handle_update,
};

pub struct ServerContext {
    pub db: marcidb::MarciDB,
}

#[tokio::main]
async fn main() {
    let schema_str = fs::read_to_string("schema.marci")
        .await
        .expect("schema.marci not found");
    fs::create_dir_all("./data").await.unwrap();

    let db  = marcidb::MarciDB::new(&schema_str, "./data");
    let ctx = Arc::new(ServerContext { db });

    // Слушаем на порту 3000 (можно сменить на 5433 по аналогии с Postgres)
    let addr: SocketAddr = ([127, 0, 0, 1], 3000).into();
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("MarciDB (MDWP) listening on {}", addr);

    loop {
        let (stream, peer) = listener.accept().await.unwrap();
        let ctx = ctx.clone();

        tokio::spawn(async move {
            if let Err(e) = stream.set_nodelay(true) {
                eprintln!("[{}] set_nodelay failed: {}", peer, e);
            }

            let (mut reader, writer) = stream.into_split();
            let mut writer = ResponseWriter::new(writer);

            loop {
                let msg = match ClientMessage::read_from(&mut reader).await {
                    Ok(Some(m)) => m,
                    Ok(None)    => break, // клиент закрыл соединение
                    Err(e)      => {
                        eprintln!("[{}] Protocol error: {}", peer, e);
                        // Пишем ошибку и закрываем соединение
                        let _ = writer.write_err(&e.to_string()).await;
                        break;
                    }
                };

                let result = match msg {
                    ClientMessage::Insert { model, json } =>
                        handle_insert(ctx.clone(), model, json).await,

                    ClientMessage::FindMany { model, json } =>
                        handle_find_many(ctx.clone(), model, json).await,

                    ClientMessage::FindFirst { model, json } =>
                        handle_find_first(ctx.clone(), model, json).await,

                    ClientMessage::Update { model, item_id, json } =>
                        handle_update(ctx.clone(), model, item_id, json).await,

                    ClientMessage::Delete { model, item_id } =>
                        handle_delete(ctx.clone(), model, item_id).await,
                };

                let send_result = match result {
                    Ok(data)  => writer.write_ok(&data).await,
                    Err(e)    => writer.write_err(e.message()).await,
                };

                if let Err(e) = send_result {
                    eprintln!("[{}] Write error: {}", peer, e);
                    break;
                }
            }   
        });
    }
}

#[cfg(test)]
mod tests;
