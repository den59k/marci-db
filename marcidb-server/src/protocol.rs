use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

// ─── Op-коды клиента ────────────────────────────────────────────────────────

pub const OP_INSERT:     u8 = 0x01;
pub const OP_FIND_MANY:  u8 = 0x02;
pub const OP_FIND_FIRST: u8 = 0x03;
pub const OP_UPDATE:     u8 = 0x04;
pub const OP_DELETE:     u8 = 0x05;

// ─── Статус-коды сервера ─────────────────────────────────────────────────────

pub const STATUS_OK:  u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;

// ─── Входящие сообщения ──────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ClientMessage {
    Insert     { model: String, json: Vec<u8> },
    FindMany   { model: String, json: Vec<u8> },
    FindFirst  { model: String, json: Vec<u8> },
    Update     { model: String, item_id: String, json: Vec<u8> },
    Delete     { model: String, item_id: String },
}

impl ClientMessage {
    pub async fn read_from(reader: &mut OwnedReadHalf) -> io::Result<Option<Self>> {
        // 1. Op-код
        let op = match reader.read_u8().await {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        let payload_len = reader.read_u32().await? as usize;

        if payload_len > 64 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Payload too large: {} bytes", payload_len),
            ));
        }

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;

        Ok(Some(Self::decode(op, payload)?))
    }

    fn decode(op: u8, payload: Vec<u8>) -> io::Result<Self> {
        let mut cur = 0usize;

        macro_rules! read_u16 {
            () => {{
                if cur + 2 > payload.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected end of payload (u16)"));
                }
                let v = u16::from_be_bytes([payload[cur], payload[cur + 1]]) as usize;
                cur += 2;
                v
            }};
        }

        macro_rules! read_str {
            ($len:expr) => {{
                if cur + $len > payload.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected end of payload (str)"));
                }
                let s = String::from_utf8(payload[cur..cur + $len].to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
                cur += $len;
                s
            }};
        }

        let model_len = read_u16!();
        let model     = read_str!(model_len);

        match op {
            OP_INSERT => {
                let json = payload[cur..].to_vec();
                Ok(ClientMessage::Insert { model, json })
            }
            OP_FIND_MANY => {
                let json = payload[cur..].to_vec();
                Ok(ClientMessage::FindMany { model, json })
            }
            OP_FIND_FIRST => {
                let json = payload[cur..].to_vec();
                Ok(ClientMessage::FindFirst { model, json })
            }
            OP_UPDATE => {
                let id_len  = read_u16!();
                let item_id = read_str!(id_len);
                let json    = payload[cur..].to_vec();
                Ok(ClientMessage::Update { model, item_id, json })
            }
            OP_DELETE => {
                let id_len  = read_u16!();
                let item_id = read_str!(id_len);
                Ok(ClientMessage::Delete { model, item_id })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown op-code: 0x{:02X}", op),
            )),
        }
    }
}

pub struct ResponseWriter {
    inner: BufWriter<OwnedWriteHalf>,
}

impl ResponseWriter {
    pub fn new(write_half: OwnedWriteHalf) -> Self {
        Self { inner: BufWriter::with_capacity(8 * 1024, write_half) }
    }

    pub async fn write_ok(&mut self, data: &[u8]) -> io::Result<()> {
        self.inner.write_u8(STATUS_OK).await?;
        self.inner.write_u32(data.len() as u32).await?;
        self.inner.write_all(data).await?;
        self.inner.flush().await
    }

    pub async fn write_err(&mut self, msg: &str) -> io::Result<()> {
        let bytes = msg.as_bytes();
        self.inner.write_u8(STATUS_ERR).await?;
        self.inner.write_u32(bytes.len() as u32).await?;
        self.inner.write_all(bytes).await?;
        self.inner.flush().await
    }
}

pub struct RequestBuilder;

impl RequestBuilder {
    fn model_prefix(model: &str) -> Vec<u8> {
        let name = model.as_bytes();
        let mut buf = Vec::with_capacity(2 + name.len());
        buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
        buf.extend_from_slice(name);
        buf
    }

    pub fn insert(model: &str, json: &[u8]) -> Vec<u8> {
        Self::frame(OP_INSERT, &[Self::model_prefix(model).as_slice(), json])
    }

    pub fn find_many(model: &str, json: &[u8]) -> Vec<u8> {
        Self::frame(OP_FIND_MANY, &[Self::model_prefix(model).as_slice(), json])
    }

    pub fn find_first(model: &str, json: &[u8]) -> Vec<u8> {
        Self::frame(OP_FIND_FIRST, &[Self::model_prefix(model).as_slice(), json])
    }

    pub fn update(model: &str, item_id: &str, json: &[u8]) -> Vec<u8> {
        let id_bytes = item_id.as_bytes();
        let id_prefix: Vec<u8> = [(id_bytes.len() as u16).to_be_bytes().as_slice(), id_bytes].concat();
        Self::frame(OP_UPDATE, &[Self::model_prefix(model).as_slice(), &id_prefix, json])
    }

    pub fn delete(model: &str, item_id: &str) -> Vec<u8> {
        let id_bytes = item_id.as_bytes();
        let id_prefix: Vec<u8> = [(id_bytes.len() as u16).to_be_bytes().as_slice(), id_bytes].concat();
        Self::frame(OP_DELETE, &[Self::model_prefix(model).as_slice(), &id_prefix])
    }

    fn frame(op: u8, parts: &[&[u8]]) -> Vec<u8> {
        let payload: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(op);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }
}
