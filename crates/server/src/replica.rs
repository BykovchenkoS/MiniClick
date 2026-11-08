//! Сетевой обмен реплики с лидером: read-only `/replica/*` и HTTP-клиент `Peer`.

use crate::error::{blocking, ApiError, ErrorResponse};
use crate::AppState;
use axum::extract::{rejection::PathRejection, Path, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::blocking::Client;
use reqwest::Url;
use std::io::Read;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;
use types::{
    Catalog, DbError, NodeIdentity, PartData, Peer, Result as DbResult, TableEntry, TableId,
};

const MAGIC: &[u8; 4] = b"MCP1";
const MAX_FILES: u32 = 4096;
const MAX_NAME_BYTES: usize = 4096;
/// Верхняя граница ответа с одним part; защищает от неограниченной загрузки в память.
const MAX_PART_BYTES: u64 = 512 * 1024 * 1024;

#[utoipa::path(get, path = "/replica/ping", tag = "replica", responses(
    (status = 200, description = "Идентичность узла в JSON")
))]
pub async fn ping(State(app): State<AppState>) -> Json<NodeIdentity> {
    Json(NodeIdentity {
        cluster_id: app.config.node.cluster_id.clone(),
        node_id: app.config.node.node_id.clone(),
    })
}

#[utoipa::path(get, path = "/replica/tables", tag = "replica", responses(
    (status = 200, description = "Таблицы лидера с идентичностями в JSON"),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn tables(State(app): State<AppState>) -> Json<Vec<TableEntry>> {
    Json(app.catalog.table_entries())
}

#[utoipa::path(get, path = "/replica/tables/{id}/parts", tag = "replica", params(
    ("id" = String, Path, description = "Идентификатор таблицы лидера")
), responses(
    (status = 200, description = "Имена активных parts", body = [String]),
    (status = 400, description = "Неверный путь", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn parts(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<Vec<String>>, ApiError> {
    let Path(id) = path?;
    blocking(move || {
        app.catalog
            .get_table_by_id(&TableId::new(id))?
            .parts()
            .map(Json)
    })
    .await
}

#[utoipa::path(get, path = "/replica/tables/{id}/parts/{part}", tag = "replica", params(
    ("id" = String, Path, description = "Идентификатор таблицы лидера"),
    ("part" = String, Path, description = "Имя активного part")
), responses(
    (status = 200, description = "Part в бинарном формате MCP1", content_type = "application/octet-stream"),
    (status = 400, description = "Неверный путь", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn part(
    State(app): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Response, ApiError> {
    let Path((id, part)) = path?;
    let bytes = blocking(move || {
        let data = app
            .catalog
            .get_table_by_id(&TableId::new(id))?
            .export_part(&part)?;
        encode_part(&data)
    })
    .await?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
}

/// Блокирующий клиент `Peer`; вызывать из `spawn_blocking`, не из async-задачи.
pub struct HttpPeer {
    base: Url,
    client: Client,
}

impl HttpPeer {
    /// `base` — полный URL лидера, например `http://127.0.0.1:8123`.
    pub fn new(base: &str) -> DbResult<HttpPeer> {
        let base = Url::parse(base)
            .map_err(|error| DbError::Syntax(format!("invalid leader URL: {error}")))?;
        if base.scheme() != "http" {
            return Err(DbError::Syntax("leader URL must use http".into()));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|error| DbError::Internal(format!("HTTP client: {error}")))?;
        Ok(HttpPeer { base, client })
    }

    /// Принимает адрес в виде `host:port` или полный URL.
    pub fn from_addr(addr: &str) -> DbResult<HttpPeer> {
        let base = if addr.contains("://") {
            addr.to_owned()
        } else {
            format!("http://{addr}")
        };
        Self::new(&base)
    }

    fn url(&self, segments: &[&str]) -> DbResult<Url> {
        let mut url = self.base.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| DbError::Syntax("invalid leader URL".into()))?;
            path.pop_if_empty();
            for segment in segments {
                path.push(segment);
            }
        }
        Ok(url)
    }

    fn get(&self, url: Url) -> DbResult<reqwest::blocking::Response> {
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|error| DbError::Io(std::io::Error::other(error)))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(DbError::Internal(format!(
                "leader returned {status}: {}",
                body.trim()
            )));
        }
        Ok(response)
    }
}

impl Peer for HttpPeer {
    fn ping(&self) -> DbResult<NodeIdentity> {
        self.get(self.url(&["replica", "ping"])?)?
            .json()
            .map_err(|error| DbError::Internal(format!("invalid identity response: {error}")))
    }

    fn list_tables(&self) -> DbResult<Vec<TableEntry>> {
        self.get(self.url(&["replica", "tables"])?)?
            .json()
            .map_err(|error| DbError::Internal(format!("invalid tables response: {error}")))
    }

    fn list_parts(&self, table: &TableId) -> DbResult<Vec<String>> {
        self.get(self.url(&["replica", "tables", table.as_str(), "parts"])?)?
            .json()
            .map_err(|error| DbError::Internal(format!("invalid parts response: {error}")))
    }

    fn fetch_part(&self, table: &TableId, part: &str) -> DbResult<PartData> {
        let response =
            self.get(self.url(&["replica", "tables", table.as_str(), "parts", part])?)?;
        if let Some(len) = response.content_length() {
            if len > MAX_PART_BYTES {
                return Err(DbError::Internal(format!(
                    "part response is too large: {len} bytes"
                )));
            }
        }
        let mut bytes = Vec::new();
        let read = response.take(MAX_PART_BYTES + 1).read_to_end(&mut bytes)?;
        if read as u64 > MAX_PART_BYTES {
            return Err(DbError::Internal(
                "part response exceeds the size limit".into(),
            ));
        }
        decode_part(&bytes)
    }
}

/// Фоновый цикл реплики: сверка с лидером и пауза `interval`.
/// Клиент создаётся один раз и переиспользует соединения.
pub fn sync_loop(
    catalog: Arc<dyn Catalog>,
    leader_addr: String,
    expected: NodeIdentity,
    interval: Duration,
    stop: std::sync::mpsc::Receiver<()>,
) {
    let peer = match HttpPeer::from_addr(&leader_addr) {
        Ok(peer) => peer,
        Err(error) => {
            tracing::error!(%error, %leader_addr, "Реплика не смогла создать клиента лидера");
            return;
        }
    };
    loop {
        match replication::sync_once(catalog.as_ref(), &peer, &expected) {
            Ok(report) if report.failures > 0 => tracing::warn!(
                created = report.tables_created,
                dropped = report.tables_dropped,
                fetched = report.parts_fetched,
                removed = report.parts_removed,
                failures = report.failures,
                "Синхронизация реплики завершена с ошибками"
            ),
            Ok(report) => tracing::info!(
                created = report.tables_created,
                dropped = report.tables_dropped,
                fetched = report.parts_fetched,
                removed = report.parts_removed,
                "Синхронизация реплики завершена"
            ),
            Err(error) => tracing::warn!(%error, "Синхронизация с лидером не удалась"),
        }
        match stop.recv_timeout(interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn encode_part(part: &PartData) -> DbResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    put_string(&mut out, &part.name)?;
    put_string(&mut out, &part.checksum)?;
    let count = u32::try_from(part.files.len())
        .map_err(|_| DbError::Internal("too many part files".into()))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (name, data) in &part.files {
        put_string(&mut out, name)?;
        let len = u32::try_from(data.len())
            .map_err(|_| DbError::Unsupported("part file is too large".into()))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(data);
    }
    Ok(out)
}

fn put_string(out: &mut Vec<u8>, value: &str) -> DbResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| DbError::Internal("part field is too long".into()))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_part(bytes: &[u8]) -> DbResult<PartData> {
    let mut reader = Reader::new(bytes);
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(DbError::Internal("invalid replica part header".into()));
    }
    let name = reader.string()?;
    let checksum = reader.string()?;
    let count = reader.u32()?;
    if count > MAX_FILES {
        return Err(DbError::Internal("too many part files".into()));
    }
    let mut files = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let file = reader.string()?;
        let len = reader.u32()? as usize;
        files.push((file, reader.take(len)?.to_vec()));
    }
    if !reader.is_empty() {
        return Err(DbError::Internal(
            "trailing bytes after replica part".into(),
        ));
    }
    Ok(PartData {
        name,
        files,
        checksum,
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Reader<'a> {
        Reader { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> DbResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| DbError::Internal("replica part length overflow".into()))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| DbError::Internal("truncated replica part".into()))?;
        self.pos = end;
        Ok(slice)
    }

    fn u32(&mut self) -> DbResult<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
            DbError::Internal("invalid u32 field".into())
        })?))
    }

    fn string(&mut self) -> DbResult<String> {
        let len = self.u32()? as usize;
        if len > MAX_NAME_BYTES {
            return Err(DbError::Internal("replica part field is too long".into()));
        }
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| DbError::Internal("replica part field is not UTF-8".into()))
    }

    fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(name: &str, file: &str, data: &[u8]) -> PartData {
        PartData {
            name: name.to_owned(),
            files: vec![(file.to_owned(), data.to_vec())],
            checksum: "checksum".to_owned(),
        }
    }

    #[test]
    fn part_codec_roundtrip_preserves_names_and_bytes() {
        let value = part("all_1_1_0", "part.json", b"\x00\x01\xff");
        let encoded = encode_part(&value).unwrap();
        assert_eq!(&encoded[..4], MAGIC);
        assert_eq!(decode_part(&encoded).unwrap(), value);
    }

    #[test]
    fn part_codec_rejects_bad_magic_truncation_and_trailing_bytes() {
        assert!(decode_part(b"XXXX").is_err());
        assert!(decode_part(b"MCP").is_err());
        let encoded = encode_part(&part("all_1_1_0", "part.json", b"x")).unwrap();
        assert!(decode_part(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_part(&trailing).is_err());
    }

    #[test]
    fn part_codec_rejects_excessive_file_count() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(MAX_FILES + 1).to_le_bytes());
        let error = decode_part(&bytes).unwrap_err();
        assert!(
            matches!(error, DbError::Internal(ref message) if message.contains("too many part files")),
            "{error}"
        );
    }
}
