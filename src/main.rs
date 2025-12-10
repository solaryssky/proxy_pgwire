use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use governor::{Quota, RateLimiter};
use governor::state::{direct::NotKeyed, InMemoryState};
use governor::clock::DefaultClock;
use std::num::NonZeroU32;

use log::{debug, error, info};
use log4rs;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::{stream, Sink};
use tokio::net::TcpListener;
use tokio_postgres::{Config as PgConfig, NoTls, Row};
use deadpool_postgres::{Manager, Pool};
use deadpool::managed::Timeouts;
use deadpool::Runtime;
use tokio::sync::Semaphore;

use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireBackendMessage;
use pgwire::api::Type as PgType;
use pgwire::tokio::process_socket;

#[derive(Clone)]
struct ProxyHandler {
    // общий пул или per-user пула ниже
    pool: Pool,
    user_pools: Arc<HashMap<String, Pool>>, // optional: per-user пул
    user_map: Arc<HashMap<String, (String, String)>>, // pgwire_user -> (up_user, up_pass)
    // добавлено: карта ключей/паролей pgwire_user -> key
    auth_keys: Arc<HashMap<String, String>>,
    // добавлено: лимиты на пользователя
    rate_limits: Arc<HashMap<String, u32>>, // pgwire_user -> rpm
    limiter: Arc<DashMap<String, Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>>>,
    concurrency: Arc<Semaphore>, // глобальный лимит одновременных запросов
}

impl PgWireServerHandlers for ProxyHandler {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl SimpleQueryHandler for ProxyHandler {
    async fn do_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let t_start = Instant::now();

        // Получаем user (логин) и database (используем как ключ/пароль)
        let md = client.metadata();
        let user = md.get("user").map(String::as_str).unwrap_or("");
        let key  = md.get("database").map(String::as_str).unwrap_or(""); // ключ/пароль

        info!("req start user='{}' key='{}' query_len={}B", user, key, query.len());

        // 1) Аутентификация: сверяем ключ
        let t_auth = Instant::now();
        if let Some(expected) = self.auth_keys.get(user) {
            if expected != key {
                let dt = t_auth.elapsed();
                error!("auth failed user='{}' expected_key='{}' got='{}' took={:?}", user, expected, key, dt);
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("authentication failed for user '{}'", user),
                ))));
            } else {
                debug!("auth ok user='{}' took={:?}", user, t_auth.elapsed());
            }
        } else {
            let dt = t_auth.elapsed();
            error!("auth denied user='{}' no key configured took={:?}", user, dt);
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("user '{}' is not allowed (no key configured)", user),
            ))));
        }

        // 2) Лимит запросов в минуту
        let t_rl = Instant::now();
        let rpm = *self.rate_limits.get(user).unwrap_or(&60);
        let rl = self.limiter.entry(user.to_string()).or_insert_with(|| {
            let quota = Quota::per_minute(NonZeroU32::new(rpm).unwrap());
            Arc::new(RateLimiter::direct(quota))
        }).clone();
        
        if rl.check().is_err() {
            error!("rate-limit exceeded user='{}' limit={}rpm", user, rpm);
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("rate limit exceeded for user '{}' ({} rpm)", user, rpm),
            ))));
        } else {
            debug!("rate-limit ok user='{}' limit={}rpm took={:?}", user, rpm, t_rl.elapsed());
        }

        // 3) Выбор upstream кредов
        let t_creds = Instant::now();
        let (up_user, _up_pass) = self
            .user_map
            .get(user)
            .cloned()
            .unwrap_or_else(|| {
                let u = std::env::var("PGUPSTREAM_USER").unwrap_or_else(|_| "postgres".into());
                let p = std::env::var("PGUPSTREAM_PASSWORD").unwrap_or_default();
                (u, p)
            });

        if up_user.is_empty() {
            error!("upstream creds missing for pgwire user='{}' took={:?}", user, t_creds.elapsed());
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("no upstream user configured for '{}'. Set PGWIRE_USERS or PGUPSTREAM_USER.", user),
            ))));
        } else {
            debug!("upstream creds selected pgwire='{}' upstream_user='{}' took={:?}", user, up_user, t_creds.elapsed());
        }

        let host = std::env::var("PGUPSTREAM_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let _port: u16 = std::env::var("PGUPSTREAM_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(5432);
        let dbname = std::env::var("PGUPSTREAM_DB").unwrap_or_else(|_| "postgres".into());
        if host.is_empty() || dbname.is_empty() {
            error!("upstream host/db missing host='{}' db='{}' user='{}'", host, dbname, user);
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "upstream host or dbname not configured".to_string(),
            ))));
        }

        info!("SimpleQuery user='{}': {}", user, query);

        // 4) Выполнение запроса в upstream
        use tokio::time::{timeout, Duration};

        // ограничение одновременных запросов с тайм-аутом
        let _t_sem = Instant::now();
        info!("step=acquire_sem start user='{}'", user);
        let permit = match timeout(Duration::from_millis(500), self.concurrency.acquire()).await {
            Ok(p) => { info!("step=acquire_sem ok user='{}'", user); p.unwrap() }
            Err(_) => {
                error!("step=acquire_sem timeout user='{}' permits_left={}", user, self.concurrency.available_permits());
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "concurrency limit",
                ))));
            }
        };

        // выбрать пул
        let pool_ref = self.user_pools.get(user).unwrap_or(&self.pool);
        info!("step=pool_get start user='{}' pool_size={}", user, pool_ref.status().max_size);
        let t_pool = Instant::now();
        let conn = match pool_ref.get().await {
            Ok(c) => {
                info!(
                    "step=pool_get ok user='{}' took={:?} available={}, size={}",
                    user, t_pool.elapsed(), pool_ref.status().available, pool_ref.status().size
                );
                c
            }
            Err(e) => {
                error!(
                    "step=pool_get error user='{}' err={} took={:?} available={}, size={}",
                    user, e, t_pool.elapsed(), pool_ref.status().available, pool_ref.status().size
                );
                drop(permit);
                return Err(PgWireError::ApiError(Box::new(e)));
            }
        };

        // выполнение запроса
        info!("step=query_exec start user='{}'", user);
        let t_query = Instant::now();
        let rows = match timeout(Duration::from_millis(2000), conn.query(query, &[])).await {
            Ok(res) => res.map_err(|e| {
                error!("step=query_exec error user='{}' err={} took={:?}", user, e, t_query.elapsed());
                PgWireError::ApiError(Box::new(e))
            })?,
            Err(_) => {
                error!("step=query_exec timeout user='{}' took={:?}", user, t_query.elapsed());
                drop(permit);
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "query timeout"))));
            }
        };

        drop(permit);
        info!("step=permit_release user='{}'", user);

        // ...формирование ответа...
        // rows уже получены выше через пул

        debug!("Rows fetched: {}", rows.len());

        if rows.is_empty() {
            info!("resp OK (0 rows) user='{}' total_took={:?}", user, t_start.elapsed());
            return Ok(vec![Response::Execution(Tag::new("OK").with_rows(0))]);
        }

        let t_resp = Instant::now();
        let field_info = rows_to_field_info(&rows)?;
        let row_stream = stream::iter(rows.into_iter().map(|row| Ok(row_to_data_row(&row))));
        debug!("resp build took={:?} user='{}'", t_resp.elapsed(), user);

        info!("req done user='{}' rows={} total_took={:?}", user, field_info.len(), t_start.elapsed());
        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(field_info),
            row_stream,
        ))])
    }
}

fn rows_to_field_info(rows: &[Row]) -> PgWireResult<Vec<FieldInfo>> {
    let Some(first) = rows.first() else { return Ok(Vec::new()); };
    
    let cols = first.columns();
    let mut fields = Vec::with_capacity(cols.len());
    
    for col in cols {
        let name = col.name().to_string();
        let ty = PgType::from_oid(col.type_().oid()).unwrap_or(PgType::UNKNOWN);
        fields.push(FieldInfo::new(name, None, None, ty, pgwire::api::results::FieldFormat::Text));
    }
    
    Ok(fields)
}

fn row_to_data_row(row: &Row) -> DataRow {
    let mut encoder = BytesMut::new();
    let col_count = row.len() as i16;
    
    for i in 0..row.len() {
        // Попытка получить как текст
        if let Ok(Some(val)) = row.try_get::<_, Option<String>>(i) {
            let bytes = val.as_bytes();
            encoder.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            encoder.extend_from_slice(bytes);
        } else {
            // NULL значение
            encoder.extend_from_slice(&(-1i32).to_be_bytes());
        }
    }
    
    DataRow::new(encoder, col_count)
}

// Заглушки COPY/Startup больше не требуются для минимального SimpleQuery

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Инициализация логгера: сначала пытаемся загрузить log4rs.yaml, иначе fallback на env_logger
    if let Ok(cfg_path) = std::env::var("LOG4RS_CONFIG") {
        if let Err(e) = log4rs::init_file(cfg_path, Default::default()) {
            eprintln!("log4rs init error: {e}, falling back to env_logger");
            env_logger::init();
        }
    } else if std::path::Path::new("log4rs.yaml").exists() {
        if let Err(e) = log4rs::init_file("log4rs.yaml", Default::default()) {
            eprintln!("log4rs init error: {e}, falling back to env_logger");
            env_logger::init();
        }
    } else {
        env_logger::init();
    }

    let host = std::env::var("PGUPSTREAM_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("PGUPSTREAM_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5432);
    let dbname = std::env::var("PGUPSTREAM_DB").unwrap_or_else(|_| "postgres".into());
    let dbuser = std::env::var("PGUPSTREAM_USER").unwrap_or_else(|_| "postgres".into());

    let listen_addr: SocketAddr = std::env::var("PGWIRE_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:55432".into())
        .parse()
        .expect("invalid PGWIRE_LISTEN");

    info!("Starting pgwire-proxy");
    info!("Upstream: host={} port={} db={} default_user={}", host, port, dbname, dbuser);
    info!("Listening on {}", listen_addr);

    // PGWIRE_USERS: {"alice":["alice_u","alice_pwd"],"bob":["bob_u","bob_pwd"]}
    let user_map_env = std::env::var("PGWIRE_USERS").unwrap_or_default();
    let user_map: Arc<HashMap<String, (String, String)>> = Arc::new(
        if user_map_env.is_empty() {
            HashMap::new()
        } else {
            match serde_json::from_str::<HashMap<String, (String, String)>>(&user_map_env) {
                Ok(m) => m,
                Err(e) => {
                    error!("Invalid PGWIRE_USERS JSON: {}", e);
                    HashMap::new()
                }
            }
        }
    );

    // PGWIRE_KEYS: {"alice":"alice_key","bob":"bob_key"} — ключ/пароль клиента, присылаемый в качестве database
    let auth_keys_env = std::env::var("PGWIRE_KEYS").unwrap_or_default();
    let auth_keys: Arc<HashMap<String, String>> = Arc::new(
        if auth_keys_env.is_empty() {
            HashMap::new()
        } else {
            match serde_json::from_str::<HashMap<String, String>>(&auth_keys_env) {
                Ok(m) => m,
                Err(e) => {
                    error!("Invalid PGWIRE_KEYS JSON: {}", e);
                    HashMap::new()
                }
            }
        }
    );

    // PGWIRE_RATE_LIMITS: {"alice": 120, "bob": 30}
    let rate_env = std::env::var("PGWIRE_RATE_LIMITS").unwrap_or_default();
    let rate_limits: Arc<HashMap<String, u32>> = Arc::new(
        if rate_env.is_empty() {
            HashMap::new()
        } else {
            match serde_json::from_str::<HashMap<String, u32>>(&rate_env) {
                Ok(m) => m,
                Err(e) => {
                    error!("Invalid PGWIRE_RATE_LIMITS JSON: {}", e);
                    HashMap::new()
                }
            }
        }
    );

    let mut base_cfg = PgConfig::new();
    base_cfg.host(host.as_str());
    base_cfg.port(port);
    base_cfg.dbname(dbname.as_str());
    base_cfg.user(std::env::var("PGUPSTREAM_USER").unwrap_or_else(|_| "postgres".into()).as_str());
    if let Ok(p) = std::env::var("PGUPSTREAM_PASSWORD") { if !p.is_empty() { base_cfg.password(p.as_str()); } }

    // размер пула и тайм-аут ожидания из ENV
    let pool_size: usize = std::env::var("PGUPSTREAM_POOL_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let pool_wait_ms: u64 = std::env::var("PGUPSTREAM_POOL_WAIT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);

    let mgr = Manager::new(base_cfg.clone(), NoTls);
    let pool = Pool::builder(mgr)
        .max_size(pool_size)
        .timeouts(Timeouts {
            wait: Some(Duration::from_millis(pool_wait_ms)),
            ..Default::default()
        })
        .runtime(Runtime::Tokio1) // указать рантайм
        .build()
        .unwrap();

    // Прогрев общего пула
    let prewarm = std::env::var("PGUPSTREAM_POOL_PREWARM")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(pool_size / 2); // дефолт 50% пула
    info!("prewarming base pool: target={}", prewarm);
    let mut prewarm_conns = Vec::new();
    for i in 0..prewarm {
        match pool.get().await {
            Ok(conn) => {
                // быстрый ping
                if let Err(e) = conn.simple_query("SELECT 1").await {
                    error!("prewarm base pool conn {} ping error: {}", i, e);
                }
                prewarm_conns.push(conn); // держим соединения до конца прогрева
            }
            Err(e) => {
                error!("prewarm base pool conn {} get error: {}", i, e);
                break;
            }
        }
    }
    info!("base pool prewarmed: size={}, available={}", pool.status().size, pool.status().available);
    drop(prewarm_conns); // возвращаем все соединения в пул

    // Аналогично для per-user пулов
    let mut user_pools_map: HashMap<String, Pool> = HashMap::new();
    for (user, (up_user, up_pass)) in user_map.iter() {
        let mut cfg = PgConfig::new();
        cfg.host(host.as_str());
        cfg.port(port);
        cfg.dbname(dbname.as_str());
        cfg.user(up_user.as_str());
        if !up_pass.is_empty() { cfg.password(up_pass.as_str()); }
        let mgr = Manager::new(cfg, NoTls);
        let user_pool = Pool::builder(mgr)
            .max_size(pool_size)
            .timeouts(Timeouts {
                wait: Some(Duration::from_millis(pool_wait_ms)),
                ..Default::default()
            })
            .runtime(Runtime::Tokio1) // указать рантайм
            .build()
            .unwrap();

        let user_prewarm = prewarm; // или отдельная переменная
        info!("prewarming pool for user='{}': target={}", user, user_prewarm);
        let mut user_prewarm_conns = Vec::new();
        for i in 0..user_prewarm {
            match user_pool.get().await {
                Ok(conn) => {
                    if let Err(e) = conn.simple_query("SELECT 1").await {
                        error!("prewarm user={} conn {} ping error: {}", user, i, e);
                    }
                    user_prewarm_conns.push(conn);
                }
                Err(e) => {
                    error!("prewarm user={} conn {} get error: {}", user, i, e);
                    break;
                }
            }
        }
        info!("user='{}' pool prewarmed: size={}, available={}", user, user_pool.status().size, user_pool.status().available);
        drop(user_prewarm_conns);
        
        user_pools_map.insert(user.clone(), user_pool);
    }

    let max_concurrency: usize = std::env::var("PGWIRE_MAX_CONCURRENCY").ok().and_then(|v| v.parse().ok()).unwrap_or(256);

    let handler = Arc::new(ProxyHandler {
        pool,
        user_pools: Arc::new(user_pools_map),
        user_map,
        auth_keys,
        rate_limits,
        limiter: Arc::new(DashMap::new()),
        concurrency: Arc::new(Semaphore::new(max_concurrency)),
    });

    let listener = TcpListener::bind(listen_addr).await?;
    info!("pgwire proxy listening on {}", listen_addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("Accepted connection from {}", addr);
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, handler).await {
                error!("Connection {} error: {}", addr, e);
            } else {
                info!("Connection {} closed", addr);
            }
        });
    }
}