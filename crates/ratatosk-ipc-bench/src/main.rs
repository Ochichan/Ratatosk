mod sys;

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use clap::Parser;
use serde::{Deserialize, Serialize};

type BenchError = Box<dyn Error + Send + Sync>;
type BenchResult<T> = Result<T, BenchError>;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUESTS_PER_ROW: usize = 50_000_000;
const SERVER_NAME: &str = "ratatosk";
const REDIS_NAME: &str = "redis";
const UNIX_UNSUPPORTED_REASON: &str =
    "server rejected RATATOSK_UNIXSOCKET (binary predates Unix-socket support)";
const SHM_UNSUPPORTED_REASON: &str =
    "server was built without the shm-transport feature (cargo build --features shm-transport)";
const SHM_REDIS_REASON: &str = "redis-server has no shared-memory transport";
const SET64_VALUE: [u8; 64] = [b'x'; 64];
const SET1K_VALUE: [u8; 1024] = [b'x'; 1024];

#[derive(Debug, Parser)]
#[command(
    name = "ratatosk-ipc-bench",
    about = "Measure lockstep RESP IPC round-trip latency"
)]
struct Cli {
    /// Ratatosk binary. Defaults to RATATOSK_BIN, then target/release/ratatosk.
    #[arg(long)]
    server: Option<PathBuf>,

    /// Optional redis-server binary for comparison rows.
    #[arg(long)]
    redis_server: Option<PathBuf>,

    /// Discover redis-server on PATH and add comparison rows when it is available.
    #[arg(long)]
    with_redis: bool,

    /// Artifact directory. Defaults to benchmarks/ipc below the workspace root.
    #[arg(long)]
    out: Option<PathBuf>,

    #[arg(long, default_value_t = 200_000)]
    samples: usize,

    #[arg(long, default_value_t = 20_000)]
    warmup: usize,

    #[arg(long, default_value = "1,4")]
    conns: String,

    #[arg(long, default_value = "ping,set64,set1k,get64")]
    payloads: String,

    /// Transports to measure: tcp,unix,shm. `shm` needs a server built with
    /// `--features shm-transport`; otherwise its rows are recorded as skipped.
    #[arg(long, default_value = "tcp,unix")]
    transports: String,

    /// Client-side spin iterations before parking on the shared-memory transport.
    /// Higher trades CPU for latency; the server side is fixed by RATATOSK_SHM_SPIN_ITERS.
    #[arg(long, default_value_t = 20_000)]
    shm_client_spin: u32,

    /// Public label included in the artifact and artifact file name.
    #[arg(long, default_value = "local")]
    host_label: String,

    #[arg(long)]
    tag: Option<String>,
}

#[derive(Clone)]
struct Options {
    server: PathBuf,
    redis_server: Option<PathBuf>,
    with_redis: bool,
    out: PathBuf,
    samples: usize,
    warmup: usize,
    connections: Vec<usize>,
    payloads: Vec<Payload>,
    transports: Vec<Transport>,
    shm_client_spin: u32,
    host_label: String,
    tag: Option<String>,
}

impl TryFrom<Cli> for Options {
    type Error = BenchError;

    fn try_from(cli: Cli) -> BenchResult<Self> {
        if cli.samples == 0 {
            return Err(error_message("--samples must be greater than zero"));
        }
        if cli.samples > MAX_REQUESTS_PER_ROW {
            return Err(error_message(format!(
                "--samples must not exceed {MAX_REQUESTS_PER_ROW}"
            )));
        }
        if cli.warmup > MAX_REQUESTS_PER_ROW {
            return Err(error_message(format!(
                "--warmup must not exceed {MAX_REQUESTS_PER_ROW}"
            )));
        }

        let workspace = workspace_root();
        let server = cli
            .server
            .or_else(|| env::var_os("RATATOSK_BIN").map(PathBuf::from))
            .unwrap_or_else(|| workspace.join("target/release/ratatosk"));
        if !server.is_file() {
            return Err(error_message(format!(
                "Ratatosk server binary not found at {}; pass --server <path>, set RATATOSK_BIN, or run cargo build -p ratatosk-server --release",
                server.display()
            )));
        }

        let connections = parse_csv(&cli.conns, "--conns")?
            .into_iter()
            .map(|value| {
                value.parse::<usize>().map_err(|error| {
                    error_message(format!("invalid --conns value {value:?}: {error}"))
                })
            })
            .collect::<BenchResult<Vec<_>>>()?;
        if connections.contains(&0) {
            return Err(error_message("--conns values must be greater than zero"));
        }

        let payloads = parse_csv(&cli.payloads, "--payloads")?
            .into_iter()
            .map(|value| Payload::parse(&value))
            .collect::<BenchResult<Vec<_>>>()?;
        let transports = parse_csv(&cli.transports, "--transports")?
            .into_iter()
            .map(|value| Transport::parse(&value))
            .collect::<BenchResult<Vec<_>>>()?;

        let out = cli.out.unwrap_or_else(|| workspace.join("benchmarks/ipc"));
        Ok(Self {
            server,
            redis_server: cli.redis_server,
            with_redis: cli.with_redis,
            out,
            samples: cli.samples,
            warmup: cli.warmup,
            connections,
            payloads,
            transports,
            shm_client_spin: cli.shm_client_spin,
            host_label: cli.host_label,
            tag: cli.tag,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Payload {
    Ping,
    Set64,
    Set1k,
    Get64,
}

impl Payload {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "ping" => Ok(Self::Ping),
            "set64" => Ok(Self::Set64),
            "set1k" => Ok(Self::Set1k),
            "get64" => Ok(Self::Get64),
            _ => Err(error_message(format!(
                "unsupported payload {value:?}; use ping,set64,set1k,get64"
            ))),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::Set64 => "set64",
            Self::Set1k => "set1k",
            Self::Get64 => "get64",
        }
    }

    const fn expected_reply(self) -> ExpectedReply {
        match self {
            Self::Ping => ExpectedReply::Pong,
            Self::Set64 | Self::Set1k => ExpectedReply::Ok,
            Self::Get64 => ExpectedReply::Bulk64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Tcp,
    Unix,
    Shm,
}

impl Transport {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "tcp" => Ok(Self::Tcp),
            "unix" => Ok(Self::Unix),
            "shm" => Ok(Self::Shm),
            _ => Err(error_message(format!(
                "unsupported transport {value:?}; use tcp,unix,shm"
            ))),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Unix => "unix",
            Self::Shm => "shm",
        }
    }
}

#[derive(Debug, Serialize)]
struct Artifact {
    schema_version: &'static str,
    created_at_utc: String,
    environment: Environment,
    config: ArtifactConfig,
    redis: String,
    resource_usage: ResourceUsage,
    results: Vec<ResultRow>,
}

#[derive(Debug, Serialize)]
struct Environment {
    cpu_model: String,
    os: String,
    kernel_version: String,
    arch: String,
    git_commit: String,
    client_build_profile: String,
    client_rustc_version: String,
    host_label: String,
    server_binary: ServerBinary,
    ratatosk_server_env: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ServerBinary {
    path: String,
    size_bytes: u64,
    sha256: Option<String>,
}

#[derive(Debug, Serialize)]
struct ArtifactConfig {
    server: String,
    samples: usize,
    warmup: usize,
    connections: Vec<usize>,
    payloads: Vec<String>,
    transports: Vec<String>,
    shm_client_spin: u32,
    tag: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResourceUsage {
    client: CpuSeconds,
    server: CpuSeconds,
    server_peak_rss_bytes: u64,
}

#[derive(Debug, Serialize)]
struct CpuSeconds {
    user_seconds: f64,
    system_seconds: f64,
}

#[derive(Debug, Serialize)]
struct ResultRow {
    server: String,
    transport: String,
    payload: String,
    concurrency: usize,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    sample_count: usize,
    warmup_count: usize,
    duration_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p95_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p99_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p99_9_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_us: Option<f64>,
    errors: u64,
    drops: u64,
    retries: u64,
    throughput_req_per_sec: f64,
}

impl ResultRow {
    fn skipped(
        server: &str,
        transport: Transport,
        payload: Payload,
        concurrency: usize,
        reason: &str,
    ) -> Self {
        Self {
            server: server.to_string(),
            transport: transport.label().to_string(),
            payload: payload.label().to_string(),
            concurrency,
            status: "skipped",
            reason: Some(reason.to_string()),
            sample_count: 0,
            warmup_count: 0,
            duration_seconds: 0.0,
            p50_us: None,
            p95_us: None,
            p99_us: None,
            p99_9_us: None,
            max_us: None,
            errors: 0,
            drops: 0,
            retries: 0,
            throughput_req_per_sec: 0.0,
        }
    }
}

#[derive(Clone)]
struct Endpoint {
    tcp_addr: String,
    unix_path: Option<PathBuf>,
    shm_path: Option<PathBuf>,
    shm_client_spin: u32,
}

struct RunningRatatosk {
    process: ManagedProcess,
    endpoint: Endpoint,
    server_env: BTreeMap<String, String>,
    unix_unavailable_reason: Option<String>,
    shm_unavailable_reason: Option<String>,
}

struct RedisServers {
    unix: ManagedProcess,
    tcp: ManagedProcess,
    endpoint: Endpoint,
}

struct ManagedProcess {
    child: Child,
    label: String,
    stderr_path: PathBuf,
}

impl ManagedProcess {
    fn stop(&mut self) -> BenchResult<()> {
        if let Some(status) = self.child.try_wait()? {
            return process_status_result(&self.label, status);
        }

        sys::terminate(self.child.id())?;
        let status = self.child.wait()?;
        process_status_result(&self.label, status)
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        // Release builds use `panic = "abort"`, which skips Drop and can orphan servers on abort.
        let running = self.child.try_wait().ok().flatten().is_none();
        if running {
            let _ = sys::terminate(self.child.id());
            let _ = self.child.wait();
        }
    }
}

struct TempWorkspace {
    root: PathBuf,
    fallback_sockets: Vec<PathBuf>,
}

impl TempWorkspace {
    fn create() -> BenchResult<Self> {
        let root = env::temp_dir().join(format!("ratatosk-ipc-bench-{}", std::process::id()));
        if root.exists() {
            return Err(error_message(format!(
                "benchmark temporary directory already exists: {}; remove it after confirming no benchmark is running",
                root.display()
            )));
        }
        fs::create_dir(&root)?;
        Ok(Self {
            root,
            fallback_sockets: Vec::new(),
        })
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    fn socket_path(&mut self, name: &str) -> PathBuf {
        let candidate = self.root.join(name);
        if unix_path_is_short(&candidate) {
            return candidate;
        }

        let fallback = PathBuf::from("/tmp").join(format!("rti-{}-{name}", std::process::id()));
        self.fallback_sockets.push(fallback.clone());
        fallback
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        // Release builds use `panic = "abort"`, which skips this temporary-workspace cleanup.
        for path in &self.fallback_sockets {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Debug, Deserialize)]
struct BoundAddress {
    bound_addr: String,
    bound_port: u16,
    unixsocket: Option<String>,
    shm_socket: Option<String>,
}

#[derive(Clone)]
struct MeasurementPlan {
    endpoint: Endpoint,
    transport: Transport,
    payload: Payload,
    concurrency: usize,
    samples: usize,
    warmup: usize,
}

struct WorkerResult {
    samples_ns: Vec<u64>,
    duration: Duration,
    counters: Counters,
}

#[derive(Default)]
struct Counters {
    errors: u64,
    drops: u64,
    retries: u64,
}

impl Counters {
    fn merge(&mut self, other: Self) {
        self.errors = self.errors.saturating_add(other.errors);
        self.drops = self.drops.saturating_add(other.drops);
        self.retries = self.retries.saturating_add(other.retries);
    }
}

enum Socket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
    #[cfg(unix)]
    Shm(BlockingShm),
}

/// Blocking adapter over the async shared-memory client. Every call enters a
/// single-threaded tokio runtime via `block_on`; that entry/exit cost is part of
/// the measured client-side latency and is documented in docs/ipc-benchmark.md.
#[cfg(unix)]
struct BlockingShm {
    runtime: tokio::runtime::Runtime,
    stream: ratatosk_shm::ShmStream,
}

#[cfg(unix)]
impl BlockingShm {
    fn connect(path: &Path, spin_iters: u32) -> io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let config = ratatosk_shm::ClientConfig {
            spin_iters,
            ..ratatosk_shm::ClientConfig::default()
        };
        let stream = runtime.block_on(ratatosk_shm::connect_shm(path, &config))?;
        Ok(Self { runtime, stream })
    }
}

#[cfg(unix)]
impl Read for BlockingShm {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        use tokio::io::AsyncReadExt;
        self.runtime.block_on(async {
            tokio::time::timeout(IO_TIMEOUT, self.stream.read(buffer))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shm read timed out"))?
        })
    }
}

#[cfg(unix)]
impl Write for BlockingShm {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        use tokio::io::AsyncWriteExt;
        self.runtime.block_on(async {
            tokio::time::timeout(IO_TIMEOUT, self.stream.write(buffer))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shm write timed out"))?
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for Socket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buffer),
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
            #[cfg(unix)]
            Self::Shm(stream) => stream.read(buffer),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buffer),
            #[cfg(unix)]
            Self::Unix(stream) => stream.write(buffer),
            #[cfg(unix)]
            Self::Shm(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            #[cfg(unix)]
            Self::Unix(stream) => stream.flush(),
            #[cfg(unix)]
            Self::Shm(stream) => stream.flush(),
        }
    }
}

struct RespConnection {
    reader: BufReader<Socket>,
    reply_line: Vec<u8>,
}

impl RespConnection {
    fn connect(endpoint: &Endpoint, transport: Transport) -> io::Result<Self> {
        let socket = match transport {
            Transport::Tcp => {
                let stream = TcpStream::connect(&endpoint.tcp_addr)?;
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(IO_TIMEOUT))?;
                stream.set_write_timeout(Some(IO_TIMEOUT))?;
                Socket::Tcp(stream)
            }
            Transport::Unix => {
                #[cfg(unix)]
                {
                    let path = endpoint.unix_path.as_deref().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, UNIX_UNSUPPORTED_REASON)
                    })?;
                    let stream = UnixStream::connect(path)?;
                    stream.set_read_timeout(Some(IO_TIMEOUT))?;
                    stream.set_write_timeout(Some(IO_TIMEOUT))?;
                    Socket::Unix(stream)
                }
                #[cfg(not(unix))]
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "Unix domain sockets are unavailable on this platform",
                    ));
                }
            }
            Transport::Shm => {
                #[cfg(unix)]
                {
                    let path = endpoint.shm_path.as_deref().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, SHM_UNSUPPORTED_REASON)
                    })?;
                    Socket::Shm(BlockingShm::connect(path, endpoint.shm_client_spin)?)
                }
                #[cfg(not(unix))]
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "shared memory transport is unavailable on this platform",
                    ));
                }
            }
        };
        Ok(Self {
            reader: BufReader::with_capacity(16 * 1024, socket),
            reply_line: Vec::with_capacity(128),
        })
    }

    fn round_trip(&mut self, request: &[u8], expected: ExpectedReply) -> io::Result<()> {
        self.reader.get_mut().write_all(request)?;
        self.reader.get_mut().flush()?;
        self.read_expected(expected)
    }

    fn read_expected(&mut self, expected: ExpectedReply) -> io::Result<()> {
        self.reply_line.clear();
        let read = self.reader.read_until(b'\n', &mut self.reply_line)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed the RESP connection",
            ));
        }

        match expected {
            ExpectedReply::Pong if self.reply_line == b"+PONG\r\n" => Ok(()),
            ExpectedReply::Ok if self.reply_line == b"+OK\r\n" => Ok(()),
            ExpectedReply::Bulk64 if self.reply_line.starts_with(b"$64\r\n") => {
                let mut body = [0_u8; 66];
                self.reader.read_exact(&mut body)?;
                if body[64..] == *b"\r\n" {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "GET bulk reply is missing its CRLF terminator",
                    ))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unexpected RESP reply: {:?}",
                    String::from_utf8_lossy(&self.reply_line)
                ),
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum ExpectedReply {
    Pong,
    Ok,
    Bulk64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ratatosk-ipc-bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let options = Options::try_from(Cli::parse())?;
    fs::create_dir_all(&options.out)?;
    let mut temporary = TempWorkspace::create()?;
    fs::create_dir(temporary.data_dir())?;

    // Take this baseline before any child is spawned. `ru_maxrss` is a high-water mark and
    // cannot be subtracted, so metadata commands run only after the Ratatosk usage is read.
    let children_before = sys::children_usage()?;
    let client_before = sys::self_usage()?;
    let wants_unix = options.transports.contains(&Transport::Unix);
    let wants_shm = options.transports.contains(&Transport::Shm);
    let mut ratatosk = start_ratatosk(&options, &mut temporary, wants_unix, wants_shm)?;

    let mut results = Vec::new();
    let ratatosk_measurement = measure_server(
        SERVER_NAME,
        &ratatosk.endpoint,
        ratatosk.unix_unavailable_reason.as_deref(),
        ratatosk.shm_unavailable_reason.as_deref(),
        &options,
        &mut results,
    );
    let ratatosk_stop = ratatosk.process.stop();
    let ratatosk_children_after = sys::children_usage();

    ratatosk_measurement?;
    ratatosk_stop?;
    let ratatosk_children_after = ratatosk_children_after?;

    let (mut redis, redis_status) = start_optional_redis(&options, &mut temporary);
    let redis_measurement = if let Some(redis) = redis.as_ref() {
        measure_server(
            REDIS_NAME,
            &redis.endpoint,
            None,
            Some(SHM_REDIS_REASON),
            &options,
            &mut results,
        )
    } else {
        Ok(())
    };
    let redis_stop = redis.as_mut().map(RedisServers::stop).transpose();
    let client_after = sys::self_usage();

    redis_measurement?;
    redis_stop?;
    let client_after = client_after?;

    let created_at_utc = utc_timestamp();
    let mut environment = collect_environment(&options)?;
    environment.ratatosk_server_env = redact_temp_workspace_paths(&ratatosk.server_env, &temporary);

    let artifact = Artifact {
        schema_version: "ratatosk-ipc-bench/v1",
        created_at_utc: created_at_utc.clone(),
        environment,
        config: artifact_config(&options),
        redis: redact_temp_workspace_path(&redis_status, &temporary),
        resource_usage: ResourceUsage {
            client: usage_delta(client_after, client_before),
            server: usage_delta(ratatosk_children_after, children_before),
            server_peak_rss_bytes: ratatosk_children_after.max_rss_bytes,
        },
        results,
    };

    let artifact_path = write_artifact(&options.out, &artifact, &created_at_utc)?;
    print_markdown_table(&artifact.results, &artifact.resource_usage);
    println!("artifact: {}", artifact_path.display());
    Ok(())
}

impl RedisServers {
    fn stop(&mut self) -> BenchResult<()> {
        self.tcp.stop()?;
        self.unix.stop()
    }
}

#[allow(clippy::too_many_arguments)]
fn measure_server(
    server: &str,
    endpoint: &Endpoint,
    unix_unavailable_reason: Option<&str>,
    shm_unavailable_reason: Option<&str>,
    options: &Options,
    rows: &mut Vec<ResultRow>,
) -> BenchResult<()> {
    for transport in &options.transports {
        let unavailable = match transport {
            Transport::Unix => unix_unavailable_reason.or_else(|| {
                endpoint
                    .unix_path
                    .is_none()
                    .then_some(UNIX_UNSUPPORTED_REASON)
            }),
            Transport::Shm => shm_unavailable_reason.or_else(|| {
                endpoint
                    .shm_path
                    .is_none()
                    .then_some(SHM_UNSUPPORTED_REASON)
            }),
            Transport::Tcp => None,
        };
        for payload in &options.payloads {
            for concurrency in &options.connections {
                if let Some(reason) = unavailable {
                    rows.push(ResultRow::skipped(
                        server,
                        *transport,
                        *payload,
                        *concurrency,
                        reason,
                    ));
                    continue;
                }
                let plan = MeasurementPlan {
                    endpoint: endpoint.clone(),
                    transport: *transport,
                    payload: *payload,
                    concurrency: *concurrency,
                    samples: options.samples,
                    warmup: options.warmup,
                };
                rows.push(measure_run(server, &plan)?);
            }
        }
    }
    Ok(())
}

fn measure_run(server: &str, plan: &MeasurementPlan) -> BenchResult<ResultRow> {
    let mut connections = Vec::with_capacity(plan.concurrency);
    for worker in 0..plan.concurrency {
        let mut connection = RespConnection::connect(&plan.endpoint, plan.transport)?;
        if plan.payload == Payload::Get64 {
            prepare_get_key(&mut connection, worker)?;
        }
        warm_connection(&mut connection, plan, worker)?;
        connections.push(connection);
    }

    let barrier = Arc::new(Barrier::new(plan.concurrency));
    let mut handles = Vec::with_capacity(plan.concurrency);
    for (worker, connection) in connections.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let worker_plan = plan.clone();
        handles.push(thread::spawn(move || {
            run_worker(connection, worker_plan, worker, barrier)
        }));
    }

    let mut samples_ns = Vec::with_capacity(plan.samples);
    let mut duration = Duration::ZERO;
    let mut counters = Counters::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| error_message("benchmark worker thread panicked"))??;
        duration = duration.max(worker.duration);
        counters.merge(worker.counters);
        samples_ns.extend(worker.samples_ns);
    }

    let summary = LatencySummary::from_samples(&mut samples_ns)?;
    let duration_seconds = duration.as_secs_f64();
    let throughput = if duration_seconds > 0.0 {
        samples_ns.len() as f64 / duration_seconds
    } else {
        0.0
    };
    Ok(ResultRow {
        server: server.to_string(),
        transport: plan.transport.label().to_string(),
        payload: plan.payload.label().to_string(),
        concurrency: plan.concurrency,
        status: "ok",
        reason: None,
        sample_count: samples_ns.len(),
        warmup_count: plan.warmup,
        duration_seconds,
        p50_us: Some(summary.p50_us),
        p95_us: Some(summary.p95_us),
        p99_us: Some(summary.p99_us),
        p99_9_us: Some(summary.p99_9_us),
        max_us: Some(summary.max_us),
        errors: counters.errors,
        drops: counters.drops,
        retries: counters.retries,
        throughput_req_per_sec: throughput,
    })
}

fn warm_connection(
    connection: &mut RespConnection,
    plan: &MeasurementPlan,
    worker: usize,
) -> BenchResult<()> {
    let count = share(plan.warmup, plan.concurrency, worker);
    for request_index in 0..count {
        let request = build_request(plan.payload, worker, request_index);
        connection.round_trip(&request, plan.payload.expected_reply())?;
    }
    Ok(())
}

fn run_worker(
    mut connection: RespConnection,
    plan: MeasurementPlan,
    worker: usize,
    barrier: Arc<Barrier>,
) -> BenchResult<WorkerResult> {
    let sample_count = share(plan.samples, plan.concurrency, worker);
    barrier.wait();
    let started = Instant::now();
    let mut samples_ns = Vec::with_capacity(sample_count);
    let mut counters = Counters::default();
    for request_index in 0..sample_count {
        let request = build_request(plan.payload, worker, request_index);
        let elapsed = measured_round_trip(&mut connection, &plan, worker, &request, &mut counters)?;
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        samples_ns.push(nanos);
    }
    Ok(WorkerResult {
        samples_ns,
        duration: started.elapsed(),
        counters,
    })
}

fn measured_round_trip(
    connection: &mut RespConnection,
    plan: &MeasurementPlan,
    worker: usize,
    request: &[u8],
    counters: &mut Counters,
) -> BenchResult<Duration> {
    let started = Instant::now();
    match connection.round_trip(request, plan.payload.expected_reply()) {
        Ok(()) => Ok(started.elapsed()),
        Err(error) => {
            let original_elapsed = started.elapsed();
            record_io_error(counters, &error);
            if is_timeout(&error) {
                return Err(timeout_error(&error));
            }
            if !is_retryable(&error) {
                return Err(Box::new(error));
            }

            let mut replacement = match RespConnection::connect(&plan.endpoint, plan.transport) {
                Ok(connection) => {
                    counters.retries = counters.retries.saturating_add(1);
                    connection
                }
                Err(error) => {
                    record_io_error(counters, &error);
                    return Err(error_message(format!(
                        "reconnect after retryable I/O error failed: {error}"
                    )));
                }
            };
            if plan.payload == Payload::Get64 {
                if let Err(error) = prepare_get_key(&mut replacement, worker) {
                    record_io_error(counters, &error);
                    if is_timeout(&error) {
                        return Err(timeout_error(&error));
                    }
                    return Err(error_message(format!(
                        "GET setup after reconnect failed: {error}"
                    )));
                }
            }
            *connection = replacement;
            if let Err(error) = connection.round_trip(request, plan.payload.expected_reply()) {
                record_io_error(counters, &error);
                if is_timeout(&error) {
                    return Err(timeout_error(&error));
                }
                return Err(error_message(format!("retry request failed: {error}")));
            }

            // The caller records this interval as the sample. The successful retry restores
            // the connection, but its latency must not hide the failed-attempt stall.
            Ok(original_elapsed)
        }
    }
}

fn record_io_error(counters: &mut Counters, error: &io::Error) {
    counters.errors = counters.errors.saturating_add(1);
    if is_drop(error) {
        counters.drops = counters.drops.saturating_add(1);
    }
}

fn is_drop(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof
    )
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn timeout_error(error: &io::Error) -> BenchError {
    error_message(format!(
        "socket I/O timed out after {} seconds and was not retried: {error}",
        IO_TIMEOUT.as_secs()
    ))
}

fn is_retryable(error: &io::Error) -> bool {
    !matches!(
        error.kind(),
        io::ErrorKind::InvalidData
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::Unsupported
            | io::ErrorKind::TimedOut
            | io::ErrorKind::WouldBlock
    )
}

fn prepare_get_key(connection: &mut RespConnection, worker: usize) -> io::Result<()> {
    let key = get_key(worker);
    let request = resp_command(&[b"SET", key.as_bytes(), &SET64_VALUE]);
    connection.round_trip(&request, ExpectedReply::Ok)?;
    Ok(())
}

fn build_request(payload: Payload, worker: usize, request_index: usize) -> Vec<u8> {
    match payload {
        Payload::Ping => resp_command(&[b"PING"]),
        Payload::Set64 => {
            let key = format!("k:{}", request_key_index(worker, request_index));
            resp_command(&[b"SET", key.as_bytes(), &SET64_VALUE])
        }
        Payload::Set1k => {
            let key = format!("k:{}", request_key_index(worker, request_index));
            resp_command(&[b"SET", key.as_bytes(), &SET1K_VALUE])
        }
        Payload::Get64 => {
            let key = get_key(worker);
            resp_command(&[b"GET", key.as_bytes()])
        }
    }
}

fn request_key_index(worker: usize, request_index: usize) -> usize {
    worker
        .saturating_mul(1_000_000_000)
        .saturating_add(request_index)
}

fn get_key(worker: usize) -> String {
    format!("get64:{worker}")
}

fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut request =
        Vec::with_capacity(parts.iter().map(|part| part.len() + 16).sum::<usize>() + 8);
    request.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        request.extend_from_slice(part);
        request.extend_from_slice(b"\r\n");
    }
    request
}

#[derive(Debug)]
struct LatencySummary {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p99_9_us: f64,
    max_us: f64,
}

impl LatencySummary {
    fn from_samples(samples: &mut [u64]) -> BenchResult<Self> {
        if samples.is_empty() {
            return Err(error_message(
                "no successful latency samples were collected",
            ));
        }
        samples.sort_unstable();
        Ok(Self {
            p50_us: percentile(samples, 0.50) as f64 / 1_000.0,
            p95_us: percentile(samples, 0.95) as f64 / 1_000.0,
            p99_us: percentile(samples, 0.99) as f64 / 1_000.0,
            p99_9_us: percentile(samples, 0.999) as f64 / 1_000.0,
            max_us: samples[samples.len() - 1] as f64 / 1_000.0,
        })
    }
}

fn percentile(samples: &[u64], percentile: f64) -> u64 {
    debug_assert!(!samples.is_empty());
    let rank = (samples.len() as f64 * percentile).ceil() as usize;
    let index = rank.saturating_sub(1).min(samples.len().saturating_sub(1));
    samples[index]
}

fn share(total: usize, participants: usize, participant: usize) -> usize {
    let base = total / participants;
    base + usize::from(participant < total % participants)
}

fn start_ratatosk(
    options: &Options,
    temporary: &mut TempWorkspace,
    wants_unix: bool,
    wants_shm: bool,
) -> BenchResult<RunningRatatosk> {
    let bound_file = temporary.root.join("bound-addr.json");
    let unix_path = wants_unix.then(|| temporary.socket_path("ratatosk.sock"));
    let shm_path = wants_shm.then(|| temporary.socket_path("shm.sock"));
    let mut shm_unavailable_reason = None;

    // First attempt: everything requested. A server built without the
    // `shm-transport` feature rejects RATATOSK_SHM_SOCKET at config validation, so
    // retry without it and record the shm rows as skipped.
    let mut server_env = ratatosk_env(
        &temporary.data_dir(),
        &bound_file,
        unix_path.as_deref(),
        shm_path.as_deref(),
    );
    let mut attempt =
        start_ratatosk_once(&options.server, &temporary.root, &bound_file, &server_env);
    if let Err(error) = &attempt {
        if wants_shm && shm_startup_rejected(error.as_ref()) {
            shm_unavailable_reason = Some(SHM_UNSUPPORTED_REASON.to_string());
            server_env = ratatosk_env(
                &temporary.data_dir(),
                &bound_file,
                unix_path.as_deref(),
                None,
            );
            attempt =
                start_ratatosk_once(&options.server, &temporary.root, &bound_file, &server_env);
        }
    }
    match attempt {
        Ok((process, bound)) => {
            let reported_unix = bound
                .unixsocket
                .map(PathBuf::from)
                .filter(|path| path.exists());
            let reported_shm = bound
                .shm_socket
                .map(PathBuf::from)
                .filter(|path| path.exists());
            let unix_unavailable_reason = if wants_unix && reported_unix.is_none() {
                Some(UNIX_UNSUPPORTED_REASON.to_string())
            } else {
                None
            };
            if wants_shm && reported_shm.is_none() && shm_unavailable_reason.is_none() {
                shm_unavailable_reason = Some(SHM_UNSUPPORTED_REASON.to_string());
            }
            Ok(RunningRatatosk {
                process,
                endpoint: Endpoint {
                    tcp_addr: bound.bound_addr,
                    unix_path: reported_unix,
                    shm_path: reported_shm,
                    shm_client_spin: options.shm_client_spin,
                },
                server_env,
                unix_unavailable_reason,
                shm_unavailable_reason,
            })
        }
        Err(error) if wants_unix && unix_startup_rejected(error.as_ref()) => {
            let server_env = ratatosk_env(&temporary.data_dir(), &bound_file, None, None);
            let (process, bound) =
                start_ratatosk_once(&options.server, &temporary.root, &bound_file, &server_env)?;
            Ok(RunningRatatosk {
                process,
                endpoint: Endpoint {
                    tcp_addr: bound.bound_addr,
                    unix_path: None,
                    shm_path: None,
                    shm_client_spin: options.shm_client_spin,
                },
                server_env,
                unix_unavailable_reason: Some(UNIX_UNSUPPORTED_REASON.to_string()),
                shm_unavailable_reason: wants_shm.then(|| SHM_UNSUPPORTED_REASON.to_string()),
            })
        }
        Err(error) => Err(error),
    }
}

fn start_ratatosk_once(
    binary: &Path,
    root: &Path,
    bound_file: &Path,
    server_env: &BTreeMap<String, String>,
) -> BenchResult<(ManagedProcess, BoundAddress)> {
    match fs::remove_file(bound_file) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(Box::new(error)),
    }
    let mut process = spawn_managed("ratatosk", binary, root, server_env)?;
    let bound = wait_for_bound_file(bound_file, &mut process)?;
    if bound.bound_port == 0 {
        return Err(error_message(format!(
            "Ratatosk wrote an invalid bound port in {}",
            bound_file.display()
        )));
    }
    Ok((process, bound))
}

fn spawn_managed(
    label: &str,
    binary: &Path,
    root: &Path,
    server_env: &BTreeMap<String, String>,
) -> BenchResult<ManagedProcess> {
    let stdout_path = root.join(format!("{label}.stdout.log"));
    let stderr_path = root.join(format!("{label}.stderr.log"));
    let stdout = File::create(stdout_path)?;
    let stderr = File::create(&stderr_path)?;
    let mut command = Command::new(binary);
    clear_ratatosk_environment(&mut command);
    command
        .envs(server_env)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let child = command.spawn()?;
    Ok(ManagedProcess {
        child,
        label: label.to_string(),
        stderr_path,
    })
}

fn clear_ratatosk_environment(command: &mut Command) {
    for (name, _) in env::vars_os() {
        if name.to_string_lossy().starts_with("RATATOSK_") {
            command.env_remove(name);
        }
    }
}

fn wait_for_bound_file(path: &Path, process: &mut ManagedProcess) -> BenchResult<BoundAddress> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        match fs::read_to_string(path) {
            Ok(contents) => {
                return serde_json::from_str(&contents).map_err(|error| {
                    error_message(format!(
                        "invalid bound address file {}: {error}",
                        path.display()
                    ))
                });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Box::new(error)),
        }
        if let Some(status) = process.child.try_wait()? {
            return Err(error_message(format!(
                "{} exited before writing {} ({status}); stderr: {}",
                process.label,
                path.display(),
                stderr_tail(&process.stderr_path)
            )));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(error_message(format!(
        "timed out after {} seconds waiting for {}; stderr: {}",
        STARTUP_TIMEOUT.as_secs(),
        path.display(),
        stderr_tail(&process.stderr_path)
    )))
}

fn ratatosk_env(
    data_dir: &Path,
    bound_file: &Path,
    unix_path: Option<&Path>,
    shm_path: Option<&Path>,
) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    values.insert("RATATOSK_BIND".to_string(), "127.0.0.1".to_string());
    values.insert("RATATOSK_PORT".to_string(), "0".to_string());
    values.insert("RATATOSK_DIR".to_string(), data_dir.display().to_string());
    values.insert(
        "RATATOSK_BOUND_ADDR_FILE".to_string(),
        bound_file.display().to_string(),
    );
    values.insert(
        "RATATOSK_DISABLE_CONFIG_AUTOLOAD".to_string(),
        "true".to_string(),
    );
    values.insert(
        "RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS".to_string(),
        "100000".to_string(),
    );
    values.insert(
        "RATATOSK_CONN_RATE_LIMIT_WINDOW_SEC".to_string(),
        "60".to_string(),
    );
    values.insert(
        "RATATOSK_METRICS_BIND".to_string(),
        "127.0.0.1:0".to_string(),
    );
    values.insert("RATATOSK_ALLOW_NO_METRICS".to_string(), "true".to_string());
    values.insert(
        "RATATOSK_AUDIT_LOG".to_string(),
        data_dir.join("audit.log").display().to_string(),
    );
    values.insert(
        "RATATOSK_AUDIT_CHAIN_STATE".to_string(),
        data_dir.join("audit.state").display().to_string(),
    );
    values.insert("RATATOSK_SHUTDOWN_GRACE_MS".to_string(), "1000".to_string());
    if let Some(unix_path) = unix_path {
        values.insert(
            "RATATOSK_UNIXSOCKET".to_string(),
            unix_path.display().to_string(),
        );
        values.insert("RATATOSK_UNIXSOCKETPERM".to_string(), "700".to_string());
    }
    if let Some(shm_path) = shm_path {
        values.insert(
            "RATATOSK_SHM_SOCKET".to_string(),
            shm_path.display().to_string(),
        );
    }
    values
}

fn start_optional_redis(
    options: &Options,
    temporary: &mut TempWorkspace,
) -> (Option<RedisServers>, String) {
    let requested = options.redis_server.is_some() || options.with_redis;
    let Some(binary) = resolve_redis_binary(options) else {
        return (
            None,
            if requested {
                "skipped: redis-server not found".to_string()
            } else {
                "not requested".to_string()
            },
        );
    };

    match start_redis_servers(&binary, temporary) {
        Ok(redis) => (Some(redis), "enabled".to_string()),
        Err(error) => (None, format!("skipped: redis-server unavailable ({error})")),
    }
}

fn resolve_redis_binary(options: &Options) -> Option<PathBuf> {
    if let Some(path) = &options.redis_server {
        if path.components().count() > 1 {
            return path.is_file().then(|| path.clone());
        }
        return find_on_path(path);
    }
    options
        .with_redis
        .then(|| find_on_path(Path::new("redis-server")))
        .flatten()
}

fn find_on_path(program: &Path) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
}

fn start_redis_servers(binary: &Path, temporary: &mut TempWorkspace) -> BenchResult<RedisServers> {
    let unix_path = temporary.socket_path("redis.sock");
    let unix_dir = temporary.root.join("redis-unix");
    let tcp_dir = temporary.root.join("redis-tcp");
    fs::create_dir(&unix_dir)?;
    fs::create_dir(&tcp_dir)?;

    let mut unix = spawn_redis(
        "redis-unix",
        binary,
        &temporary.root,
        &redis_args(
            &["--port", "0", "--unixsocket"],
            Some(&unix_path),
            &unix_dir,
        ),
    )?;
    if let Err(error) = wait_for_unix_socket(&unix_path, &mut unix) {
        let _ = unix.stop();
        return Err(error);
    }

    let port = reserve_tcp_port()?;
    let port_text = port.to_string();
    let mut tcp = spawn_redis(
        "redis-tcp",
        binary,
        &temporary.root,
        &redis_args(&["--port"], Some(Path::new(&port_text)), &tcp_dir),
    )?;
    if let Err(error) = wait_for_tcp_listener(port, &mut tcp) {
        let _ = tcp.stop();
        let _ = unix.stop();
        return Err(error);
    }

    Ok(RedisServers {
        unix,
        tcp,
        endpoint: Endpoint {
            tcp_addr: format!("127.0.0.1:{port}"),
            unix_path: Some(unix_path),
            shm_path: None,
            shm_client_spin: 0,
        },
    })
}

fn spawn_redis(
    label: &str,
    binary: &Path,
    root: &Path,
    arguments: &[OsString],
) -> BenchResult<ManagedProcess> {
    let stdout_path = root.join(format!("{label}.stdout.log"));
    let stderr_path = root.join(format!("{label}.stderr.log"));
    let stdout = File::create(stdout_path)?;
    let stderr = File::create(&stderr_path)?;
    let mut command = Command::new(binary);
    command
        .args(arguments)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let child = command.spawn()?;
    Ok(ManagedProcess {
        child,
        label: label.to_string(),
        stderr_path,
    })
}

fn redis_args(leading_args: &[&str], value: Option<&Path>, directory: &Path) -> Vec<OsString> {
    let mut arguments = leading_args.iter().map(OsString::from).collect::<Vec<_>>();
    if let Some(value) = value {
        arguments.push(value.as_os_str().to_owned());
    }
    arguments.extend([
        OsString::from("--bind"),
        OsString::from("127.0.0.1"),
        OsString::from("--save"),
        OsString::new(),
        OsString::from("--appendonly"),
        OsString::from("no"),
        OsString::from("--dir"),
        directory.as_os_str().to_owned(),
    ]);
    arguments
}

fn wait_for_unix_socket(path: &Path, process: &mut ManagedProcess) -> BenchResult<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        if let Some(status) = process.child.try_wait()? {
            return Err(error_message(format!(
                "{} exited before binding {} ({status}); stderr: {}",
                process.label,
                path.display(),
                stderr_tail(&process.stderr_path)
            )));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(error_message(format!(
        "timed out after {} seconds waiting for {}; stderr: {}",
        STARTUP_TIMEOUT.as_secs(),
        path.display(),
        stderr_tail(&process.stderr_path)
    )))
}

fn wait_for_tcp_listener(port: u16, process: &mut ManagedProcess) -> BenchResult<()> {
    let address = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        if TcpStream::connect(&address).is_ok() {
            return Ok(());
        }
        if let Some(status) = process.child.try_wait()? {
            return Err(error_message(format!(
                "{} exited before binding {address} ({status}); stderr: {}",
                process.label,
                stderr_tail(&process.stderr_path)
            )));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(error_message(format!(
        "timed out after {} seconds waiting for {address}; stderr: {}",
        STARTUP_TIMEOUT.as_secs(),
        stderr_tail(&process.stderr_path)
    )))
}

fn reserve_tcp_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    listener.local_addr().map(|address| address.port())
}

fn shm_startup_rejected(error: &(dyn Error + Send + Sync)) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("shm-transport")
}

fn unix_startup_rejected(error: &(dyn Error + Send + Sync)) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("ratatosk_unixsocket")
}

fn process_status_result(label: &str, status: std::process::ExitStatus) -> BenchResult<()> {
    if status.success() {
        Ok(())
    } else {
        Err(error_message(format!(
            "{label} exited unsuccessfully: {status}"
        )))
    }
}

fn stderr_tail(path: &Path) -> String {
    let contents = fs::read_to_string(path).unwrap_or_else(|_| "<unavailable>".to_string());
    const MAX_CHARS: usize = 1_000;
    if contents.chars().count() <= MAX_CHARS {
        contents.trim().to_string()
    } else {
        contents
            .chars()
            .rev()
            .take(MAX_CHARS)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>()
    }
}

fn collect_environment(options: &Options) -> BenchResult<Environment> {
    let workspace = workspace_root();
    Ok(Environment {
        cpu_model: cpu_model(),
        os: env::consts::OS.to_string(),
        kernel_version: command_text("uname", &["-r"]).unwrap_or_else(|| "unknown".to_string()),
        arch: env::consts::ARCH.to_string(),
        git_commit: git_description(&workspace),
        client_build_profile: option_env!("PROFILE")
            .unwrap_or(if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            })
            .to_string(),
        client_rustc_version: command_text("rustc", &["--version"])
            .unwrap_or_else(|| "unknown".to_string()),
        host_label: options.host_label.clone(),
        server_binary: server_binary(&options.server, &workspace)?,
        ratatosk_server_env: BTreeMap::new(),
    })
}

fn git_description(workspace: &Path) -> String {
    command_text_in(workspace, "git", &["describe", "--always", "--dirty"]).unwrap_or_else(|| {
        let revision = command_text_in(workspace, "git", &["rev-parse", "HEAD"])
            .unwrap_or_else(|| "unknown".to_string());
        if git_worktree_is_dirty(workspace) {
            format!("{revision}-dirty")
        } else {
            revision
        }
    })
}

fn git_worktree_is_dirty(workspace: &Path) -> bool {
    Command::new("git")
        .current_dir(workspace)
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| !String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn server_binary(server: &Path, workspace: &Path) -> BenchResult<ServerBinary> {
    let path = artifact_binary_path(server, workspace);
    let size_bytes = fs::metadata(server)?.len();
    Ok(ServerBinary {
        path,
        size_bytes,
        sha256: sha256_file(server),
    })
}

fn artifact_binary_path(server: &Path, workspace: &Path) -> String {
    let canonical_server = server
        .canonicalize()
        .unwrap_or_else(|_| server.to_path_buf());
    canonical_server
        .strip_prefix(workspace)
        .ok()
        .filter(|relative| !relative.as_os_str().is_empty())
        .map(|relative| relative.display().to_string())
        .or_else(|| {
            canonical_server
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn sha256_file(path: &Path) -> Option<String> {
    sha256_from_command("shasum", &["-a", "256"], path)
        .or_else(|| sha256_from_command("sha256sum", &[], path))
}

fn sha256_from_command(program: &str, arguments: &[&str], path: &Path) -> Option<String> {
    let output = Command::new(program)
        .args(arguments)
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let digest = stdout.split_whitespace().next()?;
    (digest.len() == 64
        && digest
            .chars()
            .all(|character| character.is_ascii_hexdigit()))
    .then(|| digest.to_ascii_lowercase())
}

fn redact_temp_workspace_paths(
    values: &BTreeMap<String, String>,
    temporary: &TempWorkspace,
) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), redact_temp_workspace_path(value, temporary)))
        .collect()
}

fn redact_temp_workspace_path(value: &str, temporary: &TempWorkspace) -> String {
    let mut redacted = value.to_string();
    for temporary_path in std::iter::once(&temporary.root).chain(temporary.fallback_sockets.iter())
    {
        let prefix = temporary_path.to_string_lossy();
        redacted = redacted.replace(prefix.as_ref(), "<tmp>");
    }
    redacted
}

#[cfg(target_os = "macos")]
fn cpu_model() -> String {
    command_text("sysctl", &["-n", "machdep.cpu.brand_string"])
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(target_os = "linux")]
fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    (name.trim() == "model name").then(|| value.trim().to_string())
                })
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn cpu_model() -> String {
    "unknown".to_string()
}

fn command_text(program: &str, arguments: &[&str]) -> Option<String> {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
}

fn command_text_in(directory: &Path, program: &str, arguments: &[&str]) -> Option<String> {
    Command::new(program)
        .current_dir(directory)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
}

fn artifact_config(options: &Options) -> ArtifactConfig {
    ArtifactConfig {
        server: artifact_binary_path(&options.server, &workspace_root()),
        samples: options.samples,
        warmup: options.warmup,
        connections: options.connections.clone(),
        payloads: options
            .payloads
            .iter()
            .map(|payload| payload.label().to_string())
            .collect(),
        transports: options
            .transports
            .iter()
            .map(|transport| transport.label().to_string())
            .collect(),
        shm_client_spin: options.shm_client_spin,
        tag: options.tag.clone(),
    }
}

fn usage_delta(after: sys::Usage, before: sys::Usage) -> CpuSeconds {
    CpuSeconds {
        user_seconds: (after.user_seconds - before.user_seconds).max(0.0),
        system_seconds: (after.system_seconds - before.system_seconds).max(0.0),
    }
}

fn write_artifact(out: &Path, artifact: &Artifact, timestamp: &str) -> BenchResult<PathBuf> {
    let host_label = file_component(&artifact.environment.host_label);
    let tag = artifact
        .config
        .tag
        .as_deref()
        .map(file_component)
        .filter(|tag| !tag.is_empty())
        .map(|tag| format!("-{tag}"))
        .unwrap_or_default();
    let path = out.join(format!("{timestamp}-{host_label}{tag}.json"));
    let document = serde_json::to_vec_pretty(artifact)?;
    fs::write(&path, document)?;
    fs::copy(&path, out.join("latest.json"))?;
    Ok(path)
}

fn utc_timestamp() -> String {
    command_text("date", &["-u", "+%Y%m%dT%H%M%SZ"]).unwrap_or_else(|| "unknown-utc".to_string())
}

fn file_component(value: &str) -> String {
    let component = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    component.trim_matches('-').to_string()
}

fn print_markdown_table(rows: &[ResultRow], usage: &ResourceUsage) {
    let client_cpu = usage.client.user_seconds + usage.client.system_seconds;
    let server_cpu = usage.server.user_seconds + usage.server.system_seconds;
    println!(
        "| Server | Transport | Payload | Conns | p50 us | p99 us | p99.9 us | max us | Client CPU s* | Server CPU s* |"
    );
    println!("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for row in rows {
        let metrics = match (row.p50_us, row.p99_us, row.p99_9_us, row.max_us) {
            (Some(p50), Some(p99), Some(p99_9), Some(max)) => {
                format!("{p50:.3} | {p99:.3} | {p99_9:.3} | {max:.3}")
            }
            _ => "- | - | - | -".to_string(),
        };
        println!(
            "| {} | {} | {} | {} | {} | {:.3} | {:.3} |",
            row.server,
            row.transport,
            row.payload,
            row.concurrency,
            metrics,
            client_cpu,
            server_cpu
        );
        if let Some(reason) = &row.reason {
            println!("<!-- skipped: {reason} -->");
        }
    }
    println!(
        "*CPU values are aggregate user+system seconds for this invocation; server peak RSS: {} bytes.",
        usage.server_peak_rss_bytes
    );
}

fn parse_csv(value: &str, flag: &str) -> BenchResult<Vec<String>> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(error_message(format!(
            "{flag} must contain at least one value"
        )));
    }
    Ok(values)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

fn unix_path_is_short(path: &Path) -> bool {
    path.as_os_str().as_encoded_bytes().len() < 100
}

fn error_message(message: impl Into<String>) -> BenchError {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        Cli, MAX_REQUESTS_PER_ROW, Options, TempWorkspace, artifact_binary_path, percentile,
        redact_temp_workspace_path,
    };

    #[test]
    fn percentile_uses_nearest_rank() {
        let samples = [1, 2, 3, 4];
        assert_eq!(percentile(&samples, 0.50), 2);
        assert_eq!(percentile(&samples, 0.99), 4);

        let two_samples = [10, 20];
        assert_eq!(percentile(&two_samples, 0.50), 10);
    }

    #[test]
    fn options_reject_request_counts_above_the_bound() {
        let samples_error = Options::try_from(test_cli(MAX_REQUESTS_PER_ROW + 1, 0))
            .err()
            .expect("samples above the bound must fail")
            .to_string();
        assert!(samples_error.contains("--samples must not exceed"));

        let warmup_error = Options::try_from(test_cli(1, MAX_REQUESTS_PER_ROW + 1))
            .err()
            .expect("warmup above the bound must fail")
            .to_string();
        assert!(warmup_error.contains("--warmup must not exceed"));
    }

    #[test]
    fn redacts_workspace_and_all_fallback_socket_paths() {
        let temporary = TempWorkspace {
            root: PathBuf::from("/var/folders/example/ratatosk-ipc-bench-1"),
            fallback_sockets: vec![
                PathBuf::from("/tmp/rti-1-ratatosk.sock"),
                PathBuf::from("/tmp/rti-1-shm.sock"),
            ],
        };

        assert_eq!(
            redact_temp_workspace_path(
                "/var/folders/example/ratatosk-ipc-bench-1/data/audit.log",
                &temporary,
            ),
            "<tmp>/data/audit.log"
        );
        assert_eq!(
            redact_temp_workspace_path("/tmp/rti-1-ratatosk.sock", &temporary),
            "<tmp>"
        );
        assert_eq!(
            redact_temp_workspace_path(
                "skipped: redis-server unavailable (/tmp/rti-1-shm.sock failed)",
                &temporary,
            ),
            "skipped: redis-server unavailable (<tmp> failed)"
        );
    }

    #[test]
    fn server_paths_in_artifacts_are_public_safe() {
        let workspace = Path::new("/workspace");
        assert_eq!(
            artifact_binary_path(Path::new("/workspace/target/release/ratatosk"), workspace),
            "target/release/ratatosk"
        );
        assert_eq!(
            artifact_binary_path(Path::new("/Users/alice/private/ratatosk"), workspace),
            "ratatosk"
        );
    }

    fn test_cli(samples: usize, warmup: usize) -> Cli {
        Cli {
            server: None,
            redis_server: None,
            with_redis: false,
            out: None,
            samples,
            warmup,
            conns: "1".to_string(),
            payloads: "ping".to_string(),
            transports: "tcp".to_string(),
            shm_client_spin: 20_000,
            host_label: "local".to_string(),
            tag: None,
        }
    }
}
