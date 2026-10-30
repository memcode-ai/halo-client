//! The Halo Agent SDK for Rust.
//!
//! It drives the installed Halo binary (`halo --protocol stream-json`), speaks Halo's machine protocol
//! (newline-delimited JSON on the process's stdin and stdout) and routes approvals, choices and offers to an
//! [`Interactor`]. It holds only process lifecycle, protocol I/O and callbacks: interpretation, permissions,
//! previews, confinement and execution all stay inside Halo.
//!
//! One [`Halo`] owns one long-lived halo process. Create it ONCE, when your program starts, and reuse it; open a
//! [`Session`] per conversation:
//!
//! ```no_run
//! # async fn demo() -> Result<(), halo_sdk::HaloError> {
//! use halo_sdk::{ClientOptions, Halo, SessionOptions};
//! let client = Halo::connect(ClientOptions::default()).await?;      // at startup
//! let session = client.session(SessionOptions { cwd: Some("/path/to/project".into()), ..Default::default() }).await?;
//! let outcome = session.run("what is using port 5432?").await?;
//! println!("{} {}", outcome.kind, outcome.message);
//! session.close().await?;
//! client.close().await;                                                  // at shutdown
//! # Ok(()) }
//! ```
//!
//! Connecting and opening sessions are local: only a turn that needs interpreting reaches the Halo service.
//!
//! Halo controls the machine and account it runs on. Never forward untrusted users' requests to it; a service that
//! takes requests from other people must isolate each user's Halo in a container or VM.

pub mod protocol;

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, watch};

pub use protocol::*;
// the protocol has a struct named Result (a result Halo returns); it stays protocol::Result
#[allow(hidden_glob_reexports)]
use std::result::Result;

/// The official way to install Halo. The SDK never downloads, updates or bundles a binary.
pub const INSTALL_LINE: &str = "curl -fsSL https://memcode.ai/halo/install.sh | sh";

/// The binding's own error codes (Halo's are in [`protocol::code`]).
pub mod local_code {
    pub const HALO_EXITED: &str = "halo_exited";
    pub const CLOSED: &str = "client_closed";
    pub const NOT_INSTALLED: &str = "not_installed";
}

/// Approval decisions. A host cannot change the operation it approves: deny, with a message, and ask again.
pub mod decision {
    pub const ALLOW: &str = "allow";
    pub const ALLOW_SESSION: &str = "allow_session";
    pub const DENY: &str = "deny";
}

const MODEL_PINNING: &str = "model_pinning";
const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// A typed failure: an error envelope from Halo, or a failure of the binding itself.
#[derive(Debug, Clone, PartialEq)]
pub struct HaloError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: i64,
}

impl HaloError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        HaloError { code: code.into(), message: message.into(), retryable: false, retry_after_ms: 0 }
    }
    fn of(d: &ErrorData) -> Self {
        HaloError { code: d.code.clone(), message: d.message.clone(), retryable: d.retryable, retry_after_ms: d.retry_after_ms }
    }
}

impl std::fmt::Display for HaloError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "halo: {}: {}", self.code, self.message)
    }
}

impl std::error::Error for HaloError {}

/// A boxed future, for [`Interactor`] methods.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An Interactor's answer to an approval request.
#[derive(Debug, Clone)]
pub struct Approval {
    pub decision: String,
    pub message: String,
}

/// What Halo asks the host. Defaults: deny, cancel, no.
pub trait Interactor: Send + Sync {
    fn approve(&self, _req: ApprovalRequestData) -> BoxFuture<'_, Approval> {
        Box::pin(async { Approval { decision: decision::DENY.into(), message: String::new() } })
    }
    /// The chosen index, or None to cancel.
    fn choose(&self, _req: ChoiceRequestData) -> BoxFuture<'_, Option<i64>> {
        Box::pin(async { None })
    }
    fn offer(&self, _req: OfferRequestData) -> BoxFuture<'_, bool> {
        Box::pin(async { false })
    }
}

/// A started halo process (injectable for tests).
pub struct ProcessIo {
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    /// Resolves when the process has exited.
    pub exited: BoxFuture<'static, ()>,
    /// Kills the process.
    pub kill: Box<dyn FnOnce() + Send>,
}

/// Starts the process (tests); None runs the binary.
pub type Launch = Arc<dyn Fn(&str, &[String]) -> std::io::Result<ProcessIo> + Send + Sync>;

/// Options of the client and its one halo process.
#[derive(Clone, Default)]
pub struct ClientOptions {
    pub bin_path: Option<PathBuf>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    /// "local" (default) or "hosted".
    pub usage: Option<String>,
    /// Answers approvals, choices and offers for every session that does not set its own; without one Halo never asks.
    pub interactor: Option<Arc<dyn Interactor>>,
    pub require: Vec<String>,
    /// Refuses an installed Halo older than this ("0.2.0"); a development build ("dev") passes.
    pub min_halo_version: Option<String>,
    pub launch: Option<Launch>,
}

/// Options of one logical session.
#[derive(Clone, Default)]
pub struct SessionOptions {
    pub cwd: Option<PathBuf>,
    /// default | dontAsk | acceptEdits | plan | bypassPermissions
    pub permission_mode: Option<String>,
    pub allowed_operations: Vec<String>,
    pub disallowed_operations: Vec<String>,
    pub allow_catastrophic: bool,
    pub allow_unconfined: bool,
    /// CLI settings to inherit: none unless named ("user", "project").
    pub setting_sources: Vec<String>,
    /// Pin an exact public Halo model; None pins the default at the first turn.
    pub model: Option<String>,
    /// Close the session after this long without a turn (None: never).
    pub idle_timeout: Option<Duration>,
    /// Overrides the client's Interactor (the client must have one, so that Halo asks).
    pub interactor: Option<Arc<dyn Interactor>>,
}

/// One protocol event of a turn. Unknown event types pass through (a newer Halo's).
#[derive(Debug, Clone)]
pub struct TurnEvent {
    pub kind: String,
    pub turn_id: String,
    pub id: String,
    pub data: serde_json::Value,
}

// --- accidental per-request construction (advice, never a refusal) ----------------------------------------------

static LIVE_CLIENTS: AtomicUsize = AtomicUsize::new(0);
static WARNED_REUSE: AtomicBool = AtomicBool::new(false);
static RECENT_STARTS: Mutex<Vec<Instant>> = Mutex::new(Vec::new());

fn note_client_start() {
    let live = LIVE_CLIENTS.fetch_add(1, Ordering::SeqCst) + 1;
    let recent = {
        let mut r = RECENT_STARTS.lock().unwrap();
        let now = Instant::now();
        r.push(now);
        r.retain(|t| now.duration_since(*t) < Duration::from_secs(60));
        r.len()
    };
    if (live > 1 || recent >= 5) && !WARNED_REUSE.swap(true, Ordering::SeqCst) {
        eprintln!("halo-sdk: warning: several Halo clients were created in this process. Create one client when your program starts and reuse it: one client keeps one Halo process, with a session per conversation.");
    }
}

// --- discovery -----------------------------------------------------------------------------------------------------

fn executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0).unwrap_or(false)
}

/// The installed halo binary: bin_path, then $HALO_BIN, then `halo` on PATH, then the installer's locations.
pub fn find(bin_path: Option<&Path>) -> Result<PathBuf, HaloError> {
    let missing = || HaloError::new(local_code::NOT_INSTALLED, format!("Halo is not installed (install it with: {INSTALL_LINE})"));
    if let Some(p) = bin_path {
        return if executable(p) { Ok(p.into()) } else { Err(missing()) };
    }
    if let Ok(p) = std::env::var("HALO_BIN") {
        return if executable(Path::new(&p)) { Ok(p.into()) } else { Err(missing()) };
    }
    if let Ok(path) = std::env::var("PATH") {
        for d in std::env::split_paths(&path) {
            let p = d.join("halo");
            if executable(&p) {
                return Ok(p);
            }
        }
    }
    let home = std::env::var("HOME").unwrap_or_default();
    for d in [std::env::var("HALO_INSTALL_DIR").unwrap_or_default(), "/usr/local/bin".into(), format!("{home}/.local/bin")] {
        if !d.is_empty() && executable(&Path::new(&d).join("halo")) {
            return Ok(Path::new(&d).join("halo"));
        }
    }
    Err(missing())
}

fn spawn_launch(bin: &str, args: &[String]) -> std::io::Result<ProcessIo> {
    let mut child = tokio::process::Command::new(bin)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let (kill_tx, kill_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::select! {
            _ = child.wait() => {}
            _ = kill_rx => { let _ = child.start_kill(); let _ = child.wait().await; }
        }
        let _ = done_tx.send(true);
    });
    let mut done = done_rx;
    Ok(ProcessIo {
        stdin: Box::new(stdin),
        stdout: Box::new(stdout),
        exited: Box::pin(async move {
            while !*done.borrow() {
                if done.changed().await.is_err() {
                    break;
                }
            }
        }),
        kill: Box::new(move || {
            let _ = kill_tx.send(());
        }),
    })
}

fn older(have: &str, want: &str) -> bool {
    let parse = |s: &str| -> Vec<i64> {
        s.trim_start_matches('v').split('.').map(|p| p.split('-').next().unwrap_or("0").parse().unwrap_or(0)).collect()
    };
    let (h, w) = (parse(have), parse(want));
    for i in 0..h.len().max(w.len()) {
        let (a, b) = (h.get(i).copied().unwrap_or(0), w.get(i).copied().unwrap_or(0));
        if a != b {
            return a < b;
        }
    }
    false
}

// --- the client ----------------------------------------------------------------------------------------------------

struct ExecInner {
    turn_id: String,
    events: mpsc::UnboundedSender<TurnEvent>,
    result: Mutex<Option<oneshot::Sender<Result<OutcomeData, HaloError>>>>,
    cancel_sent: AtomicBool,
}

impl ExecInner {
    fn finish(&self, r: Result<OutcomeData, HaloError>) {
        if let Some(tx) = self.result.lock().unwrap().take() {
            let _ = tx.send(r);
        }
    }
}

struct SessionInner {
    id: Mutex<String>,
    model: Mutex<String>,
    opts: SessionOptions,
    exec: Mutex<Option<Arc<ExecInner>>>,
    ended: Mutex<Option<HaloError>>,
    warnings: Mutex<Vec<WarningData>>,
}

impl SessionInner {
    fn end(&self, err: HaloError) {
        let mut ended = self.ended.lock().unwrap();
        if ended.is_some() {
            return;
        }
        *ended = Some(err.clone());
        drop(ended);
        if let Some(ex) = self.exec.lock().unwrap().take() {
            ex.finish(Err(err));
        }
    }
}

struct State {
    sessions: HashMap<String, Arc<SessionInner>>,
    creating: HashMap<String, Arc<SessionInner>>,
    waiting: HashMap<String, oneshot::Sender<Envelope>>,
    warnings: Vec<WarningData>,
    seq: HashMap<char, u64>,
    exited: bool,
}

struct Inner {
    opts: ClientOptions,
    // the writer's channel; taking it closes halo's stdin (EOF)
    out: Mutex<Option<mpsc::UnboundedSender<String>>>,
    state: Mutex<State>,
    info: Mutex<InitializedData>,
    kill: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    exited: watch::Receiver<bool>,
    closed: AtomicBool,
    counted: AtomicBool,
}

/// One running halo process. Reuse it: clone it freely (clones share the process) and use it from any task.
#[derive(Clone)]
pub struct Halo {
    inner: Arc<Inner>,
}

fn envelope(kind: &str) -> Envelope {
    Envelope { version: PROTOCOL_VERSION.into(), r#type: kind.into(), ..Default::default() }
}

fn with_data<T: Serialize>(mut e: Envelope, data: &T) -> Envelope {
    e.data = Some(serde_json::to_value(data).expect("serializable"));
    e
}

impl Halo {
    /// Starts the halo process and completes the handshake. Call it once, at startup, and reuse the client. It talks
    /// to nothing but the local process.
    pub async fn connect(opts: ClientOptions) -> Result<Halo, HaloError> {
        let args = vec!["--protocol".to_string(), "stream-json".to_string()];
        let io = match &opts.launch {
            Some(l) => l(&opts.bin_path.clone().unwrap_or_else(|| "halo".into()).to_string_lossy(), &args),
            None => {
                let bin = find(opts.bin_path.as_deref())?;
                spawn_launch(&bin.to_string_lossy(), &args)
            }
        }
        .map_err(|e| HaloError::new(local_code::HALO_EXITED, format!("starting halo: {e}")))?;
        let ProcessIo { mut stdin, stdout, exited, kill } = io;
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
        // one writer: lines from concurrent sessions never interleave
        tokio::spawn(async move {
            while let Some(line) = out_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
            let _ = stdin.shutdown().await;
        });
        let (exit_tx, exit_rx) = watch::channel(false);
        tokio::spawn(async move {
            exited.await;
            let _ = exit_tx.send(true);
        });
        let inner = Arc::new(Inner {
            opts: opts.clone(),
            out: Mutex::new(Some(out_tx)),
            state: Mutex::new(State {
                sessions: HashMap::new(),
                creating: HashMap::new(),
                waiting: HashMap::new(),
                warnings: vec![],
                seq: HashMap::new(),
                exited: false,
            }),
            info: Mutex::new(InitializedData::default()),
            kill: Mutex::new(Some(kill)),
            exited: exit_rx,
            closed: AtomicBool::new(false),
            counted: AtomicBool::new(false),
        });
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), HaloError>>();
        tokio::spawn(read_loop(inner.clone(), stdout, ready_tx));
        let client = Halo { inner };
        let init = InitializeData {
            protocol: PROTOCOL_VERSION.into(),
            client: ClientInfo {
                name: opts.client_name.clone().unwrap_or_else(|| "halo-sdk-rust".into()),
                version: opts.client_version.clone().unwrap_or_default(),
            },
            capabilities: if opts.interactor.is_some() { vec!["approvals".into(), "choices".into(), "offers".into()] } else { vec![] },
            require: opts.require.clone(),
            usage: opts.usage.clone().unwrap_or_default(),
        };
        let handshake = async {
            client.send(with_data(envelope(msg::INITIALIZE), &init))?;
            ready_rx.await.map_err(|_| HaloError::new(local_code::HALO_EXITED, "halo exited before the handshake"))??;
            let info = client.info();
            if info.protocol != PROTOCOL_VERSION {
                return Err(HaloError::new(code::VERSION_MISMATCH, format!("halo speaks machine protocol {}, this SDK {}; run `halo upgrade` or update the SDK", info.protocol, PROTOCOL_VERSION)));
            }
            if let Some(min) = &opts.min_halo_version {
                if info.halo_version != "dev" && older(&info.halo_version, min) {
                    return Err(HaloError::new(code::VERSION_MISMATCH, format!("halo {} is older than {min}; run `halo upgrade`", info.halo_version)));
                }
            }
            Ok(())
        };
        if let Err(e) = handshake.await {
            client.kill().await;
            return Err(e);
        }
        client.inner.counted.store(true, Ordering::SeqCst);
        note_client_start();
        Ok(client)
    }

    /// The handshake: Halo's version and capabilities, and whether it is signed in.
    pub fn info(&self) -> InitializedData {
        self.inner.info.lock().unwrap().clone()
    }

    /// Halo's advice so far about how this client uses it.
    pub fn warnings(&self) -> Vec<WarningData> {
        self.inner.state.lock().unwrap().warnings.clone()
    }

    fn next(&self, kind: char) -> String {
        let mut st = self.inner.state.lock().unwrap();
        let n = st.seq.entry(kind).or_insert(0);
        *n += 1;
        format!("{kind}{n}")
    }

    fn send(&self, e: Envelope) -> Result<(), HaloError> {
        if self.inner.state.lock().unwrap().exited {
            return Err(HaloError::new(local_code::HALO_EXITED, "halo has exited"));
        }
        let mut line = serde_json::to_string(&e).expect("serializable");
        line.push('\n');
        match self.inner.out.lock().unwrap().as_ref() {
            Some(out) => out.send(line).map_err(|_| HaloError::new(local_code::HALO_EXITED, "halo has exited")),
            None => Err(HaloError::new(local_code::CLOSED, "the client is closed")),
        }
    }

    async fn request(&self, e: Envelope) -> Result<Envelope, HaloError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut st = self.inner.state.lock().unwrap();
            if st.exited {
                return Err(HaloError::new(local_code::HALO_EXITED, "halo has exited"));
            }
            st.waiting.insert(e.id.clone(), tx);
        }
        let id = e.id.clone();
        if let Err(err) = self.send(e) {
            self.inner.state.lock().unwrap().waiting.remove(&id);
            return Err(err);
        }
        let reply = rx.await.map_err(|_| HaloError::new(local_code::HALO_EXITED, "halo exited"))?;
        if reply.r#type == msg::ERROR {
            let d: ErrorData = reply.data.as_ref().and_then(|v| serde_json::from_value(v.clone()).ok()).unwrap_or_default();
            return Err(HaloError::of(&d));
        }
        Ok(reply)
    }

    /// Opens a logical session (one conversation). It is local: nothing reaches the Halo service.
    pub async fn session(&self, opts: SessionOptions) -> Result<Session, HaloError> {
        if opts.model.is_some() && !self.info().capabilities.iter().any(|c| c == MODEL_PINNING) {
            return Err(HaloError::new(code::VERSION_MISMATCH, "this Halo cannot pin a model; run `halo upgrade`"));
        }
        let data = CreateSessionData {
            cwd: opts.cwd.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
            permission_mode: opts.permission_mode.clone().unwrap_or_default(),
            allowed_operations: opts.allowed_operations.clone(),
            disallowed_operations: opts.disallowed_operations.clone(),
            allow_catastrophic: opts.allow_catastrophic,
            allow_unconfined: opts.allow_unconfined,
            setting_sources: opts.setting_sources.clone(),
            model: opts.model.clone().unwrap_or_default(),
            idle_timeout_ms: opts.idle_timeout.map(|d| d.as_millis() as i64).unwrap_or(0),
        };
        let id = self.next('s');
        let s = Arc::new(SessionInner {
            id: Mutex::new(String::new()),
            model: Mutex::new(String::new()),
            opts,
            exec: Mutex::new(None),
            ended: Mutex::new(None),
            warnings: Mutex::new(vec![]),
        });
        self.inner.state.lock().unwrap().creating.insert(id.clone(), s.clone());
        let mut e = with_data(envelope(msg::CREATE_SESSION), &data);
        e.id = id.clone();
        let r = self.request(e).await;
        self.inner.state.lock().unwrap().creating.remove(&id);
        r?;
        Ok(Session { client: self.clone(), inner: s })
    }

    /// Shuts the process down: Halo cancels running turns and closes every session, then exits. Idempotent.
    pub async fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            let mut ex = self.inner.exited.clone();
            while !*ex.borrow() {
                if ex.changed().await.is_err() {
                    break;
                }
            }
            return;
        }
        if !self.inner.state.lock().unwrap().exited {
            let mut e = envelope(msg::SHUTDOWN);
            e.id = self.next('q');
            let _ = tokio::time::timeout(CLOSE_GRACE, self.request(e)).await;
        }
        self.kill().await;
    }

    async fn kill(&self) {
        // dropping the writer's channel closes halo's stdin (EOF)
        self.inner.out.lock().unwrap().take();
        let mut ex = self.inner.exited.clone();
        let wait = async {
            while !*ex.borrow() {
                if ex.changed().await.is_err() {
                    break;
                }
            }
        };
        if tokio::time::timeout(CLOSE_GRACE, wait).await.is_err() {
            if let Some(k) = self.inner.kill.lock().unwrap().take() {
                k();
            }
        }
    }
}

async fn read_loop(inner: Arc<Inner>, stdout: Box<dyn AsyncRead + Send + Unpin>, ready: oneshot::Sender<Result<(), HaloError>>) {
    let mut ready = Some(ready);
    let mut lines = BufReader::with_capacity(1 << 16, stdout).lines();
    let mut handshook = false;
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(env) = serde_json::from_str::<Envelope>(&line) else { continue }; // a malformed line is skipped
        if env.r#type.is_empty() {
            continue;
        }
        if !handshook {
            if env.r#type == msg::INITIALIZED {
                handshook = true;
                *inner.info.lock().unwrap() = env.data.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
                if let Some(r) = ready.take() {
                    let _ = r.send(Ok(()));
                }
            } else if env.r#type == msg::ERROR {
                let d: ErrorData = env.data.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
                if let Some(r) = ready.take() {
                    let _ = r.send(Err(HaloError::of(&d)));
                }
            }
            continue;
        }
        route(&inner, env);
    }
    // halo is gone
    let err = HaloError::new(local_code::HALO_EXITED, "halo exited");
    if let Some(r) = ready.take() {
        let _ = r.send(Err(HaloError::new(local_code::HALO_EXITED, "halo exited before the handshake (is it a Halo with machine mode?)")));
    }
    let (sessions, waiting) = {
        let mut st = inner.state.lock().unwrap();
        st.exited = true;
        (std::mem::take(&mut st.sessions), std::mem::take(&mut st.waiting))
    };
    drop(waiting); // their receivers see halo_exited
    for s in sessions.values() {
        s.end(err.clone());
    }
    if inner.counted.load(Ordering::SeqCst) {
        LIVE_CLIENTS.fetch_sub(1, Ordering::SeqCst);
    }
}

fn route(inner: &Arc<Inner>, env: Envelope) {
    let kind = env.r#type.clone();
    let (session, waiter) = {
        let mut st = inner.state.lock().unwrap();
        if kind == msg::SESSION_CREATED {
            if let Some(s) = st.creating.get(&env.id).cloned() {
                let d: SessionCreatedData = env.data.clone().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
                *s.id.lock().unwrap() = d.session_id.clone();
                *s.model.lock().unwrap() = d.model;
                st.sessions.insert(d.session_id, s);
            }
        }
        let session = st.sessions.get(&env.session_id).cloned();
        if kind == msg::WARNING {
            let w: WarningData = env.data.clone().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
            st.warnings.push(w.clone());
            if let Some(s) = &session {
                s.warnings.lock().unwrap().push(w);
            }
            return;
        }
        if kind == msg::SESSION_CLOSED && !env.session_id.is_empty() {
            if let Some(s) = &session {
                s.end(HaloError::new(local_code::CLOSED, "the session is closed"));
            }
            st.sessions.remove(&env.session_id);
        }
        let request = kind == msg::APPROVAL_REQUEST || kind == msg::CHOICE_REQUEST || kind == msg::OFFER_REQUEST;
        let waiter = if !env.id.is_empty() && !request && (kind != msg::ERROR || env.turn_id.is_empty()) {
            st.waiting.remove(&env.id)
        } else {
            None
        };
        (session, waiter)
    };
    if let Some(w) = waiter {
        let _ = w.send(env);
        return;
    }
    let Some(s) = session else { return };
    if kind == msg::APPROVAL_REQUEST || kind == msg::CHOICE_REQUEST || kind == msg::OFFER_REQUEST {
        let client = Halo { inner: inner.clone() };
        tokio::spawn(answer(client, s, env));
        return;
    }
    let ex = s.exec.lock().unwrap().clone();
    let Some(ex) = ex else { return };
    if env.turn_id != ex.turn_id {
        return; // a late event after its outcome
    }
    if kind == msg::OUTCOME {
        let o: OutcomeData = env.data.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        if !o.model.is_empty() {
            *s.model.lock().unwrap() = o.model.clone();
        }
        *s.exec.lock().unwrap() = None;
        ex.finish(Ok(o));
    } else if kind == msg::ERROR {
        let d: ErrorData = env.data.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        *s.exec.lock().unwrap() = None;
        ex.finish(Err(HaloError::of(&d)));
    } else {
        let _ = ex.events.send(TurnEvent { kind, turn_id: env.turn_id, id: env.id, data: env.data.unwrap_or_default() });
    }
}

// answers with the Interactor; with none: deny, cancel, no
async fn answer(client: Halo, s: Arc<SessionInner>, env: Envelope) {
    let it = s.opts.interactor.clone().or_else(|| client.inner.opts.interactor.clone());
    let mut reply = Envelope {
        version: PROTOCOL_VERSION.into(),
        id: env.id.clone(),
        session_id: s.id.lock().unwrap().clone(),
        turn_id: env.turn_id.clone(),
        ..Default::default()
    };
    let data = env.data.clone().unwrap_or_default();
    let body = if env.r#type == msg::APPROVAL_REQUEST {
        reply.r#type = msg::APPROVAL_RESPONSE.into();
        let mut a = Approval { decision: decision::DENY.into(), message: "denied by the SDK default (no Interactor)".into() };
        if let (Some(it), Ok(req)) = (&it, serde_json::from_value::<ApprovalRequestData>(data)) {
            a = it.approve(req).await;
        }
        let decision = if a.decision == decision::ALLOW || a.decision == decision::ALLOW_SESSION { a.decision } else { decision::DENY.into() };
        serde_json::to_value(ApprovalResponseData { decision, message: a.message }).unwrap()
    } else if env.r#type == msg::CHOICE_REQUEST {
        reply.r#type = msg::CHOICE_RESPONSE.into();
        let mut n = None;
        if let (Some(it), Ok(req)) = (&it, serde_json::from_value::<ChoiceRequestData>(data)) {
            n = it.choose(req).await;
        }
        match n {
            Some(i) => serde_json::to_value(ChoiceResponseData { index: i, cancel: false }).unwrap(),
            None => serde_json::to_value(ChoiceResponseData { index: 0, cancel: true }).unwrap(),
        }
    } else {
        reply.r#type = msg::OFFER_RESPONSE.into();
        let mut yes = false;
        if let (Some(it), Ok(req)) = (&it, serde_json::from_value::<OfferRequestData>(data)) {
            yes = it.offer(req).await;
        }
        serde_json::to_value(OfferResponseData { yes }).unwrap()
    };
    reply.data = Some(body);
    let _ = client.send(reply);
}

// --- sessions and turns --------------------------------------------------------------------------------------------

/// One conversation in the client's process: its own context, held results, permissions and model.
#[derive(Clone)]
pub struct Session {
    client: Halo,
    inner: Arc<SessionInner>,
}

impl Session {
    /// The session's id in the protocol.
    pub fn id(&self) -> String {
        self.inner.id.lock().unwrap().clone()
    }

    /// The public Halo model this session is pinned to ("" until the first turn pins the default).
    pub fn model(&self) -> String {
        self.inner.model.lock().unwrap().clone()
    }

    /// Halo's advice about this session.
    pub fn warnings(&self) -> Vec<WarningData> {
        self.inner.warnings.lock().unwrap().clone()
    }

    /// Submits a request in words and waits for its outcome.
    pub async fn run(&self, prompt: &str) -> Result<OutcomeData, HaloError> {
        let mut ex = self.begin(prompt)?;
        while ex.next_event().await.is_some() {}
        ex.outcome().await
    }

    /// Submits a request and returns its Execution at once.
    pub fn begin(&self, prompt: &str) -> Result<Execution, HaloError> {
        if let Some(err) = self.inner.ended.lock().unwrap().clone() {
            return Err(if err.code == local_code::CLOSED { HaloError::new(code::UNKNOWN_SESSION, "the session is closed") } else { err });
        }
        let mut slot = self.inner.exec.lock().unwrap();
        if slot.is_some() {
            return Err(HaloError::new(code::TURN_IN_PROGRESS, "a turn is already running in this session"));
        }
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (res_tx, res_rx) = oneshot::channel();
        let turn_id = self.client.next('t');
        let ex = Arc::new(ExecInner { turn_id: turn_id.clone(), events: ev_tx, result: Mutex::new(Some(res_tx)), cancel_sent: AtomicBool::new(false) });
        *slot = Some(ex.clone());
        drop(slot);
        let mut e = with_data(envelope(msg::USER_TURN), &UserTurnData { text: prompt.into() });
        e.session_id = self.id();
        e.turn_id = turn_id.clone();
        if let Err(err) = self.client.send(e) {
            *self.inner.exec.lock().unwrap() = None;
            return Err(err);
        }
        Ok(Execution { session: self.clone(), inner: ex, events: ev_rx, result: Some(res_rx), outcome: None })
    }

    /// Clears the session's context, held results and remembered approvals; the id and model stay.
    pub async fn reset(&self) -> Result<(), HaloError> {
        if self.inner.exec.lock().unwrap().is_some() {
            return Err(HaloError::new(code::TURN_IN_PROGRESS, "a turn is already running in this session"));
        }
        let mut e = envelope(msg::RESET);
        e.session_id = self.id();
        e.id = self.client.next('r');
        self.client.request(e).await.map(|_| ())
    }

    /// Ends the session (a running turn is cancelled first); the client and its other sessions are untouched.
    pub async fn close(&self) -> Result<(), HaloError> {
        if self.inner.ended.lock().unwrap().is_some() {
            return Ok(());
        }
        let mut e = envelope(msg::CLOSE_SESSION);
        e.session_id = self.id();
        e.id = self.client.next('x');
        match self.client.request(e).await {
            Ok(_) => Ok(()),
            Err(err) if err.code == code::UNKNOWN_SESSION || err.code == local_code::HALO_EXITED => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// One running turn.
pub struct Execution {
    session: Session,
    inner: Arc<ExecInner>,
    events: mpsc::UnboundedReceiver<TurnEvent>,
    result: Option<oneshot::Receiver<Result<OutcomeData, HaloError>>>,
    outcome: Option<Result<OutcomeData, HaloError>>,
}

impl Execution {
    /// The turn's id in the protocol.
    pub fn turn_id(&self) -> &str {
        &self.inner.turn_id
    }

    /// The next event of the turn (turn_started, proposal, step_result, and any newer type); None after it ends.
    pub async fn next_event(&mut self) -> Option<TurnEvent> {
        if let Ok(e) = self.events.try_recv() {
            return Some(e);
        }
        if self.outcome.is_some() {
            return None;
        }
        let rx = self.result.as_mut()?;
        tokio::select! {
            biased;
            e = self.events.recv() => e,
            r = rx => {
                self.outcome = Some(r.unwrap_or_else(|_| Err(HaloError::new(local_code::HALO_EXITED, "halo exited"))));
                self.result = None;
                self.events.try_recv().ok()
            }
        }
    }

    /// The turn's outcome.
    pub async fn outcome(&mut self) -> Result<OutcomeData, HaloError> {
        if self.outcome.is_none() {
            let rx = self.result.take().expect("outcome pending");
            self.outcome = Some(rx.await.unwrap_or_else(|_| Err(HaloError::new(local_code::HALO_EXITED, "halo exited"))));
        }
        self.outcome.clone().unwrap()
    }

    /// Interrupts the turn (idempotent; this session's turn only).
    pub fn cancel(&self) {
        if self.inner.result.lock().unwrap().is_none() || self.inner.cancel_sent.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut e = envelope(msg::CANCEL);
        e.session_id = self.session.id();
        e.turn_id = self.inner.turn_id.clone();
        let _ = self.session.client.send(e);
    }
}
