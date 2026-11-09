//! The SDK's simple surface: [`query`] for one request, [`open`] for a conversation. One Halo process serves
//! the application (started on first use).
//!
//! ```no_run
//! # async fn demo() -> Result<(), halo_sdk::HaloError> {
//! let r = halo_sdk::query("what is using port 5432?", halo_sdk::Options::default()).await?;
//! println!("{}", r.text);
//! # Ok(()) }
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;

use crate::{
    decision, Approval, ApprovalRequestData, BoxFuture, ChoiceRequestData, ClientOptions, Halo, HaloError, Interactor,
    OfferRequestData, OutcomeData, Session, SessionOptions,
};

/// What `can_use_operation` is told about an operation that needs approval.
#[derive(Debug, Clone)]
pub struct OperationRequest {
    pub operations: Vec<String>,
    /// READ | WRITE | DESTRUCTIVE | COMMAND | EXECUTE
    pub effect: String,
    pub reason: String,
    pub command: String,
    pub authorities: Vec<String>,
    pub rememberable: bool,
}

/// The answer to an operation that needs approval.
#[derive(Debug, Clone)]
pub enum Permission {
    Allow,
    /// Allow for the rest of the conversation.
    AllowSession,
    Deny(String),
}

pub type CanUseOperation = Arc<dyn Fn(OperationRequest) -> BoxFuture<'static, Permission> + Send + Sync>;

/// Options for a request or a conversation.
#[derive(Clone, Default)]
pub struct Options {
    pub cwd: Option<PathBuf>,
    pub model: Option<String>,
    /// default | acceptEdits | plan | dontAsk | bypassPermissions
    pub permission_mode: Option<String>,
    /// Run without asking ("git.*", "files.read").
    pub allowed_operations: Vec<String>,
    /// Never run, in every mode ("files.delete").
    pub disallowed_operations: Vec<String>,
    /// Decides what needs approval when the allow list and the mode don't.
    pub can_use_operation: Option<CanUseOperation>,
    /// Lets Halo send a path's real name to the service when it needs it (default: never).
    pub allow_sending_paths: bool,
    pub bin_path: Option<PathBuf>,
}

/// A request's result: the answer as Halo would say it, and the same thing typed.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// done or answered
    pub ok: bool,
    /// done | answered | needs_input | proposed | unsupported | denied | cancelled | failed
    pub status: String,
    /// What Halo would say.
    pub text: String,
    /// Typed rows when the result is a table; otherwise empty.
    pub rows: Vec<serde_json::Map<String, Value>>,
    pub columns: Vec<String>,
    /// A single value (a count, a total, a calculation, yes/no), when the result is one.
    pub value: Option<Value>,
    pub question: Option<String>,
    pub options: Vec<String>,
    pub operation: Option<String>,
    pub command: Option<String>,
    pub session_id: String,
    /// The full outcome.
    pub outcome: OutcomeData,
}

/// Ask Halo one thing and get its typed result. A request Halo declines or can't do is a result (status), not an
/// error; errors are failures to reach Halo.
pub async fn query(prompt: &str, opts: Options) -> Result<QueryResult, HaloError> {
    let s = open(opts).await?;
    let r = s.query(prompt).await;
    let _ = s.close().await;
    r
}

/// Start a conversation: follow-ups ("kill it", "only the PDFs") refer to earlier results. Close it when done.
pub async fn open(opts: Options) -> Result<Session, HaloError> {
    let c = shared(opts.bin_path.clone()).await?;
    let interactor: Arc<dyn Interactor> = Arc::new(OptionsInteractor { can_use: opts.can_use_operation.clone(), paths: opts.allow_sending_paths });
    c.session(SessionOptions {
        cwd: opts.cwd.clone(),
        permission_mode: opts.permission_mode.clone(),
        allowed_operations: opts.allowed_operations.clone(),
        disallowed_operations: opts.disallowed_operations.clone(),
        model: opts.model.clone(),
        interactor: Some(interactor),
        ..Default::default()
    })
    .await
}

impl Session {
    /// Ask within this conversation.
    pub async fn query(&self, prompt: &str) -> Result<QueryResult, HaloError> {
        let o = self.run(prompt).await?;
        Ok(to_result(o, self.id()))
    }
}

static SHARED: Mutex<Option<Halo>> = Mutex::const_new(None);

async fn shared(bin: Option<PathBuf>) -> Result<Halo, HaloError> {
    let mut g = SHARED.lock().await;
    if let Some(c) = g.as_ref() {
        if !c.has_exited() {
            return Ok(c.clone());
        }
    }
    let deny: Arc<dyn Interactor> = Arc::new(OptionsInteractor { can_use: None, paths: false });
    let c = Halo::connect(ClientOptions { bin_path: bin, client_name: Some("halo-sdk-rust".into()), interactor: Some(deny), ..Default::default() }).await?;
    *g = Some(c.clone());
    Ok(c)
}

struct OptionsInteractor {
    can_use: Option<CanUseOperation>,
    paths: bool,
}

impl Interactor for OptionsInteractor {
    fn approve(&self, req: ApprovalRequestData) -> BoxFuture<'_, Approval> {
        Box::pin(async move {
            let Some(f) = &self.can_use else {
                return Approval {
                    decision: decision::DENY.into(),
                    message: "needs approval: allow it with allowed_operations, permission_mode or can_use_operation".into(),
                };
            };
            let op = OperationRequest {
                operations: req.operations.clone(),
                effect: req.effect.clone(),
                reason: req.reason.clone(),
                command: req.plan.as_ref().map(|p| p.command.clone()).unwrap_or_default(),
                authorities: req.authorities.iter().map(|a| a.name.clone()).collect(),
                rememberable: req.rememberable,
            };
            match f(op).await {
                Permission::Allow => Approval { decision: decision::ALLOW.into(), message: String::new() },
                Permission::AllowSession => Approval { decision: decision::ALLOW_SESSION.into(), message: String::new() },
                Permission::Deny(m) => Approval { decision: decision::DENY.into(), message: if m.is_empty() { "denied".into() } else { m } },
            }
        })
    }
    fn choose(&self, _req: ChoiceRequestData) -> BoxFuture<'_, Option<i64>> {
        Box::pin(async { None })
    }
    fn offer(&self, _req: OfferRequestData) -> BoxFuture<'_, bool> {
        let yes = self.paths;
        Box::pin(async move { yes })
    }
}

/// Turn an outcome into a [`QueryResult`].
pub fn to_result(o: OutcomeData, session_id: String) -> QueryResult {
    let status = match o.kind.as_str() {
        "answer" => "answered",
        "result" => "done",
        "question" | "choice" => "needs_input",
        "proposed" => "proposed",
        "unsupported" | "unrecognized" => "unsupported",
        "denied" | "declined" => "denied",
        "cancelled" => "cancelled",
        _ => "failed",
    };
    let v = serde_json::to_value(&o).unwrap_or(Value::Null);
    let res = v.get("result").filter(|r| !r.is_null()).cloned().or_else(|| {
        v.pointer("/program/steps").and_then(|s| s.as_array()).and_then(|steps| steps.iter().rev().find_map(|st| st.get("result").filter(|r| !r.is_null()).cloned()))
    });
    let mut rows: Vec<serde_json::Map<String, Value>> = vec![];
    let mut columns: Vec<String> = vec![];
    let mut value = None;
    let mut note = None;
    let mut rtext = None;
    if let Some(r) = &res {
        rows = r.get("rows").and_then(|x| x.as_array()).map(|a| a.iter().filter_map(|x| x.as_object().cloned()).collect()).unwrap_or_default();
        columns = r.get("columns").and_then(|x| x.as_array()).map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default();
        if rows.is_empty() {
            if let Some(files) = r.get("files").and_then(|x| x.as_array()) {
                for f in files {
                    let mut m = serde_json::Map::new();
                    for k in ["path", "size", "modified"] {
                        m.insert(k.into(), f.get(k).cloned().unwrap_or(Value::Null));
                    }
                    rows.push(m);
                }
                if !rows.is_empty() {
                    columns = vec!["path".into(), "size".into(), "modified".into()];
                }
            }
        }
        if let Some(a) = r.get("answer") {
            value = ["number", "count", "exists"].iter().find_map(|k| a.get(*k).filter(|x| !x.is_null()).cloned());
        }
        note = r.get("note").and_then(|x| x.as_str()).filter(|s| !s.is_empty()).map(String::from);
        rtext = r.get("text").and_then(|x| x.as_str()).filter(|s| !s.is_empty()).map(String::from);
    }
    if !rows.is_empty() && columns.is_empty() {
        columns = rows[0].keys().cloned().collect();
    }
    if value.is_none() && o.kind == "answer" && !o.message.is_empty() {
        value = Some(Value::String(o.message.clone()));
    }
    let text = if !o.message.is_empty() {
        o.message.clone()
    } else if !o.question.is_empty() {
        o.question.clone()
    } else if let Some(n) = note.filter(|_| rows.is_empty() || value.is_some()) {
        n
    } else if let Some(t) = rtext {
        t
    } else if !rows.is_empty() {
        table(&columns, &rows)
    } else {
        o.reason.clone()
    };
    let plan = o.plan.clone();
    QueryResult {
        ok: status == "done" || status == "answered",
        status: status.into(),
        text,
        rows,
        columns,
        value,
        question: Some(o.question.clone()).filter(|s| !s.is_empty()),
        options: o.options.clone(),
        operation: plan.as_ref().map(|p| p.operation.clone()).filter(|s| !s.is_empty()),
        command: Some(o.command.clone()).filter(|s| !s.is_empty()).or_else(|| plan.as_ref().map(|p| p.command.clone()).filter(|s| !s.is_empty())),
        session_id,
        outcome: o,
    }
}

fn table(cols: &[String], rows: &[serde_json::Map<String, Value>]) -> String {
    let cell = |v: Option<&Value>| match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(x) => x.to_string(),
    };
    let w: Vec<usize> = cols.iter().map(|c| rows.iter().map(|r| cell(r.get(c)).len()).max().unwrap_or(0).max(c.len())).collect();
    let line = |vals: Vec<String>| vals.iter().enumerate().map(|(i, v)| format!("{v:<width$}", width = w[i])).collect::<Vec<_>>().join("  ").trim_end().to_string();
    let mut out = vec![line(cols.iter().map(|c| c.to_uppercase()).collect())];
    for r in rows {
        out.push(line(cols.iter().map(|c| cell(r.get(c))).collect()));
    }
    out.join("\n")
}
