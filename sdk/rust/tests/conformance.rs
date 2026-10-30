//! Replays every binding conformance transcript (../conformance/*.jsonl) through the public API: a fake process plays
//! halo's lines, the binding is driven from the host's lines, and it must send exactly the transcript's envelopes and
//! report the same events and outcomes.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use halo_sdk::*;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{watch, Notify};

fn canonical(v: &Value) -> String {
    // serde_json's Map is a BTreeMap: keys come out sorted
    serde_json::to_string(v).unwrap()
}

struct Fake {
    read: watch::Sender<usize>,
    errs: Arc<Mutex<Vec<String>>>,
    done: watch::Receiver<bool>,
}

fn fake(lines: Vec<Value>) -> (Fake, ProcessIo, tokio::task::JoinHandle<()>) {
    let (bind_in, mut halo_in) = tokio::io::duplex(1 << 20); // binding writes, fake reads
    let (mut halo_out, bind_out) = tokio::io::duplex(1 << 20); // fake writes, binding reads
    let (read_tx, _) = watch::channel(0usize);
    let (done_tx, done_rx) = watch::channel(false);
    let errs = Arc::new(Mutex::new(vec![]));
    let (errs2, read2) = (errs.clone(), read_tx.clone());
    let task = tokio::spawn(async move {
        let mut reader = BufReader::new(&mut halo_in).lines();
        for (i, l) in lines.iter().enumerate() {
            if let Some(o) = l.get("out") {
                let _ = halo_out.write_all(format!("{}\n", canonical(o)).as_bytes()).await;
            } else if let Some(raw) = l.get("out_raw") {
                let _ = halo_out.write_all(format!("{}\n", raw.as_str().unwrap()).as_bytes()).await;
            } else if let Some(want) = l.get("in") {
                match reader.next_line().await {
                    Ok(Some(got)) => {
                        let got: Value = serde_json::from_str(&got).unwrap();
                        if canonical(&got) != canonical(want) {
                            errs2.lock().unwrap().push(format!("line {}: the binding sent\n  {}\nwant\n  {}", i + 2, canonical(&got), canonical(want)));
                            break;
                        }
                        read2.send_replace(i + 1);
                    }
                    _ => {
                        errs2.lock().unwrap().push(format!("line {}: the binding sent nothing, want {}", i + 2, canonical(want)));
                        break;
                    }
                }
            } else if l.get("eof").is_some() {
                if let Ok(Some(got)) = reader.next_line().await {
                    errs2.lock().unwrap().push(format!("line {}: want EOF from the binding, got {got}", i + 2));
                }
                break;
            } else if l.get("halo_exit").is_some() {
                break;
            }
        }
        drop(halo_out);
        let _ = done_tx.send(true);
    });
    let mut d = done_rx.clone();
    let io = ProcessIo {
        stdin: Box::new(bind_in),
        stdout: Box::new(bind_out),
        exited: Box::pin(async move {
            while !*d.borrow() {
                if d.changed().await.is_err() {
                    break;
                }
            }
        }),
        kill: Box::new(|| {}),
    };
    (Fake { read: read_tx, errs, done: done_rx }, io, task)
}

impl Fake {
    async fn consumed(&self, i: usize) {
        let mut r = self.read.subscribe();
        let mut d = self.done.clone();
        loop {
            if *r.borrow() > i || *d.borrow() {
                return;
            }
            tokio::select! { _ = r.changed() => {}, _ = d.changed() => {} }
        }
    }
}

#[derive(Default)]
struct Scripted {
    approvals: Mutex<Vec<Value>>,
    choices: Mutex<Vec<Value>>,
    offers: Mutex<Vec<Value>>,
    released: Notify,
}

impl Scripted {
    async fn take(&self, q: &Mutex<Vec<Value>>) -> Value {
        loop {
            let notified = self.released.notified();
            if let Some(v) = { let mut g = q.lock().unwrap(); if g.is_empty() { None } else { Some(g.remove(0)) } } {
                return v;
            }
            notified.await;
        }
    }
}

struct Script(Arc<Scripted>);

impl Interactor for Script {
    fn approve(&self, _r: ApprovalRequestData) -> BoxFuture<'_, Approval> {
        Box::pin(async move {
            let a = self.0.take(&self.0.approvals).await;
            Approval { decision: a["decision"].as_str().unwrap().into(), message: a.get("message").and_then(|m| m.as_str()).unwrap_or("").into() }
        })
    }
    fn choose(&self, _r: ChoiceRequestData) -> BoxFuture<'_, Option<i64>> {
        Box::pin(async move {
            let c = self.0.take(&self.0.choices).await;
            if c.get("cancel").and_then(|v| v.as_bool()).unwrap_or(false) { None } else { c.get("index").and_then(|v| v.as_i64()).or(Some(0)) }
        })
    }
    fn offer(&self, _r: OfferRequestData) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.0.take(&self.0.offers).await.get("yes").and_then(|v| v.as_bool()).unwrap_or(false) })
    }
}

struct Run {
    id: String,
    session: String,
    handle: tokio::task::JoinHandle<(Vec<String>, std::result::Result<OutcomeData, HaloError>)>,
    started: Arc<Notify>,
    seen_start: Arc<Mutex<bool>>,
    cancel: tokio::sync::mpsc::UnboundedSender<()>,
}

fn strs(v: &Value, k: &str) -> Vec<String> {
    v.get(k).and_then(|x| x.as_array()).map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect()).unwrap_or_default()
}

fn expected_start(rest: &[Value]) -> String {
    for l in rest {
        if l.get("halo_exit").is_some() {
            return "halo_exited".into();
        }
        if let Some(o) = l.get("out") {
            if o["type"] == "initialized" {
                return String::new();
            }
            if o["type"] == "error" {
                return o["data"]["code"].as_str().unwrap().into();
            }
        }
    }
    "halo_exited".into()
}

async fn replay(name: &str, lines: Vec<Value>) {
    let (fake, io, task) = fake(lines.clone());
    let io = Mutex::new(Some(io));
    let launch: Launch = Arc::new(move |_b, _a| Ok(io.lock().unwrap().take().expect("one launch")));
    let script = Arc::new(Scripted::default());
    let mut client: Option<Halo> = None;
    let mut closed = false;
    let mut sessions: HashMap<String, Session> = HashMap::new();
    let mut runs: Vec<Run> = vec![];
    let mut results: HashMap<String, (Vec<String>, std::result::Result<OutcomeData, HaloError>)> = HashMap::new();
    for i in 0..lines.len() {
        let Some(e) = lines[i].get("in") else { continue };
        let d = e.get("data").cloned().unwrap_or(Value::Null);
        let sid = e.get("session_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        match e["type"].as_str().unwrap() {
            "initialize" => {
                let want = expected_start(&lines[i + 1..]);
                let opts = ClientOptions {
                    client_name: d["client"]["name"].as_str().map(String::from),
                    client_version: d["client"].get("version").and_then(|v| v.as_str()).map(String::from),
                    require: strs(&d, "require"),
                    usage: d.get("usage").and_then(|v| v.as_str()).map(String::from),
                    interactor: if strs(&d, "capabilities").is_empty() { None } else { Some(Arc::new(Script(script.clone())) as Arc<dyn Interactor>) },
                    launch: Some(launch.clone()),
                    ..Default::default()
                };
                match Halo::connect(opts).await {
                    Ok(c) => {
                        assert_eq!(want, "", "{name}: connect succeeded");
                        client = Some(c);
                    }
                    Err(err) => {
                        assert_eq!(err.code, want, "{name}: connect");
                        let _ = task.await;
                        assert!(fake.errs.lock().unwrap().is_empty(), "{name}: {:?}", fake.errs.lock().unwrap());
                        return;
                    }
                }
            }
            "create_session" => {
                let want = lines[i + 1..].iter().find_map(|l| l.get("out").filter(|o| o.get("id") == e.get("id"))).unwrap().clone();
                let opts = SessionOptions {
                    cwd: d.get("cwd").and_then(|v| v.as_str()).map(PathBuf::from),
                    permission_mode: d.get("permission_mode").and_then(|v| v.as_str()).map(String::from),
                    allowed_operations: strs(&d, "allowed_operations"),
                    disallowed_operations: strs(&d, "disallowed_operations"),
                    allow_catastrophic: d.get("allow_catastrophic").and_then(|v| v.as_bool()).unwrap_or(false),
                    allow_unconfined: d.get("allow_unconfined").and_then(|v| v.as_bool()).unwrap_or(false),
                    setting_sources: strs(&d, "setting_sources"),
                    model: d.get("model").and_then(|v| v.as_str()).map(String::from),
                    idle_timeout: d.get("idle_timeout_ms").and_then(|v| v.as_u64()).map(Duration::from_millis),
                    interactor: None,
                };
                match client.as_ref().unwrap().session(opts).await {
                    Ok(s) => {
                        assert_eq!(want["type"], "session_created", "{name}");
                        assert_eq!(s.id(), want["data"]["session_id"].as_str().unwrap(), "{name}");
                        assert_eq!(s.model(), want["data"].get("model").and_then(|v| v.as_str()).unwrap_or(""), "{name}");
                        sessions.insert(s.id(), s);
                    }
                    Err(err) => assert_eq!(err.code, want["data"]["code"].as_str().unwrap(), "{name}"),
                }
            }
            "user_turn" => {
                if let Some(pos) = runs.iter().rposition(|r| r.session == sid) {
                    let r = runs.remove(pos);
                    results.insert(r.id.clone(), r.handle.await.unwrap());
                }
                let s = sessions.get(&sid).unwrap().clone();
                let mut ex = s.begin(d["text"].as_str().unwrap()).unwrap();
                assert_eq!(ex.turn_id(), e["turn_id"].as_str().unwrap(), "{name}");
                let (started, seen) = (Arc::new(Notify::new()), Arc::new(Mutex::new(false)));
                let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                let (st2, seen2) = (started.clone(), seen.clone());
                let handle = tokio::spawn(async move {
                    let mut events = vec![];
                    loop {
                        tokio::select! {
                            ev = ex.next_event() => match ev {
                                Some(ev) => {
                                    if ev.kind == "turn_started" { *seen2.lock().unwrap() = true; st2.notify_waiters(); }
                                    events.push(ev.kind);
                                }
                                None => break,
                            },
                            Some(()) = cancel_rx.recv() => ex.cancel(),
                        }
                    }
                    (events, ex.outcome().await)
                });
                runs.push(Run { id: e["turn_id"].as_str().unwrap().into(), session: sid, handle, started, seen_start: seen, cancel: cancel_tx });
            }
            "approval_response" => { script.approvals.lock().unwrap().push(d); script.released.notify_waiters(); }
            "choice_response" => { script.choices.lock().unwrap().push(d); script.released.notify_waiters(); }
            "offer_response" => { script.offers.lock().unwrap().push(d); script.released.notify_waiters(); }
            "cancel" => {
                let r = runs.iter().find(|r| r.id == e["turn_id"].as_str().unwrap()).unwrap();
                while !*r.seen_start.lock().unwrap() {
                    let n = r.started.notified();
                    if *r.seen_start.lock().unwrap() { break; }
                    let _ = tokio::time::timeout(Duration::from_millis(5), n).await;
                }
                let _ = r.cancel.send(());
            }
            "reset" => {
                if let Some(pos) = runs.iter().rposition(|r| r.session == sid) {
                    let r = runs.remove(pos);
                    results.insert(r.id.clone(), r.handle.await.unwrap());
                }
                sessions.get(&sid).unwrap().reset().await.unwrap();
            }
            "close_session" => sessions.get(&sid).unwrap().close().await.unwrap(),
            "shutdown" => {
                client.as_ref().unwrap().close().await;
                closed = true;
            }
            _ => {}
        }
        fake.consumed(i).await;
    }
    for r in runs.drain(..) {
        results.insert(r.id.clone(), r.handle.await.unwrap());
    }
    for (id, (events, got)) in &results {
        check_turn(name, id, &lines, events, got);
    }
    if let (Some(c), false) = (&client, closed) {
        c.close().await;
    }
    let _ = task.await;
    assert!(fake.errs.lock().unwrap().is_empty(), "{name}: {:?}", fake.errs.lock().unwrap());
}

fn check_turn(name: &str, id: &str, lines: &[Value], got_events: &[String], got: &std::result::Result<OutcomeData, HaloError>) {
    let (mut events, mut terminal, mut exited) = (vec![], None, false);
    for l in lines {
        if l.get("halo_exit").is_some() {
            exited = true;
        }
        let Some(o) = l.get("out") else { continue };
        if o.get("turn_id").and_then(|v| v.as_str()) != Some(id) {
            continue;
        }
        match o["type"].as_str().unwrap() {
            "approval_request" | "choice_request" | "offer_request" => {}
            "outcome" | "error" => terminal = Some(o.clone()),
            t => events.push(t.to_string()),
        }
    }
    assert_eq!(got_events, events.as_slice(), "{name} {id}: events");
    match terminal {
        None => {
            assert!(exited, "{name} {id}: no terminal envelope");
            assert_eq!(got.as_ref().err().map(|e| e.code.as_str()), Some("halo_exited"), "{name} {id}");
        }
        Some(t) if t["type"] == "outcome" => {
            let o = got.as_ref().unwrap_or_else(|e| panic!("{name} {id}: {e}"));
            let w = &t["data"];
            assert_eq!(o.kind, w["kind"].as_str().unwrap(), "{name} {id}");
            assert_eq!(o.code, w.get("code").and_then(|v| v.as_str()).unwrap_or(""), "{name} {id}");
            assert_eq!(o.completed, w["completed"].as_bool().unwrap(), "{name} {id}");
        }
        Some(t) => {
            let e = got.as_ref().err().unwrap_or_else(|| panic!("{name} {id}: an outcome, want an error"));
            let w = &t["data"];
            assert_eq!(e.code, w["code"].as_str().unwrap(), "{name} {id}");
            assert_eq!(e.retryable, w["retryable"].as_bool().unwrap(), "{name} {id}");
            assert_eq!(e.retry_after_ms, w.get("retry_after_ms").and_then(|v| v.as_i64()).unwrap_or(0), "{name} {id}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("conformance");
    if !dir.exists() {
        eprintln!("transcripts not present: run from the halo-client repository");
        return;
    }
    let mut files: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|x| x == "jsonl")).collect();
    files.sort();
    let mut n = 0;
    for f in files {
        let text = std::fs::read_to_string(&f).unwrap();
        let mut it = text.trim().lines();
        let meta: Value = serde_json::from_str(it.next().unwrap()).unwrap();
        let meta = &meta["meta"];
        if !strs(meta, "applies").iter().any(|a| a == "binding") {
            continue;
        }
        n += 1;
        let name = meta["name"].as_str().unwrap().to_string();
        let lines: Vec<Value> = it.map(|l| serde_json::from_str(l).unwrap()).collect();
        tokio::time::timeout(Duration::from_secs(20), replay(&name, lines)).await.unwrap_or_else(|_| panic!("{name}: timed out"));
    }
    assert!(n >= 20, "only {n} binding transcripts");
}
