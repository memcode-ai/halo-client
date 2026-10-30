use std::sync::{Arc, Mutex};

use halo_sdk::*;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// a minimal halo: it completes the handshake and opens sessions; other lines land in `sent`, and `say` writes to the
// binding
fn mini() -> (Launch, Arc<Mutex<Vec<Value>>>, tokio::sync::mpsc::UnboundedSender<Value>) {
    let (bind_in, halo_in) = tokio::io::duplex(1 << 16);
    let (mut halo_out, bind_out) = tokio::io::duplex(1 << 16);
    let sent = Arc::new(Mutex::new(vec![]));
    let (say_tx, mut say_rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
    let sent2 = sent.clone();
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let mut lines = BufReader::new(halo_in).lines();
        let mut n = 0;
        loop {
            tokio::select! {
                l = lines.next_line() => {
                    let Ok(Some(l)) = l else { break };
                    let e: Value = serde_json::from_str(&l).unwrap();
                    let reply = match e["type"].as_str().unwrap() {
                        "initialize" => Some(json!({"version":"2","type":"initialized","data":{"protocol":"2","halo_version":"test","service":"halo/1","signed_in":true,"capabilities":["sessions"]}})),
                        "create_session" => { n += 1; Some(json!({"version":"2","type":"session_created","id":e["id"],"session_id":format!("S{n}"),"data":{"session_id":format!("S{n}")}})) }
                        "shutdown" => Some(json!({"version":"2","type":"shutdown_complete","id":e["id"],"data":{}})),
                        _ => { sent2.lock().unwrap().push(e); None }
                    };
                    if let Some(r) = reply { halo_out.write_all(format!("{r}\n").as_bytes()).await.unwrap(); }
                }
                Some(v) = say_rx.recv() => { halo_out.write_all(format!("{v}\n").as_bytes()).await.unwrap(); }
            }
        }
        done_tx.send_replace(true);
    });
    let io = Mutex::new(Some((bind_in, bind_out, done_rx)));
    let launch: Launch = Arc::new(move |_b, _a| {
        let (i, o, mut d) = io.lock().unwrap().take().unwrap();
        Ok(ProcessIo {
            stdin: Box::new(i),
            stdout: Box::new(o),
            exited: Box::pin(async move { while !*d.borrow() { if d.changed().await.is_err() { break; } } }),
            kill: Box::new(|| {}),
        })
    });
    (launch, sent, say_tx)
}

#[tokio::test]
async fn one_turn_per_session_and_model_pin() {
    let (launch, sent, say) = mini();
    let c = Halo::connect(ClientOptions { launch: Some(launch), ..Default::default() }).await.unwrap();
    let s = c.session(SessionOptions::default()).await.unwrap();
    let mut ex = s.begin("hang").unwrap();
    assert_eq!(s.begin("again").err().unwrap().code, "turn_in_progress");
    let other = c.session(SessionOptions::default()).await.unwrap();
    let mut ex2 = other.begin("elsewhere").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let got: Vec<(String, String)> = sent.lock().unwrap().iter().map(|e| (e["type"].as_str().unwrap().into(), e["session_id"].as_str().unwrap().into())).collect();
    assert_eq!(got, vec![("user_turn".into(), "S1".into()), ("user_turn".into(), "S2".into())]);
    say.send(json!({"version":"2","type":"outcome","session_id":"S2","turn_id":ex2.turn_id(),"data":{"kind":"answer","completed":true}})).unwrap();
    assert_eq!(ex2.outcome().await.unwrap().kind, "answer");
    say.send(json!({"version":"2","type":"outcome","session_id":"S1","turn_id":ex.turn_id(),"data":{"kind":"answer","completed":true,"model":"halo-orbit-preview"}})).unwrap();
    ex.outcome().await.unwrap();
    assert_eq!(s.model(), "halo-orbit-preview");
    c.close().await;
}

#[test]
fn a_missing_halo_names_the_install_line() {
    let err = find(Some(std::path::Path::new("/nonexistent/halo"))).unwrap_err();
    assert!(err.message.contains(INSTALL_LINE));
}

// end to end with a real halo (HALO_E2E_BIN, plus HALO_SERVER for a local server); skipped otherwise
#[tokio::test]
async fn a_real_halo() {
    let Ok(bin) = std::env::var("HALO_E2E_BIN") else { return };
    let c = Halo::connect(ClientOptions { bin_path: Some(bin.into()), ..Default::default() }).await.unwrap();
    let dir = std::env::temp_dir().join(format!("halo-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let s = c.session(SessionOptions { cwd: Some(std::fs::canonicalize(&dir).unwrap()), ..Default::default() }).await.unwrap();
    let o = s.run("list the files here").await.unwrap();
    assert!(o.kind == "result" || o.kind == "answer", "{}", o.kind);
    assert!(!s.model().is_empty());
    s.close().await.unwrap();
    c.close().await;
}
