use halo_sdk::{to_result, OutcomeData};

#[test]
fn results_are_typed_without_a_switch() {
    let o: OutcomeData = serde_json::from_value(serde_json::json!({"kind": "result", "completed": true,
        "result": {"kind": "rows", "columns": ["region", "avg price"], "rows": [{"region": "west", "avg price": 600.3}]}})).unwrap();
    let r = to_result(o, "s1".into());
    assert!(r.ok);
    assert_eq!(r.rows.len(), 1);
    assert!(r.text.starts_with("REGION  AVG PRICE\nwest"), "{}", r.text);

    let o: OutcomeData = serde_json::from_value(serde_json::json!({"kind": "result", "completed": true,
        "result": {"kind": "rows", "note": "3", "answer": {"aggregate": "count", "count": 3}}})).unwrap();
    let r = to_result(o, "s1".into());
    assert_eq!((r.value, r.text.as_str()), (Some(serde_json::json!(3)), "3"));

    let o: OutcomeData = serde_json::from_value(serde_json::json!({"kind": "choice", "completed": false, "question": "Which file?", "options": ["a.pdf", "b.pdf"]})).unwrap();
    let r = to_result(o, "s1".into());
    assert_eq!((r.status.as_str(), r.ok, r.options.len()), ("needs_input", false, 2));

    let o: OutcomeData = serde_json::from_value(serde_json::json!({"kind": "denied", "completed": false, "reason": "needs approval"})).unwrap();
    let r = to_result(o, "s1".into());
    assert_eq!((r.status.as_str(), r.text.as_str()), ("denied", "needs approval"));
}
