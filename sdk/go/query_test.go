package halo

import (
	"regexp"
	"testing"
)

func TestResultsAreTypedWithoutASwitch(t *testing.T) {
	rows := ToResult(&Outcome{Kind: "result", Result: &Result{Kind: "rows", Columns: []string{"region", "avg price"}, Rows: []Row{{"region": "west", "avg price": 600.3}}}}, "s1")
	if !rows.OK || rows.Status != "done" || len(rows.Rows) != 1 || !regexp.MustCompile(`REGION\s+AVG PRICE\nwest\s+600\.3`).MatchString(rows.Text) {
		t.Errorf("rows: %+v", rows)
	}
	n := 3
	count := ToResult(&Outcome{Kind: "result", Result: &Result{Kind: "rows", Note: "3", Answer: &Answer{Aggregate: "count", Count: &n}}}, "s1")
	if count.Value != 3 || count.Text != "3" {
		t.Errorf("count: %+v", count)
	}
	if a := ToResult(&Outcome{Kind: "answer", Message: "125.66 lb"}, "s1"); a.Status != "answered" || a.Value != "125.66 lb" {
		t.Errorf("answer: %+v", a)
	}
	if c := ToResult(&Outcome{Kind: "choice", Question: "Which file?", Options: []string{"a.pdf", "b.pdf"}}, "s1"); c.Status != "needs_input" || c.OK || len(c.Options) != 2 {
		t.Errorf("choice: %+v", c)
	}
	if d := ToResult(&Outcome{Kind: "denied", Reason: "needs approval"}, "s1"); d.Status != "denied" || d.Text != "needs approval" {
		t.Errorf("denied: %+v", d)
	}
}
