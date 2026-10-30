package halo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// The conformance transcripts (../conformance/*.jsonl) are scripted sessions of envelopes in (host → halo) and out
// (halo → host). This test plays halo's side of every transcript that applies to bindings through an injected
// Process, drives the binding from the host's side of it, and requires that the binding sends exactly the
// transcript's envelopes and reports the same events and outcome.

type meta struct {
	Name    string   `json:"name"`
	Applies []string `json:"applies"`
}

type line struct {
	In       json.RawMessage `json:"in,omitempty"`
	Out      json.RawMessage `json:"out,omitempty"`
	InRaw    *string         `json:"in_raw,omitempty"`
	OutRaw   *string         `json:"out_raw,omitempty"`
	EOF      bool            `json:"eof,omitempty"`
	HaloExit bool            `json:"halo_exit,omitempty"`
}

func readTranscript(t *testing.T, path string) (meta, []line) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ls := bytes.Split(bytes.TrimSpace(b), []byte("\n"))
	var head struct{ Meta meta }
	if err := json.Unmarshal(ls[0], &head); err != nil || head.Meta.Name == "" {
		t.Fatalf("%s: bad meta line", path)
	}
	var out []line
	for _, l := range ls[1:] {
		var x line
		if err := json.Unmarshal(l, &x); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		out = append(out, x)
	}
	return head.Meta, out
}

func canonical(b []byte) string {
	var v any
	if json.Unmarshal(b, &v) != nil {
		return "MALFORMED " + string(b)
	}
	out, _ := json.Marshal(v)
	return string(out)
}

func envelopeOf(raw json.RawMessage) Envelope {
	var e Envelope
	_ = json.Unmarshal(raw, &e)
	return e
}

// fakeHalo is halo's side of a transcript, as a Process.
type fakeHalo struct {
	inR    *io.PipeReader
	inW    *io.PipeWriter
	outR   *io.PipeReader
	outW   *io.PipeWriter
	exited chan struct{}
	once   sync.Once
	errs   []string
	mu     sync.Mutex
}

func newFakeHalo() *fakeHalo {
	f := &fakeHalo{exited: make(chan struct{})}
	f.inR, f.inW = io.Pipe()
	f.outR, f.outW = io.Pipe()
	return f
}

func (f *fakeHalo) Stdin() io.WriteCloser { return f.inW }
func (f *fakeHalo) Stdout() io.Reader     { return f.outR }
func (f *fakeHalo) Wait() error           { <-f.exited; return nil }
func (f *fakeHalo) Kill() error           { f.exit(); return nil }
func (f *fakeHalo) exit() {
	f.once.Do(func() {
		f.outW.Close()
		f.inR.Close()
		close(f.exited)
	})
}

func (f *fakeHalo) fail(format string, a ...any) {
	f.mu.Lock()
	f.errs = append(f.errs, fmt.Sprintf(format, a...))
	f.mu.Unlock()
}

// play runs halo's side: write what halo sends, read and check what the binding sends.
func (f *fakeHalo) play(lines []line) {
	defer f.exit()
	sc := bufio.NewScanner(f.inR)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for i, l := range lines {
		switch {
		case l.Out != nil:
			f.outW.Write(append([]byte(canonical(l.Out)), '\n'))
		case l.OutRaw != nil:
			f.outW.Write([]byte(*l.OutRaw + "\n"))
		case l.In != nil:
			if !sc.Scan() {
				f.fail("line %d: the binding sent nothing, want %s", i+2, canonical(l.In))
				return
			}
			if got, want := canonical(sc.Bytes()), canonical(l.In); got != want {
				f.fail("line %d: the binding sent\n  %s\nwant\n  %s", i+2, got, want)
				return
			}
		case l.EOF:
			if sc.Scan() {
				f.fail("line %d: want EOF from the binding, got %s", i+2, sc.Text())
			}
			return
		case l.HaloExit:
			return
		}
	}
}

// scripted answers with the transcript's replies, in order.
type scripted struct {
	mu        sync.Mutex
	approvals []ApprovalResponseData
	choices   []ChoiceResponseData
	offers    []OfferResponseData
}

func (s *scripted) Approve(context.Context, ApprovalRequest) Approval {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.approvals) == 0 {
		return Approval{Decision: DecisionDeny}
	}
	a := s.approvals[0]
	s.approvals = s.approvals[1:]
	return Approval{Decision: a.Decision, Message: a.Message}
}

func (s *scripted) Choose(context.Context, ChoiceRequest) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.choices) == 0 {
		return 0, false
	}
	c := s.choices[0]
	s.choices = s.choices[1:]
	return c.Index, !c.Cancel
}

func (s *scripted) Offer(context.Context, OfferRequest) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offers) == 0 {
		return false
	}
	o := s.offers[0]
	s.offers = s.offers[1:]
	return o.Yes
}

// observed records a turn's events as they arrive.
type observed struct {
	mu    sync.Mutex
	types []string
	more  chan struct{}
}

func (o *observed) has(typ string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, t := range o.types {
		if t == typ {
			return true
		}
	}
	return false
}

type turnRun struct {
	id   string
	ex   *Execution
	seen *observed
}

func TestConformance(t *testing.T) {
	files, _ := filepath.Glob("../conformance/*.jsonl")
	n := 0
	for _, path := range files {
		m, lines := readTranscript(t, path)
		if !contains(m.Applies, "binding") {
			continue
		}
		n++
		t.Run(m.Name, func(t *testing.T) { replay(t, lines) })
	}
	if n < 20 {
		t.Fatalf("only %d binding transcripts", n)
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if x == y {
			return true
		}
	}
	return false
}

func replay(t *testing.T, lines []line) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fake := newFakeHalo()
	go fake.play(lines)
	script := &scripted{}
	for _, l := range lines {
		if l.In == nil {
			continue
		}
		e := envelopeOf(l.In)
		switch e.Type {
		case MsgApprovalResponse:
			var d ApprovalResponseData
			json.Unmarshal(e.Data, &d)
			script.approvals = append(script.approvals, d)
		case MsgChoiceResponse:
			var d ChoiceResponseData
			json.Unmarshal(e.Data, &d)
			script.choices = append(script.choices, d)
		case MsgOfferResponse:
			var d OfferResponseData
			json.Unmarshal(e.Data, &d)
			script.offers = append(script.offers, d)
		}
	}
	var s *Session
	var runs []*turnRun
	wait := func() {
		if len(runs) > 0 {
			runs[len(runs)-1].ex.Outcome()
		}
	}
	for i, l := range lines {
		if l.In == nil {
			continue
		}
		e := envelopeOf(l.In)
		switch e.Type {
		case MsgInitialize:
			var d InitializeData
			json.Unmarshal(e.Data, &d)
			opts := Options{Cwd: d.Cwd, ClientName: d.Client.Name, ClientVersion: d.Client.Version, PermissionMode: d.PermissionMode,
				AllowedOperations: d.AllowedOperations, DisallowedOperations: d.DisallowedOperations, AllowCatastrophic: d.AllowCatastrophic,
				AllowUnconfined: d.AllowUnconfined, SettingSources: d.SettingSources, Require: d.Require, Model: d.Model,
				Launch: func(context.Context, string, []string, string, io.Writer) (Process, error) { return fake, nil }}
			if len(d.Capabilities) > 0 {
				opts.Interactor = script
			}
			var err error
			s, err = Start(ctx, opts)
			want := expectedStart(lines[i+1:])
			if want == "" && err != nil || want != "" && (err == nil || err.(*Error).Code != want) {
				t.Fatalf("Start: %v, want %q", err, want)
			}
			if err != nil {
				checkFake(t, fake)
				return
			}
		case MsgUserTurn:
			wait()
			var d UserTurnData
			json.Unmarshal(e.Data, &d)
			ex, err := s.Begin(ctx, d.Text)
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if ex.TurnID() != e.TurnID {
				t.Fatalf("turn id %s, want %s", ex.TurnID(), e.TurnID)
			}
			r := &turnRun{id: e.TurnID, ex: ex, seen: &observed{more: make(chan struct{}, 64)}}
			go func() {
				for ev := range ex.Events() {
					r.seen.mu.Lock()
					r.seen.types = append(r.seen.types, ev.Type)
					r.seen.mu.Unlock()
					r.seen.more <- struct{}{}
				}
			}()
			runs = append(runs, r)
		case MsgCancel:
			r := runs[len(runs)-1]
			prev := envelopeOf(lines[i-1].Out).Type
			for !r.seen.has(prev) {
				select {
				case <-r.seen.more:
				case <-ctx.Done():
					t.Fatalf("never saw %s before cancel", prev)
				}
			}
			r.ex.Cancel()
		case MsgReset:
			wait()
			if err := s.Reset(ctx); err != nil {
				t.Fatalf("Reset: %v", err)
			}
		}
	}
	wait()
	for _, r := range runs {
		checkTurn(t, r, lines)
	}
	if s != nil {
		s.Close()
	}
	checkFake(t, fake)
}

// expectedStart: "" when the handshake succeeds, else the error code Start must return.
func expectedStart(rest []line) string {
	for _, l := range rest {
		switch {
		case l.HaloExit:
			return CodeHaloExited
		case l.Out != nil:
			e := envelopeOf(l.Out)
			if e.Type == MsgInitialized {
				return ""
			}
			if e.Type == MsgError {
				var d ErrorData
				json.Unmarshal(e.Data, &d)
				return d.Code
			}
		}
	}
	return CodeHaloExited
}

// checkTurn compares a turn with the transcript: its events (requests go to the Interactor) and how it ended.
func checkTurn(t *testing.T, r *turnRun, lines []line) {
	t.Helper()
	var events []string
	var terminal *Envelope
	exited := false
	for _, l := range lines {
		if l.HaloExit {
			exited = true
		}
		if l.Out == nil {
			continue
		}
		e := envelopeOf(l.Out)
		if e.TurnID != r.id {
			continue
		}
		switch e.Type {
		case MsgApprovalRequest, MsgChoiceRequest, MsgOfferRequest:
		case MsgOutcome, MsgError:
			terminal = &e
		default:
			events = append(events, e.Type)
		}
	}
	o, err := r.ex.Outcome()
	// the event channel closes after the outcome; let the collector finish
	deadline := time.After(2 * time.Second)
	for {
		r.seen.mu.Lock()
		n := len(r.seen.types)
		r.seen.mu.Unlock()
		if n >= len(events) {
			break
		}
		select {
		case <-r.seen.more:
		case <-deadline:
			t.Fatalf("%s: events %v, want %v", r.id, r.seen.types, events)
		}
	}
	r.seen.mu.Lock()
	got := append([]string(nil), r.seen.types...)
	r.seen.mu.Unlock()
	if !reflect.DeepEqual(got, events) && !(len(got) == 0 && len(events) == 0) {
		t.Errorf("%s: events %v, want %v", r.id, got, events)
	}
	switch {
	case terminal == nil && exited:
		if e, ok := err.(*Error); !ok || e.Code != CodeHaloExited {
			t.Errorf("%s: %v %v, want halo_exited", r.id, o, err)
		}
	case terminal == nil:
		t.Errorf("%s: the transcript has no terminal envelope", r.id)
	case terminal.Type == MsgOutcome:
		var want Outcome
		json.Unmarshal(terminal.Data, &want)
		if err != nil || o == nil || o.Kind != want.Kind || o.Code != want.Code || o.Completed != want.Completed {
			t.Errorf("%s: %+v %v, want kind %s code %q", r.id, o, err, want.Kind, want.Code)
		}
	default:
		var want ErrorData
		json.Unmarshal(terminal.Data, &want)
		e, ok := err.(*Error)
		if !ok || e.Code != want.Code || e.Retryable != want.Retryable || e.RetryAfterMs != want.RetryAfterMs {
			t.Errorf("%s: %v, want error %+v", r.id, err, want)
		}
	}
}

func checkFake(t *testing.T, f *fakeHalo) {
	t.Helper()
	select {
	case <-f.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the transcript did not finish")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.errs {
		t.Error(e)
	}
}
