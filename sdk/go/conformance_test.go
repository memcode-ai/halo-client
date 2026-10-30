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
	read   int           // host lines consumed so far (transcript line index + 1)
	moved  chan struct{} // signalled whenever read advances
}

func newFakeHalo() *fakeHalo {
	f := &fakeHalo{exited: make(chan struct{}), moved: make(chan struct{}, 1)}
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
			f.mu.Lock()
			f.read = i + 1
			f.mu.Unlock()
			select {
			case f.moved <- struct{}{}:
			default:
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

// consumed waits until the fake has read transcript line i from the binding (or ended).
func (f *fakeHalo) consumed(ctx context.Context, i int) {
	for {
		f.mu.Lock()
		done := f.read > i
		f.mu.Unlock()
		if done {
			return
		}
		select {
		case <-f.moved:
		case <-f.exited:
			return
		case <-ctx.Done():
			return
		}
	}
}

// scripted answers with the transcript's replies, each only when the replay reaches its line: in an interleaved
// transcript another session's turn runs while a request waits.
type scripted struct {
	approvals chan ApprovalResponseData
	choices   chan ChoiceResponseData
	offers    chan OfferResponseData
}

func newScripted() *scripted {
	return &scripted{approvals: make(chan ApprovalResponseData, 16), choices: make(chan ChoiceResponseData, 16), offers: make(chan OfferResponseData, 16)}
}

func (s *scripted) Approve(ctx context.Context, _ ApprovalRequest) Approval {
	select {
	case a := <-s.approvals:
		return Approval{Decision: a.Decision, Message: a.Message}
	case <-ctx.Done():
		return Approval{Decision: DecisionDeny}
	}
}

func (s *scripted) Choose(ctx context.Context, _ ChoiceRequest) (int, bool) {
	select {
	case c := <-s.choices:
		return c.Index, !c.Cancel
	case <-ctx.Done():
		return 0, false
	}
}

func (s *scripted) Offer(ctx context.Context, _ OfferRequest) bool {
	select {
	case o := <-s.offers:
		return o.Yes
	case <-ctx.Done():
		return false
	}
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
	id      string
	session string
	ex      *Execution
	seen    *observed
}

func TestConformance(t *testing.T) {
	if _, err := os.Stat("../conformance"); err != nil {
		// a downloaded module carries no transcripts (they sit beside it in the repository, sdk/conformance)
		t.Skip("conformance transcripts not present: run from the halo-client repository")
	}
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

// replay drives the binding through its public API from the host's lines of a transcript, while the fake plays
// halo's lines and checks every envelope the binding sends.
func replay(t *testing.T, lines []line) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fake := newFakeHalo()
	go fake.play(lines)
	script := newScripted()
	var c *Client
	closed := false
	sessions := map[string]*Session{}
	var runs []*turnRun
	last := func(session string) *turnRun {
		for i := len(runs) - 1; i >= 0; i-- {
			if runs[i].session == session {
				return runs[i]
			}
		}
		return nil
	}
	waitSession := func(session string) {
		if r := last(session); r != nil {
			r.ex.Outcome()
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
			opts := ClientOptions{ClientName: d.Client.Name, ClientVersion: d.Client.Version, Require: d.Require, Usage: d.Usage,
				Launch: func(context.Context, string, []string, string, io.Writer) (Process, error) { return fake, nil }}
			if len(d.Capabilities) > 0 {
				opts.Interactor = script
			}
			var err error
			c, err = Connect(ctx, opts)
			want := expectedStart(lines[i+1:])
			if want == "" && err != nil || want != "" && (err == nil || err.(*Error).Code != want) {
				t.Fatalf("Connect: %v, want %q", err, want)
			}
			if err != nil {
				checkFake(t, fake)
				return
			}
		case MsgCreateSession:
			var d CreateSessionData
			json.Unmarshal(e.Data, &d)
			s, err := c.NewSession(ctx, SessionOptions{Cwd: d.Cwd, PermissionMode: d.PermissionMode, AllowedOperations: d.AllowedOperations,
				DisallowedOperations: d.DisallowedOperations, AllowCatastrophic: d.AllowCatastrophic, AllowUnconfined: d.AllowUnconfined,
				SettingSources: d.SettingSources, Model: d.Model, IdleTimeout: time.Duration(d.IdleTimeoutMs) * time.Millisecond})
			want := answerTo(lines[i+1:], e.ID)
			switch {
			case want.Type == MsgSessionCreated && err != nil:
				t.Fatalf("NewSession: %v", err)
			case want.Type == MsgError && (err == nil || err.(*Error).Code != decodeError(want).Code):
				t.Fatalf("NewSession: %v, want %+v", err, decodeError(want))
			}
			if s != nil {
				var created SessionCreatedData
				json.Unmarshal(want.Data, &created)
				if s.ID() != created.SessionID || s.Model() != created.Model {
					t.Fatalf("session %s model %q, want %s %q", s.ID(), s.Model(), created.SessionID, created.Model)
				}
				sessions[s.ID()] = s
			}
		case MsgUserTurn:
			waitSession(e.SessionID)
			var d UserTurnData
			json.Unmarshal(e.Data, &d)
			s := sessions[e.SessionID]
			if s == nil {
				t.Fatalf("a turn for %s, which this replay never opened", e.SessionID)
			}
			ex, err := s.Begin(ctx, d.Text)
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if ex.TurnID() != e.TurnID {
				t.Fatalf("turn id %s, want %s", ex.TurnID(), e.TurnID)
			}
			r := &turnRun{id: e.TurnID, session: e.SessionID, ex: ex, seen: &observed{more: make(chan struct{}, 64)}}
			go func() {
				for ev := range ex.Events() {
					r.seen.mu.Lock()
					r.seen.types = append(r.seen.types, ev.Type)
					r.seen.mu.Unlock()
					r.seen.more <- struct{}{}
				}
			}()
			runs = append(runs, r)
		case MsgApprovalResponse:
			var d ApprovalResponseData
			json.Unmarshal(e.Data, &d)
			script.approvals <- d
		case MsgChoiceResponse:
			var d ChoiceResponseData
			json.Unmarshal(e.Data, &d)
			script.choices <- d
		case MsgOfferResponse:
			var d OfferResponseData
			json.Unmarshal(e.Data, &d)
			script.offers <- d
		case MsgCancel:
			var r *turnRun
			for _, x := range runs {
				if x.id == e.TurnID {
					r = x
				}
			}
			for !r.seen.has(MsgTurnStarted) {
				select {
				case <-r.seen.more:
				case <-ctx.Done():
					t.Fatalf("never saw turn_started before cancel")
				}
			}
			r.ex.Cancel()
		case MsgReset:
			waitSession(e.SessionID)
			if err := sessions[e.SessionID].Reset(ctx); err != nil {
				t.Fatalf("Reset: %v", err)
			}
		case MsgCloseSession:
			if err := sessions[e.SessionID].Close(ctx); err != nil {
				t.Fatalf("Close session: %v", err)
			}
		case MsgShutdown:
			c.Close()
			closed = true
		}
		// the binding's envelopes go out in transcript order: wait until this one has been sent and checked
		fake.consumed(ctx, i)
	}
	for _, r := range runs {
		r.ex.Outcome()
	}
	for _, r := range runs {
		checkTurn(t, r, lines)
	}
	if c != nil && !closed {
		c.Close()
	}
	checkFake(t, fake)
}

// answerTo is halo's envelope answering the request with this id.
func answerTo(rest []line, id string) Envelope {
	for _, l := range rest {
		if l.Out != nil {
			if e := envelopeOf(l.Out); e.ID == id {
				return e
			}
		}
	}
	return Envelope{}
}

// expectedStart: "" when the handshake succeeds, else the error code Connect must return.
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
