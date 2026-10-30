package halo

import (
	"bufio"
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The binding is standard library only: it imports nothing of Halo's private module (or anything else), so it
// can ship on its own and holds no Halo logic.
func TestImportsOnlyTheStandardLibrary(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	for _, f := range files {
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(strings.SplitN(p, "/", 2)[0], ".") {
				t.Errorf("%s imports %s: the binding may import only the standard library", f, p)
			}
		}
	}
	b, _ := os.ReadFile("go.mod")
	if strings.Contains(string(b), "require") {
		t.Error("go.mod requires a module")
	}
}

func TestFind(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "halo")
	os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755)
	if p, err := Find(bin); err != nil || p != bin {
		t.Errorf("BinPath: %s %v", p, err)
	}
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HALO_BIN", bin)
	if p, err := Find(""); err != nil || p != bin {
		t.Errorf("HALO_BIN: %s %v", p, err)
	}
	t.Setenv("HALO_BIN", "")
	t.Setenv("HALO_INSTALL_DIR", dir)
	if p, err := Find(""); err != nil || p != bin {
		t.Errorf("HALO_INSTALL_DIR: %s %v", p, err)
	}
	t.Setenv("HALO_INSTALL_DIR", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	if _, err := os.Stat("/usr/local/bin/halo"); err == nil {
		t.Skip("halo is installed in /usr/local/bin")
	}
	if _, err := Find(""); err != ErrNotInstalled || !strings.Contains(err.Error(), InstallLine) {
		t.Errorf("not installed: %v", err)
	}
	if _, err := Find(filepath.Join(dir, "missing")); err != ErrNotInstalled {
		t.Errorf("a missing BinPath: %v", err)
	}
}

func TestOlder(t *testing.T) {
	for _, c := range []struct {
		have, want string
		older      bool
	}{{"0.1.9", "0.1.10", true}, {"v0.2.0", "0.1.10", false}, {"1.0.0", "1.0.0", false}, {"0.1.5-rc1", "0.1.5", false}} {
		if older(c.have, c.want) != c.older {
			t.Errorf("%s < %s should be %v", c.have, c.want, c.older)
		}
	}
}

// a minimal halo: it completes the handshake (with caps) and opens every session it is asked for; every other
// line the binding sends arrives on the channel, and halo exits when the binding closes its stdin
func handshake(t *testing.T, version string, caps ...string) (*fakeHalo, <-chan Envelope, func(Envelope, any)) {
	f := newFakeHalo()
	var wmu sync.Mutex
	write := func(e Envelope, data any) {
		e.Version = ProtocolVersion
		if data != nil {
			e.Data, _ = json.Marshal(data)
		}
		b, _ := json.Marshal(e)
		wmu.Lock()
		f.outW.Write(append(b, '\n'))
		wmu.Unlock()
	}
	got := make(chan Envelope, 16)
	go func() {
		defer f.exit()
		sc := bufio.NewScanner(f.inR)
		n := 0
		for sc.Scan() {
			var e Envelope
			json.Unmarshal(sc.Bytes(), &e)
			switch e.Type {
			case MsgInitialize:
				write(Envelope{Type: MsgInitialized}, InitializedData{Protocol: ProtocolVersion, HaloVersion: version, Service: "halo/1", SignedIn: true, Capabilities: caps})
			case MsgCreateSession:
				n++
				id := "S" + strconv.Itoa(n)
				write(Envelope{Type: MsgSessionCreated, ID: e.ID, SessionID: id}, SessionCreatedData{SessionID: id})
			case MsgShutdown:
				write(Envelope{Type: MsgShutdownComplete, ID: e.ID}, struct{}{})
			default:
				got <- e
			}
		}
	}()
	return f, got, write
}

func launcher(f *fakeHalo) func(context.Context, string, []string, string, io.Writer) (Process, error) {
	return func(context.Context, string, []string, string, io.Writer) (Process, error) { return f, nil }
}

func receive(t *testing.T, got <-chan Envelope) Envelope {
	t.Helper()
	select {
	case e := <-got:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("the binding sent nothing")
		return Envelope{}
	}
}

// open connects and opens one session.
func open(t *testing.T, f *fakeHalo, opts ClientOptions) (*Client, *Session) {
	t.Helper()
	opts.Launch = launcher(f)
	c, err := Connect(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	s, err := c.NewSession(context.Background(), SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func TestMinHaloVersion(t *testing.T) {
	f, _, _ := handshake(t, "0.1.4")
	_, err := Connect(context.Background(), ClientOptions{Launch: launcher(f), MinHaloVersion: "0.1.5"})
	if e, ok := err.(*Error); !ok || e.Code != CodeVersionMismatch || !strings.Contains(e.Message, "halo upgrade") {
		t.Errorf("an old halo: %v", err)
	}
}

// One turn at a time per session: a second Run while one is in flight is refused locally, and nothing is sent.
// Another session of the same client is not affected.
func TestConcurrentRunRefused(t *testing.T) {
	f, got, write := handshake(t, "test")
	c, s := open(t, f, ClientOptions{})
	ex, err := s.Begin(context.Background(), "hang")
	if err != nil {
		t.Fatal(err)
	}
	receive(t, got) // the first user_turn
	if _, err := s.Run(context.Background(), "again"); err != ErrBusy {
		t.Errorf("second run: %v", err)
	}
	select {
	case e := <-got:
		t.Errorf("a refused run sent %+v", e)
	default:
	}
	other, err := c.NewSession(context.Background(), SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ex2, err := other.Begin(context.Background(), "in another session")
	if err != nil {
		t.Fatalf("another session: %v", err)
	}
	e := receive(t, got)
	if e.SessionID != other.ID() {
		t.Fatalf("sent for %s, want %s", e.SessionID, other.ID())
	}
	write(Envelope{Type: MsgOutcome, SessionID: other.ID(), TurnID: e.TurnID}, OutcomeData{Kind: KindAnswer, Completed: true})
	if o, err := ex2.Outcome(); err != nil || o.Kind != KindAnswer {
		t.Fatalf("other session: %v %v", o, err)
	}
	write(Envelope{Type: MsgOutcome, SessionID: s.ID(), TurnID: ex.TurnID()}, OutcomeData{Kind: KindAnswer, Message: "ok", Completed: true, Model: "halo-orbit-preview"})
	if o, err := ex.Outcome(); err != nil || o.Kind != KindAnswer {
		t.Fatalf("%v %v", o, err)
	}
	if s.Model() != "halo-orbit-preview" {
		t.Errorf("the first turn did not pin the model: %q", s.Model())
	}
}

// Cancelling the context of Run cancels the turn in halo, in its session only.
func TestContextCancelsTurn(t *testing.T) {
	f, got, write := handshake(t, "test")
	_, s := open(t, f, ClientOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	ex, _ := s.Begin(ctx, "hang")
	receive(t, got)
	cancel()
	if e := receive(t, got); e.Type != MsgCancel || e.TurnID != ex.TurnID() || e.SessionID != s.ID() {
		t.Fatalf("sent %+v", e)
	}
	write(Envelope{Type: MsgOutcome, SessionID: s.ID(), TurnID: ex.TurnID()}, OutcomeData{Kind: KindCancelled})
	if o, _ := ex.Outcome(); o == nil || o.Kind != KindCancelled {
		t.Errorf("%+v", o)
	}
}

// With no Interactor the binding answers any request with the safe default: deny, cancel, no.
func TestNoInteractorDefaults(t *testing.T) {
	f, got, write := handshake(t, "test")
	_, s := open(t, f, ClientOptions{})
	ex, _ := s.Begin(context.Background(), "x")
	receive(t, got)
	want := map[string]string{
		MsgApprovalResponse: `{"decision":"deny","message":"denied by the SDK default (no Interactor)"}`,
		MsgChoiceResponse:   `{"cancel":true,"index":0}`,
		MsgOfferResponse:    `{"yes":false}`,
	}
	for req, reply := range map[string]string{MsgApprovalRequest: MsgApprovalResponse, MsgChoiceRequest: MsgChoiceResponse, MsgOfferRequest: MsgOfferResponse} {
		write(Envelope{Type: req, ID: "r1", SessionID: s.ID(), TurnID: ex.TurnID()}, map[string]any{})
		e := receive(t, got)
		if e.Type != reply || e.ID != "r1" || e.SessionID != s.ID() || canonical(e.Data) != canonical([]byte(want[reply])) {
			t.Errorf("%s: replied %+v %s", req, e, e.Data)
		}
	}
	write(Envelope{Type: MsgOutcome, SessionID: s.ID(), TurnID: ex.TurnID()}, OutcomeData{Kind: KindDenied, Code: CodeApprovalRequired, Completed: true})
	ex.Outcome()
}

// A pinned model needs a Halo that pins (version_mismatch before anything is sent otherwise); the pin itself is
// checked by the service on the first turn.
func TestPinnedModelNeedsAPinningHalo(t *testing.T) {
	f, _, _ := handshake(t, "test")
	c, err := Connect(context.Background(), ClientOptions{Launch: launcher(f)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.NewSession(context.Background(), SessionOptions{Model: "halo-orbit-preview"}); err == nil || err.(*Error).Code != CodeVersionMismatch {
		t.Errorf("an old halo: %v", err)
	}
	f2, _, _ := handshake(t, "test", CapModelPinning, CapSessions)
	c2, err := Connect(context.Background(), ClientOptions{Launch: launcher(f2)})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if s, err := c2.NewSession(context.Background(), SessionOptions{Model: "halo-orbit-preview"}); err != nil || s == nil {
		t.Errorf("a pinning halo: %v", err)
	}
}

// Close shuts halo down (shutdown, then EOF) and its sessions refuse further turns.
func TestCloseShutsDown(t *testing.T) {
	f, _, _ := handshake(t, "test")
	c, s := open(t, f, ClientOptions{})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("halo did not exit after Close")
	}
	if _, err := s.Begin(context.Background(), "after close"); err == nil {
		t.Error("a turn after Close was accepted")
	}
}
