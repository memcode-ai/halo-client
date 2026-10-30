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
	"strings"
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

// a minimal halo that completes the handshake; every later line the binding sends arrives on the channel, and
// halo exits when the binding closes its stdin
func handshake(t *testing.T, version string) (*fakeHalo, <-chan Envelope, func(Envelope, any)) {
	f := newFakeHalo()
	write := func(e Envelope, data any) {
		e.Version = "1"
		if data != nil {
			e.Data, _ = json.Marshal(data)
		}
		b, _ := json.Marshal(e)
		f.outW.Write(append(b, '\n'))
	}
	got := make(chan Envelope, 16)
	go func() {
		defer f.exit()
		sc := bufio.NewScanner(f.inR)
		first := true
		for sc.Scan() {
			var e Envelope
			json.Unmarshal(sc.Bytes(), &e)
			if first {
				first = false
				write(Envelope{Type: MsgInitialized}, InitializedData{SessionID: "s", Protocol: "1", HaloVersion: version, Service: "halo/1", SignedIn: true})
				continue
			}
			got <- e
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

func TestMinHaloVersion(t *testing.T) {
	f, _, _ := handshake(t, "0.1.4")
	_, err := Start(context.Background(), Options{Launch: launcher(f), MinHaloVersion: "0.1.5"})
	if e, ok := err.(*Error); !ok || e.Code != CodeVersionMismatch || !strings.Contains(e.Message, "halo upgrade") {
		t.Errorf("an old halo: %v", err)
	}
}

// One turn at a time: a second Run while one is in flight is refused locally, and nothing is sent.
func TestConcurrentRunRefused(t *testing.T) {
	f, got, write := handshake(t, "test")
	s, err := Start(context.Background(), Options{Launch: launcher(f)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
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
	write(Envelope{Type: MsgOutcome, TurnID: ex.TurnID()}, OutcomeData{Kind: KindAnswer, Message: "ok", Completed: true})
	if o, err := ex.Outcome(); err != nil || o.Kind != KindAnswer {
		t.Fatalf("%v %v", o, err)
	}
	ex2, err := s.Begin(context.Background(), "next")
	if err != nil {
		t.Fatalf("after the outcome: %v", err)
	}
	e := receive(t, got)
	write(Envelope{Type: MsgOutcome, TurnID: e.TurnID}, OutcomeData{Kind: KindAnswer, Completed: true})
	if o, err := ex2.Outcome(); err != nil || o.Kind != KindAnswer {
		t.Errorf("after the outcome: %v %v", o, err)
	}
}

// Cancelling the context of Run cancels the turn in halo.
func TestContextCancelsTurn(t *testing.T) {
	f, got, write := handshake(t, "test")
	s, err := Start(context.Background(), Options{Launch: launcher(f)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ex, _ := s.Begin(ctx, "hang")
	receive(t, got)
	cancel()
	if e := receive(t, got); e.Type != MsgCancel || e.TurnID != ex.TurnID() {
		t.Fatalf("sent %+v", e)
	}
	write(Envelope{Type: MsgOutcome, TurnID: ex.TurnID()}, OutcomeData{Kind: KindCancelled})
	if o, _ := ex.Outcome(); o == nil || o.Kind != KindCancelled {
		t.Errorf("%+v", o)
	}
}

// With no Interactor the binding answers any request with the safe default: deny, cancel, no.
func TestNoInteractorDefaults(t *testing.T) {
	f, got, write := handshake(t, "test")
	s, err := Start(context.Background(), Options{Launch: launcher(f)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ex, _ := s.Begin(context.Background(), "x")
	receive(t, got)
	want := map[string]string{
		MsgApprovalResponse: `{"decision":"deny","message":"denied by the SDK default (no Interactor)"}`,
		MsgChoiceResponse:   `{"cancel":true,"index":0}`,
		MsgOfferResponse:    `{"yes":false}`,
	}
	for req, reply := range map[string]string{MsgApprovalRequest: MsgApprovalResponse, MsgChoiceRequest: MsgChoiceResponse, MsgOfferRequest: MsgOfferResponse} {
		write(Envelope{Type: req, ID: "r1", TurnID: ex.TurnID()}, map[string]any{})
		e := receive(t, got)
		if e.Type != reply || e.ID != "r1" || canonical(e.Data) != canonical([]byte(want[reply])) {
			t.Errorf("%s: replied %+v %s", req, e, e.Data)
		}
	}
	write(Envelope{Type: MsgOutcome, TurnID: ex.TurnID()}, OutcomeData{Kind: KindDenied, Code: CodeApprovalRequired, Completed: true})
	ex.Outcome()
}

// A pinned model: an old Halo that cannot pin fails the handshake, a different pin fails it, and a pin not yet
// resolved (the service unreachable at the handshake) is left to Halo, which enforces it at the first turn.
func TestPinnedModelHandshake(t *testing.T) {
	opts := Options{Model: "halo-orbit-preview"}
	caps := []string{CapModelPinning}
	for name, c := range map[string]struct {
		info InitializedData
		code string
	}{
		"old halo":   {InitializedData{Protocol: ProtocolVersion, SignedIn: true}, CodeVersionMismatch},
		"other pin":  {InitializedData{Protocol: ProtocolVersion, SignedIn: true, Capabilities: caps, Model: &ModelInfo{ID: "halo-dev"}}, CodeModelNotAvailable},
		"pinned":     {InitializedData{Protocol: ProtocolVersion, SignedIn: true, Capabilities: caps, Model: &ModelInfo{ID: "halo-orbit-preview"}}, ""},
		"unresolved": {InitializedData{Protocol: ProtocolVersion, SignedIn: true, Capabilities: caps}, ""},
	} {
		err := checkVersion(c.info, opts)
		got := ""
		if e, ok := err.(*Error); ok {
			got = e.Code
		}
		if got != c.code {
			t.Errorf("%s: %v (want %q)", name, err, c.code)
		}
	}
	if err := checkVersion(InitializedData{Protocol: ProtocolVersion}, Options{}); err != nil {
		t.Errorf("no model asked for: %v", err)
	}
}
