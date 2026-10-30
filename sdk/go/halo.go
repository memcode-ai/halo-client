// Package halo drives the installed Halo binary from Go: it starts `halo --protocol stream-json`, speaks Halo's
// machine protocol (newline-delimited JSON on the process's stdin and stdout) and routes approvals, choices and
// offers to an Interactor. It holds only process lifecycle, protocol I/O and callbacks: interpretation,
// permissions, previews, confinement and execution all stay inside Halo.
//
//	s, err := halo.Start(ctx, halo.Options{Cwd: ".", Interactor: myInteractor})
//	defer s.Close()
//	out, err := s.Run(ctx, "what is using port 5432?")
//
// Defaults are safe: with no Interactor, Halo never asks, so whatever needs approval is denied
// (approval_required), choices end the turn and offers are declined.
package halo

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Public names for the generated protocol types.
type (
	Outcome         = OutcomeData
	ApprovalRequest = ApprovalRequestData
	ChoiceRequest   = ChoiceRequestData
	OfferRequest    = OfferRequestData
)

// Approval is an Interactor's answer to an approval request.
type Approval struct {
	Decision string // DecisionAllow, DecisionAllowSession or DecisionDeny
	Message  string // why, when denied
}

// Approval decisions. A host cannot change the operation it approves: deny, with a message, and ask again in words.
const (
	DecisionAllow        = "allow"
	DecisionAllowSession = "allow_session" // and remember its authorities for the rest of this session only
	DecisionDeny         = "deny"
)

// Interactor answers what Halo asks the host. Each method is called on its own goroutine with the turn's context.
type Interactor interface {
	Approve(ctx context.Context, req ApprovalRequest) Approval
	Choose(ctx context.Context, req ChoiceRequest) (int, bool) // false cancels
	Offer(ctx context.Context, req OfferRequest) bool
}

// Process is a started halo process (an injectable transport for tests).
type Process interface {
	Stdin() io.WriteCloser
	Stdout() io.Reader
	Wait() error
	Kill() error
}

// Options configure a session. The zero value finds the installed halo, runs in the caller's folder, under the
// default permission mode, with no Interactor.
type Options struct {
	BinPath string // "" discovers it (Find)
	Cwd     string // the trusted folder for the whole session; "" is the caller's

	ClientName    string // the host, as Halo records it ("" = "halo-sdk-go")
	ClientVersion string

	// Session permissions: Halo's permission mode (ModeDefault, ModeDontAsk, ModeAcceptEdits, ModePlan,
	// ModeBypassPermissions), operations pre-approved or always denied ("git.*", "files.delete"), the two
	// explicit flags, and the CLI settings to inherit (none unless named: "user", "project").
	PermissionMode       string
	AllowedOperations    []string
	DisallowedOperations []string
	AllowCatastrophic    bool
	AllowUnconfined      bool
	SettingSources       []string

	// Model pins the session to an exact public Halo model ("halo-orbit-preview"); "" takes the service's
	// default, which the handshake resolves to a concrete model (Session.Model). A model the service does not
	// serve fails Start with model_not_available: Halo never substitutes another model, and updating Halo never
	// changes a pinned one. Production integrations should pin.
	Model string

	// Require names capabilities the session needs; an older Halo then fails the handshake with version_mismatch.
	Require []string
	// MinHaloVersion refuses an installed Halo older than this ("0.2.0"); a development build ("dev") passes.
	MinHaloVersion string

	Interactor Interactor
	Stderr     io.Writer // halo's diagnostics; nil discards them

	// Launch starts the process (tests); nil runs the binary.
	Launch func(ctx context.Context, bin string, args []string, dir string, stderr io.Writer) (Process, error)
}

// Error is a typed failure: an error envelope from Halo, or a failure of the binding itself.
type Error struct {
	Code         string
	Retryable    bool
	RetryAfterMs int64
	Message      string
}

func (e *Error) Error() string { return "halo: " + e.Code + ": " + e.Message }

// Local error codes (the binding's own; Halo's are the generated Code* constants).
const (
	CodeHaloExited = "halo_exited" // the process ended (before the handshake, or during a turn)

	// CapModelPinning is announced by a Halo that pins sessions to a model (Options.Model)
	CapModelPinning = "model_pinning"
	CodeBusy        = "turn_in_progress"
)

// ErrBusy: a turn is already running in this session (turns are sequential; start more sessions for more).
var ErrBusy = &Error{Code: CodeBusy, Message: "a turn is already running in this session"}

// Event is one protocol event of a turn. Unknown event types pass through: Halo adds events within a protocol
// version, and a binding that does not know one simply sees it here.
type Event struct {
	Type   string
	TurnID string
	ID     string
	Data   json.RawMessage
}

// Decode unmarshals the event's payload (into *TurnStartedData, *ProposalData, *StepResultData …).
func (e Event) Decode(v any) error { return json.Unmarshal(e.Data, v) }

// Session is one running halo process: one Halo session.
type Session struct {
	opts Options
	proc Process

	encMu sync.Mutex
	stdin io.WriteCloser

	// Info is the handshake: the session's id, Halo's version and capabilities, and whether it is signed in.
	Info InitializedData

	mu      sync.Mutex
	exec    *Execution
	turnSeq int
	resetCh chan error

	ready     chan error
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// closeGrace bounds how long Close waits for a clean exit before it kills the process.
const closeGrace = 5 * time.Second

// Start launches halo in machine mode and completes the handshake.
func Start(ctx context.Context, opts Options) (*Session, error) {
	launch := opts.Launch
	bin := opts.BinPath
	if launch == nil {
		var err error
		if bin, err = Find(opts.BinPath); err != nil {
			return nil, err
		}
		launch = execLaunch
	}
	stderr := opts.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	proc, err := launch(ctx, bin, []string{"--protocol", "stream-json"}, opts.Cwd, stderr)
	if err != nil {
		return nil, fmt.Errorf("starting halo: %w", err)
	}
	s := &Session{opts: opts, proc: proc, stdin: proc.Stdin(), ready: make(chan error, 1), done: make(chan struct{})}
	go s.readLoop(proc.Stdout())
	init := InitializeData{Protocol: ProtocolVersion, Cwd: opts.Cwd, Client: ClientInfo{Name: opts.ClientName, Version: opts.ClientVersion},
		PermissionMode: opts.PermissionMode, AllowedOperations: opts.AllowedOperations, DisallowedOperations: opts.DisallowedOperations,
		AllowCatastrophic: opts.AllowCatastrophic, AllowUnconfined: opts.AllowUnconfined, SettingSources: opts.SettingSources, Require: opts.Require, Model: opts.Model}
	if init.Client.Name == "" {
		init.Client.Name = "halo-sdk-go"
	}
	if opts.Interactor != nil {
		init.Capabilities = []string{"approvals", "choices", "offers"}
	}
	if err := s.send(Envelope{Type: MsgInitialize}, init); err != nil {
		s.Close()
		return nil, err
	}
	select {
	case err := <-s.ready:
		if err == nil {
			err = checkVersion(s.Info, opts)
		}
		if err != nil {
			s.Close()
			return nil, err
		}
		return s, nil
	case <-s.done:
		s.Close()
		return nil, &Error{Code: CodeHaloExited, Message: "halo exited before the handshake (is it a Halo with machine mode? check Options.Stderr)"}
	case <-ctx.Done():
		s.Close()
		return nil, ctx.Err()
	}
}

func checkVersion(info InitializedData, opts Options) error {
	if info.Protocol != ProtocolVersion {
		return &Error{Code: CodeVersionMismatch, Message: "halo speaks machine protocol " + info.Protocol + ", this SDK " + ProtocolVersion + "; run `halo upgrade` or update the SDK"}
	}
	if opts.MinHaloVersion != "" && info.HaloVersion != "dev" && older(info.HaloVersion, opts.MinHaloVersion) {
		return &Error{Code: CodeVersionMismatch, Message: "halo " + info.HaloVersion + " is older than " + opts.MinHaloVersion + "; run `halo upgrade`"}
	}
	// a pinned model is the session's model or no session. A Halo too old to pin models cannot keep the promise;
	// a pin not yet resolved (the service unreachable or signed out at the handshake) is enforced by Halo at the
	// first turn, which then fails with model_not_available rather than use another model
	if opts.Model != "" {
		switch {
		case !slices.Contains(info.Capabilities, CapModelPinning):
			return &Error{Code: CodeVersionMismatch, Message: "this Halo cannot pin a model; run `halo upgrade`"}
		case info.Model != nil && info.Model.ID != opts.Model:
			return &Error{Code: CodeModelNotAvailable, Message: "the session is pinned to " + info.Model.ID + ", not " + opts.Model}
		}
	}
	return nil
}

// Model is the public Halo model this session is pinned to ("" until a signed-out session's first turn).
func (s *Session) Model() string {
	if s.Info.Model != nil {
		return s.Info.Model.ID
	}
	return ""
}

// older compares dotted versions numerically ("v0.1.9" < "0.1.10").
func older(have, want string) bool {
	h := strings.Split(strings.TrimPrefix(have, "v"), ".")
	w := strings.Split(strings.TrimPrefix(want, "v"), ".")
	for i := 0; i < len(h) || i < len(w); i++ {
		var a, b int
		if i < len(h) {
			a, _ = strconv.Atoi(strings.SplitN(h[i], "-", 2)[0])
		}
		if i < len(w) {
			b, _ = strconv.Atoi(strings.SplitN(w[i], "-", 2)[0])
		}
		if a != b {
			return a < b
		}
	}
	return false
}

func (s *Session) send(env Envelope, data any) error {
	env.Version = ProtocolVersion
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		env.Data = raw
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	s.encMu.Lock()
	defer s.encMu.Unlock()
	_, err = s.stdin.Write(append(b, '\n'))
	return err
}

// Run submits a request in words and waits for its outcome. An error is a typed *Error (throttled, not signed in,
// the service unreachable …) or the context's.
func (s *Session) Run(ctx context.Context, prompt string) (*Outcome, error) {
	ex, err := s.Begin(ctx, prompt)
	if err != nil {
		return nil, err
	}
	for range ex.Events() {
	}
	return ex.Outcome()
}

// Begin submits a request and returns its Execution at once. Cancelling ctx cancels the turn.
func (s *Session) Begin(ctx context.Context, prompt string) (*Execution, error) {
	s.mu.Lock()
	if s.exec != nil {
		s.mu.Unlock()
		return nil, ErrBusy
	}
	select {
	case <-s.done:
		s.mu.Unlock()
		return nil, &Error{Code: CodeHaloExited, Message: "the halo session has ended"}
	default:
	}
	s.turnSeq++
	ex := newExecution(s, ctx, "t"+strconv.Itoa(s.turnSeq))
	s.exec = ex
	s.mu.Unlock()
	if err := s.send(Envelope{Type: MsgUserTurn, TurnID: ex.turnID}, UserTurnData{Text: prompt}); err != nil {
		s.finish(ex, nil, &Error{Code: CodeHaloExited, Message: err.Error()})
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			ex.Cancel()
		case <-ex.finished:
		}
	}()
	return ex, nil
}

// Reset starts a new Halo session in the same process: held results and context are cleared, and anything
// remembered for the session is forgotten.
func (s *Session) Reset(ctx context.Context) error {
	s.mu.Lock()
	if s.exec != nil {
		s.mu.Unlock()
		return ErrBusy
	}
	ch := make(chan error, 1)
	s.resetCh = ch
	s.mu.Unlock()
	if err := s.send(Envelope{Type: MsgReset}, nil); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-s.done:
		return &Error{Code: CodeHaloExited, Message: "the halo session has ended"}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close ends the session: it cancels a running turn, closes stdin (EOF ends the session) and kills the process if
// it has not exited after a grace period. Safe to call more than once.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		ex := s.exec
		s.mu.Unlock()
		if ex != nil {
			ex.Cancel()
		}
		_ = s.stdin.Close()
		// read everything halo still writes before waiting for it (os/exec: Wait closes the pipes)
		select {
		case <-s.done:
		case <-time.After(closeGrace):
			_ = s.proc.Kill()
			<-s.done
		}
		s.closeErr = s.proc.Wait()
	})
	return s.closeErr
}

func (s *Session) readLoop(stdout io.Reader) {
	defer func() {
		close(s.done)
		s.mu.Lock()
		ex := s.exec
		s.mu.Unlock()
		if ex != nil {
			s.finish(ex, nil, &Error{Code: CodeHaloExited, Message: "halo exited during the turn"})
		}
	}()
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	handshook := false
	for sc.Scan() {
		var env Envelope
		if json.Unmarshal(sc.Bytes(), &env) != nil || env.Type == "" {
			continue // a malformed line is skipped
		}
		if !handshook {
			switch env.Type {
			case MsgInitialized:
				handshook = true
				_ = json.Unmarshal(env.Data, &s.Info)
				s.ready <- nil
			case MsgError:
				s.ready <- decodeError(env)
				return
			}
			continue
		}
		switch env.Type {
		case MsgApprovalRequest, MsgChoiceRequest, MsgOfferRequest:
			go s.answer(env)
			continue
		case MsgSessionReset:
			s.resetDone(nil)
			continue
		}
		s.mu.Lock()
		ex := s.exec
		s.mu.Unlock()
		if env.Type == MsgError && env.TurnID == "" {
			s.resetDone(decodeError(env))
			continue
		}
		if ex == nil || env.TurnID != ex.turnID {
			continue // not this session's running turn (a late event after its outcome)
		}
		switch env.Type {
		case MsgOutcome:
			var o Outcome
			if err := json.Unmarshal(env.Data, &o); err != nil {
				s.finish(ex, nil, &Error{Code: CodeInternal, Message: "unreadable outcome: " + err.Error()})
				continue
			}
			s.finish(ex, &o, nil)
		case MsgError:
			s.finish(ex, nil, decodeError(env))
		default:
			ex.push(Event{Type: env.Type, TurnID: env.TurnID, ID: env.ID, Data: env.Data})
		}
	}
}

func (s *Session) resetDone(err error) {
	s.mu.Lock()
	ch := s.resetCh
	s.resetCh = nil
	s.mu.Unlock()
	if ch != nil {
		ch <- err
	}
}

func decodeError(env Envelope) *Error {
	var d ErrorData
	_ = json.Unmarshal(env.Data, &d)
	return &Error{Code: d.Code, Retryable: d.Retryable, RetryAfterMs: d.RetryAfterMs, Message: d.Message}
}

func (s *Session) finish(ex *Execution, o *Outcome, err error) {
	s.mu.Lock()
	if s.exec == ex {
		s.exec = nil
	}
	s.mu.Unlock()
	ex.end(o, err)
}

// answer asks the Interactor and replies with the request's id. With no Interactor: deny, cancel, no.
func (s *Session) answer(env Envelope) {
	ctx := context.Background()
	s.mu.Lock()
	if ex := s.exec; ex != nil && ex.turnID == env.TurnID {
		ctx = ex.ctx
	}
	s.mu.Unlock()
	in := s.opts.Interactor
	reply := Envelope{ID: env.ID, TurnID: env.TurnID}
	var data any
	switch env.Type {
	case MsgApprovalRequest:
		reply.Type = MsgApprovalResponse
		a := Approval{Decision: DecisionDeny, Message: "denied by the SDK default (no Interactor)"}
		var req ApprovalRequest
		if in != nil && json.Unmarshal(env.Data, &req) == nil {
			a = in.Approve(ctx, req)
		}
		if a.Decision != DecisionAllow && a.Decision != DecisionAllowSession {
			a.Decision = DecisionDeny
		}
		data = ApprovalResponseData{Decision: a.Decision, Message: a.Message}
	case MsgChoiceRequest:
		reply.Type = MsgChoiceResponse
		r := ChoiceResponseData{Cancel: true}
		var req ChoiceRequest
		if in != nil && json.Unmarshal(env.Data, &req) == nil {
			if n, ok := in.Choose(ctx, req); ok {
				r = ChoiceResponseData{Index: n}
			}
		}
		data = r
	case MsgOfferRequest:
		reply.Type = MsgOfferResponse
		var req OfferRequest
		yes := in != nil && json.Unmarshal(env.Data, &req) == nil && in.Offer(ctx, req)
		data = OfferResponseData{Yes: yes}
	}
	_ = s.send(reply, data)
}

// Execution is one running turn.
type Execution struct {
	s      *Session
	ctx    context.Context
	turnID string

	mu       sync.Mutex
	queue    []Event
	wake     chan struct{}
	events   chan Event
	finished chan struct{}
	outcome  *Outcome
	err      error
	ended    bool
	cancel   sync.Once
}

func newExecution(s *Session, ctx context.Context, id string) *Execution {
	ex := &Execution{s: s, ctx: ctx, turnID: id, wake: make(chan struct{}, 1), events: make(chan Event), finished: make(chan struct{})}
	go ex.pump()
	return ex
}

// pump delivers events in order without ever blocking the session's reader (an unbounded queue).
func (ex *Execution) pump() {
	defer close(ex.events)
	for {
		ex.mu.Lock()
		if len(ex.queue) == 0 {
			ended := ex.ended
			ex.mu.Unlock()
			if ended {
				return
			}
			<-ex.wake
			continue
		}
		e := ex.queue[0]
		ex.queue = ex.queue[1:]
		ex.mu.Unlock()
		ex.events <- e
	}
}

func (ex *Execution) push(e Event) {
	ex.mu.Lock()
	ex.queue = append(ex.queue, e)
	ex.mu.Unlock()
	select {
	case ex.wake <- struct{}{}:
	default:
	}
}

func (ex *Execution) end(o *Outcome, err error) {
	ex.mu.Lock()
	if ex.ended {
		ex.mu.Unlock()
		return
	}
	ex.ended, ex.outcome, ex.err = true, o, err
	ex.mu.Unlock()
	close(ex.finished)
	select {
	case ex.wake <- struct{}{}:
	default:
	}
}

// TurnID is the turn's id in the protocol.
func (ex *Execution) TurnID() string { return ex.turnID }

// Events are the turn's events in order (turn_started, proposal, step_result, and any newer type); the channel is
// closed after the turn ends. Approval, choice and offer requests go to the Interactor, not here.
func (ex *Execution) Events() <-chan Event { return ex.events }

// Outcome waits for the turn's outcome.
func (ex *Execution) Outcome() (*Outcome, error) {
	<-ex.finished
	return ex.outcome, ex.err
}

// Cancel interrupts the turn (idempotent); its outcome is then cancelled unless it had already finished.
func (ex *Execution) Cancel() {
	ex.cancel.Do(func() {
		select {
		case <-ex.finished:
			return
		default:
		}
		_ = ex.s.send(Envelope{Type: MsgCancel, TurnID: ex.turnID}, nil)
	})
}

// execLaunch runs the binary.
func execLaunch(ctx context.Context, bin string, args []string, dir string, stderr io.Writer) (Process, error) {
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Stderr = stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &cmdProcess{cmd: cmd, in: in, out: out}, nil
}

type cmdProcess struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out io.Reader
}

func (p *cmdProcess) Stdin() io.WriteCloser { return p.in }
func (p *cmdProcess) Stdout() io.Reader     { return p.out }
func (p *cmdProcess) Wait() error           { return p.cmd.Wait() }
func (p *cmdProcess) Kill() error           { return p.cmd.Process.Kill() }
