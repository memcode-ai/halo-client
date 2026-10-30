// Package halo drives the installed Halo binary from Go: it starts `halo --protocol stream-json`, speaks Halo's
// machine protocol (newline-delimited JSON on the process's stdin and stdout) and routes approvals, choices and
// offers to an Interactor. It holds only process lifecycle, protocol I/O and callbacks: interpretation,
// permissions, previews, confinement and execution all stay inside Halo.
//
// One Client owns one long-lived halo process; create it once, when your program starts, and reuse it. A Session
// is one conversation inside that process (not a process): open one per conversation and close it when done.
//
//	c, err := halo.Connect(ctx, halo.ClientOptions{})
//	defer c.Close()
//	s, err := c.NewSession(ctx, halo.SessionOptions{Cwd: "/path/to/project"})
//	defer s.Close(ctx)
//	out, err := s.Run(ctx, "what is using port 5432?")
//
// Connecting and opening sessions are local: only a turn that needs interpreting reaches the Halo service.
//
// Halo acts on the machine and account it runs on. Never forward untrusted users' requests to it; a service that
// takes requests from other people must isolate each user's Halo in a container or VM.
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
	Warning         = WarningData
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

// Usage hints (ClientOptions.Usage): guidance only; they never change what Halo may do.
const (
	UsageLocal  = "local"  // a tool or agent on the user's own machine, acting for that user (the default)
	UsageHosted = "hosted" // a service: Halo warns about permissive modes and inherited user settings
)

// Warning codes (advice; nothing is refused).
const (
	WarnFrequentStarts    = "frequent_process_starts"
	WarnHostedPermissive  = "hosted_permissive_mode"
	WarnHostedUserSetting = "hosted_user_settings"
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

// ClientOptions configure the client and its one halo process.
type ClientOptions struct {
	BinPath string // "" discovers it (Find)

	ClientName    string // the host, as Halo records it ("" = "halo-sdk-go")
	ClientVersion string
	Usage         string // UsageLocal (default) or UsageHosted

	// Interactor answers approvals, choices and offers for every session that does not set its own. Without one
	// (here or on a session) Halo never asks.
	Interactor Interactor

	// Require names capabilities the client needs; an older Halo then fails Connect with version_mismatch.
	Require []string
	// MinHaloVersion refuses an installed Halo older than this ("0.2.0"); a development build ("dev") passes.
	MinHaloVersion string

	Stderr io.Writer // halo's diagnostics; nil discards them
	// Launch starts the process (tests); nil runs the binary.
	Launch func(ctx context.Context, bin string, args []string, dir string, stderr io.Writer) (Process, error)
}

// SessionOptions configure one logical session. The zero value runs in the client's folder, under the default
// permission mode, with the client's Interactor.
type SessionOptions struct {
	Cwd string // the session's trusted folder; "" is the halo process's

	// Session permissions: Halo's permission mode (ModeDefault, ModeDontAsk, ModeAcceptEdits, ModePlan,
	// ModeBypassPermissions), operations pre-approved or always denied ("git.*", "files.delete"), the two
	// explicit flags, and the CLI settings to inherit (none unless named: "user", "project").
	PermissionMode       string
	AllowedOperations    []string
	DisallowedOperations []string
	AllowCatastrophic    bool
	AllowUnconfined      bool
	SettingSources       []string

	// Model pins the session to an exact public Halo model ("halo-orbit-preview"); "" takes the default, pinned
	// by the session's first turn. A model the service does not serve fails that turn with model_not_available:
	// Halo never substitutes another model, and updating Halo never changes a pinned one.
	Model string

	// IdleTimeout closes the session after this long without a turn (0: never). The caller's choice.
	IdleTimeout time.Duration

	// Interactor overrides the client's for this session (it needs the client to have one, so that Halo asks).
	Interactor Interactor
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
	CodeHaloExited = "halo_exited"   // the process ended (before the handshake, or with work outstanding)
	CodeClosed     = "client_closed" // the client or session was closed
	CodeBusy       = "turn_in_progress"
	// CapModelPinning is announced by a Halo that pins sessions to a model (SessionOptions.Model).
	CapModelPinning = "model_pinning"
	// CapSessions is announced by a Halo that holds several sessions in one process.
	CapSessions = "sessions"
)

// ErrBusy: a turn is already running in this session (turns in one session are sequential; open more sessions
// for parallel work).
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

// Client is one running halo process. Create it once and reuse it; it is safe for concurrent use.
type Client struct {
	opts ClientOptions
	proc Process

	encMu sync.Mutex
	stdin io.WriteCloser

	// Info is the handshake: Halo's version and capabilities, and whether it is signed in.
	Info InitializedData

	mu       sync.Mutex
	sessions map[string]*Session
	waiting  map[string]chan Envelope // lifecycle requests by id: create_session, close_session, reset, shutdown
	creating map[string]*Session      // sessions being created, by create_session id: the reader registers them
	warnings []Warning
	seq      map[byte]int // per-kind request counters (s, x, r, q) and turns (t)

	ready     chan error
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// closeGrace bounds how long Close waits for a clean shutdown before it kills the process.
const closeGrace = 5 * time.Second

// Connect launches halo in machine mode and completes the handshake. It talks to nothing but the local process.
func Connect(ctx context.Context, opts ClientOptions) (*Client, error) {
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
	proc, err := launch(ctx, bin, []string{"--protocol", "stream-json"}, "", stderr)
	if err != nil {
		return nil, fmt.Errorf("starting halo: %w", err)
	}
	c := &Client{opts: opts, proc: proc, stdin: proc.Stdin(), sessions: map[string]*Session{}, waiting: map[string]chan Envelope{}, creating: map[string]*Session{},
		seq: map[byte]int{}, ready: make(chan error, 1), done: make(chan struct{})}
	go c.readLoop(proc.Stdout())
	init := InitializeData{Protocol: ProtocolVersion, Client: ClientInfo{Name: opts.ClientName, Version: opts.ClientVersion},
		Require: opts.Require, Usage: opts.Usage}
	if init.Client.Name == "" {
		init.Client.Name = "halo-sdk-go"
	}
	if opts.Interactor != nil {
		init.Capabilities = []string{"approvals", "choices", "offers"}
	}
	if err := c.send(Envelope{Type: MsgInitialize}, init); err != nil {
		c.kill()
		return nil, err
	}
	select {
	case err := <-c.ready:
		if err == nil {
			err = checkVersion(c.Info, opts)
		}
		if err != nil {
			c.kill()
			return nil, err
		}
		return c, nil
	case <-c.done:
		c.kill()
		return nil, &Error{Code: CodeHaloExited, Message: "halo exited before the handshake (is it a Halo with machine mode? check ClientOptions.Stderr)"}
	case <-ctx.Done():
		c.kill()
		return nil, ctx.Err()
	}
}

func checkVersion(info InitializedData, opts ClientOptions) error {
	if info.Protocol != ProtocolVersion {
		return &Error{Code: CodeVersionMismatch, Message: "halo speaks machine protocol " + info.Protocol + ", this SDK " + ProtocolVersion + "; run `halo upgrade` or update the SDK"}
	}
	if opts.MinHaloVersion != "" && info.HaloVersion != "dev" && older(info.HaloVersion, opts.MinHaloVersion) {
		return &Error{Code: CodeVersionMismatch, Message: "halo " + info.HaloVersion + " is older than " + opts.MinHaloVersion + "; run `halo upgrade`"}
	}
	return nil
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

// Warnings are the advice Halo has sent so far about how this client uses it (process-wide).
func (c *Client) Warnings() []Warning {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Warning(nil), c.warnings...)
}

func (c *Client) next(kind byte) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq[kind]++
	return string(kind) + strconv.Itoa(c.seq[kind])
}

func (c *Client) send(env Envelope, data any) error {
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
	c.encMu.Lock()
	defer c.encMu.Unlock()
	_, err = c.stdin.Write(append(b, '\n'))
	return err
}

// request sends a lifecycle request and waits for the envelope answering its id.
func (c *Client) request(ctx context.Context, env Envelope, data any) (Envelope, error) {
	ch := make(chan Envelope, 1)
	c.mu.Lock()
	c.waiting[env.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiting, env.ID)
		c.mu.Unlock()
	}()
	if err := c.send(env, data); err != nil {
		return Envelope{}, &Error{Code: CodeHaloExited, Message: err.Error()}
	}
	select {
	case e := <-ch:
		if e.Type == MsgError {
			return e, decodeError(e)
		}
		return e, nil
	case <-c.done:
		return Envelope{}, &Error{Code: CodeHaloExited, Message: "halo exited"}
	case <-ctx.Done():
		return Envelope{}, ctx.Err()
	}
}

// NewSession opens a logical session in the client's process. It is local: nothing reaches the Halo service.
func (c *Client) NewSession(ctx context.Context, opts SessionOptions) (*Session, error) {
	if opts.Model != "" && !slices.Contains(c.Info.Capabilities, CapModelPinning) {
		return nil, &Error{Code: CodeVersionMismatch, Message: "this Halo cannot pin a model; run `halo upgrade`"}
	}
	data := CreateSessionData{Cwd: opts.Cwd, PermissionMode: opts.PermissionMode, AllowedOperations: opts.AllowedOperations,
		DisallowedOperations: opts.DisallowedOperations, AllowCatastrophic: opts.AllowCatastrophic, AllowUnconfined: opts.AllowUnconfined,
		SettingSources: opts.SettingSources, Model: opts.Model, IdleTimeoutMs: opts.IdleTimeout.Milliseconds()}
	id := c.next('s')
	// the reader registers the session the moment session_created arrives: Halo may send its events right after
	s := &Session{c: c, opts: opts, closed: make(chan struct{})}
	ch := make(chan Envelope, 1)
	c.mu.Lock()
	c.waiting[id], c.creating[id] = ch, s
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiting, id)
		delete(c.creating, id)
		c.mu.Unlock()
	}()
	if err := c.send(Envelope{Type: MsgCreateSession, ID: id}, data); err != nil {
		return nil, &Error{Code: CodeHaloExited, Message: err.Error()}
	}
	select {
	case e := <-ch:
		if e.Type == MsgError {
			return nil, decodeError(e)
		}
		return s, nil
	case <-c.done:
		return nil, &Error{Code: CodeHaloExited, Message: "halo exited"}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close shuts the process down: Halo cancels running turns and closes every session, then exits; the process is
// killed if it has not exited after a grace period. Safe to call more than once.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), closeGrace)
		defer cancel()
		select {
		case <-c.done:
		default:
			_, _ = c.request(ctx, Envelope{Type: MsgShutdown, ID: c.next('q')}, nil)
		}
		_ = c.stdin.Close()
		select {
		case <-c.done:
		case <-time.After(closeGrace):
			_ = c.proc.Kill()
			<-c.done
		}
		c.closeErr = c.proc.Wait()
	})
	return c.closeErr
}

// kill ends a process that never completed the handshake.
func (c *Client) kill() {
	c.closeOnce.Do(func() {
		_ = c.stdin.Close()
		select {
		case <-c.done:
		case <-time.After(closeGrace):
			_ = c.proc.Kill()
			<-c.done
		}
		c.closeErr = c.proc.Wait()
	})
}

func (c *Client) readLoop(stdout io.Reader) {
	defer func() {
		close(c.done)
		c.mu.Lock()
		var open []*Session
		for _, s := range c.sessions {
			open = append(open, s)
		}
		c.mu.Unlock()
		for _, s := range open {
			s.ended(&Error{Code: CodeHaloExited, Message: "halo exited"})
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
				_ = json.Unmarshal(env.Data, &c.Info)
				c.ready <- nil
			case MsgError:
				c.ready <- decodeError(env)
				return
			}
			continue
		}
		c.route(env)
	}
}

// route delivers one envelope: to a waiting lifecycle request, a session, or the client's warnings.
func (c *Client) route(env Envelope) {
	c.mu.Lock()
	if env.Type == MsgSessionCreated {
		if s := c.creating[env.ID]; s != nil {
			var d SessionCreatedData
			_ = json.Unmarshal(env.Data, &d)
			s.id, s.model = d.SessionID, d.Model
			c.sessions[s.id] = s
		}
	}
	ch := c.waiting[env.ID]
	s := c.sessions[env.SessionID]
	c.mu.Unlock()
	switch env.Type {
	case MsgWarning:
		var w Warning
		_ = json.Unmarshal(env.Data, &w)
		c.mu.Lock()
		c.warnings = append(c.warnings, w)
		c.mu.Unlock()
		if s != nil {
			s.mu.Lock()
			s.warnings = append(s.warnings, w)
			s.mu.Unlock()
		}
		return
	case MsgSessionClosed:
		if s != nil {
			s.ended(&Error{Code: CodeClosed, Message: "the session is closed"})
		}
		c.mu.Lock()
		delete(c.sessions, env.SessionID)
		c.mu.Unlock()
	}
	if ch != nil && (env.Type != MsgError || env.TurnID == "") && env.Type != MsgApprovalRequest && env.Type != MsgChoiceRequest && env.Type != MsgOfferRequest {
		ch <- env
		return
	}
	if s != nil {
		s.route(env)
	}
}

func decodeError(env Envelope) *Error {
	var d ErrorData
	_ = json.Unmarshal(env.Data, &d)
	return &Error{Code: d.Code, Retryable: d.Retryable, RetryAfterMs: d.RetryAfterMs, Message: d.Message}
}

// Session is one conversation in the client's process: its own context, held results, permissions and model.
type Session struct {
	c    *Client
	opts SessionOptions
	id   string

	mu       sync.Mutex
	model    string
	exec     *Execution
	warnings []Warning
	closed   chan struct{}
	once     sync.Once
	endErr   *Error
}

// ID is the session's id in the protocol.
func (s *Session) ID() string { return s.id }

// Model is the public Halo model this session is pinned to: the one asked for, or the default once the first
// turn has pinned it ("" before then).
func (s *Session) Model() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model
}

// Warnings are Halo's advice about this session (for example, a permissive mode under UsageHosted).
func (s *Session) Warnings() []Warning {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Warning(nil), s.warnings...)
}

// ended marks the session over (closed, or the process gone): a running turn ends with err.
func (s *Session) ended(err *Error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.endErr = err
		ex := s.exec
		s.mu.Unlock()
		close(s.closed)
		if ex != nil {
			s.finish(ex, nil, err)
		}
	})
}

// Run submits a request in words and waits for its outcome. An error is a typed *Error (throttled, not signed in,
// the service unreachable, model_not_available …) or the context's.
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
	case <-s.c.done:
		s.mu.Unlock()
		return nil, &Error{Code: CodeHaloExited, Message: "halo has exited"}
	case <-s.closed:
		err := s.endErr
		s.mu.Unlock()
		if err.Code == CodeClosed {
			return nil, &Error{Code: CodeUnknownSession, Message: "the session is closed"}
		}
		return nil, err
	default:
	}
	ex := newExecution(s, ctx, s.c.next('t'))
	s.exec = ex
	s.mu.Unlock()
	if err := s.c.send(Envelope{Type: MsgUserTurn, SessionID: s.id, TurnID: ex.turnID}, UserTurnData{Text: prompt}); err != nil {
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

// Reset clears the session's context, held results and anything remembered for it; the session keeps its id
// and model.
func (s *Session) Reset(ctx context.Context) error {
	s.mu.Lock()
	busy := s.exec != nil
	s.mu.Unlock()
	if busy {
		return ErrBusy
	}
	_, err := s.c.request(ctx, Envelope{Type: MsgReset, SessionID: s.id, ID: s.c.next('r')}, nil)
	return err
}

// Close ends the session (a running turn is cancelled first); the client and its other sessions are untouched.
func (s *Session) Close(ctx context.Context) error {
	select {
	case <-s.closed:
		return nil
	default:
	}
	_, err := s.c.request(ctx, Envelope{Type: MsgCloseSession, SessionID: s.id, ID: s.c.next('x')}, nil)
	if e, ok := err.(*Error); ok && (e.Code == CodeUnknownSession || e.Code == CodeHaloExited) {
		return nil // already over
	}
	return err
}

// route handles one envelope of this session.
func (s *Session) route(env Envelope) {
	switch env.Type {
	case MsgApprovalRequest, MsgChoiceRequest, MsgOfferRequest:
		go s.answer(env)
		return
	}
	s.mu.Lock()
	ex := s.exec
	s.mu.Unlock()
	if ex == nil || env.TurnID != ex.turnID {
		return // not the running turn (a late event after its outcome)
	}
	switch env.Type {
	case MsgOutcome:
		var o Outcome
		if err := json.Unmarshal(env.Data, &o); err != nil {
			s.finish(ex, nil, &Error{Code: CodeInternal, Message: "unreadable outcome: " + err.Error()})
			return
		}
		if o.Model != "" {
			s.mu.Lock()
			s.model = o.Model
			s.mu.Unlock()
		}
		s.finish(ex, &o, nil)
	case MsgError:
		s.finish(ex, nil, decodeError(env))
	default:
		ex.push(Event{Type: env.Type, TurnID: env.TurnID, ID: env.ID, Data: env.Data})
	}
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
	if in == nil {
		in = s.c.opts.Interactor
	}
	reply := Envelope{ID: env.ID, SessionID: s.id, TurnID: env.TurnID}
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
	_ = s.c.send(reply, data)
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

// pump delivers events in order without ever blocking the client's reader (an unbounded queue).
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

// Cancel interrupts the turn (idempotent); its outcome is then cancelled unless it had already finished. It
// reaches only this session's turn.
func (ex *Execution) Cancel() {
	ex.cancel.Do(func() {
		select {
		case <-ex.finished:
			return
		default:
		}
		_ = ex.s.c.send(Envelope{Type: MsgCancel, SessionID: ex.s.id, TurnID: ex.turnID}, nil)
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
