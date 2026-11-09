package halo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// The SDK's simple surface: Query for one request, Open for a conversation. It uses one Halo process for the
// application (started on first use).
//
//	r, err := halo.Query(ctx, "what is using port 5432?", halo.Options{})
//	fmt.Println(r.Text)

// Options configure a request or a conversation.
type Options struct {
	Cwd   string // the folder Halo works in; "" is the current directory
	Model string // a public Halo model; "" is the service's default

	// PermissionMode: "default" (what needs approval asks CanUseOperation, or is denied without it), "acceptEdits"
	// (writes in the folder run), "plan" (nothing runs; the result is the proposal), "dontAsk" (what needs approval
	// is denied), "bypassPermissions" (everything runs except disallowed operations and Halo's catastrophic guards).
	PermissionMode       string
	AllowedOperations    []string // run without asking ("git.*", "files.read")
	DisallowedOperations []string // never run, in every mode ("files.delete")

	// CanUseOperation decides what needs approval when the allow list and the mode don't.
	CanUseOperation func(ctx context.Context, op OperationRequest) Permission
	// OnChoice picks among readings when a request has several (-1: none); without it the result asks.
	OnChoice func(ctx context.Context, question string, options []string) int
	// AllowSendingPaths lets Halo send a path's real name to the service when it needs it (default: never).
	AllowSendingPaths bool

	BinPath string // the halo binary; "" discovers it
}

// OperationRequest is what CanUseOperation is told about an operation that needs approval.
type OperationRequest struct {
	Operations   []string // "files.delete", "git.push"
	Effect       string   // READ | WRITE | DESTRUCTIVE | COMMAND | EXECUTE
	Reason       string
	Command      string
	Authorities  []string
	Rememberable bool
}

// Permission is CanUseOperation's answer: Allow(), AllowSession() or Deny(why).
type Permission struct {
	allow, session bool
	message        string
}

func Allow() Permission              { return Permission{allow: true} }
func AllowSession() Permission       { return Permission{allow: true, session: true} } // for the rest of the conversation
func Deny(message string) Permission { return Permission{message: message} }

// QueryResult is a request's result: the answer as Halo would say it, and the same thing typed.
type QueryResult struct {
	OK        bool   // done or answered
	Status    string // done | answered | needs_input | proposed | unsupported | denied | cancelled | failed
	Text      string // what Halo would say
	Rows      []Row  // typed rows when the result is a table; otherwise empty
	Columns   []string
	Value     any      // a single value (a count, a total, a calculation, yes/no), when the result is one
	Question  string   // needs_input: what Halo asks; answer with another query
	Options   []string // and the options when it is a choice
	Operation string
	Command   string
	SessionID string
	Outcome   *Outcome // the full outcome
}

// Query asks Halo one thing and returns its typed result. A request Halo declines or can't do is a result
// (Status), not an error; errors are failures to reach Halo (not installed, not signed in, throttled).
func Query(ctx context.Context, prompt string, opts Options) (*QueryResult, error) {
	s, err := Open(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer s.Close(context.Background())
	return s.Query(ctx, prompt)
}

// Open starts a conversation: follow-ups ("kill it", "only the PDFs") refer to earlier results. Close it when done.
func Open(ctx context.Context, opts Options) (*Session, error) {
	c, err := shared(ctx, opts.BinPath)
	if err != nil {
		return nil, err
	}
	return c.NewSession(ctx, SessionOptions{Cwd: opts.Cwd, Model: opts.Model, PermissionMode: opts.PermissionMode,
		AllowedOperations: opts.AllowedOperations, DisallowedOperations: opts.DisallowedOperations,
		Interactor: optionsInteractor{opts}})
}

// Query asks within this conversation.
func (s *Session) Query(ctx context.Context, prompt string) (*QueryResult, error) {
	o, err := s.Run(ctx, prompt)
	if err != nil {
		return nil, err
	}
	return ToResult(o, s.ID()), nil
}

var (
	sharedMu     sync.Mutex
	sharedClient *Client
)

func shared(ctx context.Context, bin string) (*Client, error) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedClient != nil {
		select {
		case <-sharedClient.done:
		default:
			return sharedClient, nil
		}
	}
	c, err := Connect(ctx, ClientOptions{BinPath: bin, ClientName: "halo-sdk-go", Interactor: optionsInteractor{}})
	if err != nil {
		return nil, err
	}
	sharedClient = c
	return c, nil
}

type optionsInteractor struct{ o Options }

func (i optionsInteractor) Approve(ctx context.Context, r ApprovalRequest) Approval {
	if i.o.CanUseOperation == nil {
		return Approval{Decision: DecisionDeny, Message: "needs approval: allow it with AllowedOperations, PermissionMode or CanUseOperation"}
	}
	req := OperationRequest{Operations: r.Operations, Effect: r.Effect, Reason: r.Reason, Rememberable: r.Rememberable}
	if r.Plan != nil {
		req.Command = r.Plan.Command
	}
	for _, a := range r.Authorities {
		req.Authorities = append(req.Authorities, a.Name)
	}
	p := i.o.CanUseOperation(ctx, req)
	switch {
	case p.allow && p.session:
		return Approval{Decision: DecisionAllowSession}
	case p.allow:
		return Approval{Decision: DecisionAllow}
	}
	if p.message == "" {
		p.message = "denied"
	}
	return Approval{Decision: DecisionDeny, Message: p.message}
}

func (i optionsInteractor) Choose(ctx context.Context, r ChoiceRequest) (int, bool) {
	if i.o.OnChoice == nil {
		return 0, false
	}
	n := i.o.OnChoice(ctx, r.Question, r.Options)
	return n, n >= 0
}

func (i optionsInteractor) Offer(ctx context.Context, _ OfferRequest) bool {
	return i.o.AllowSendingPaths
}

var statusOf = map[string]string{"answer": "answered", "result": "done", "question": "needs_input", "choice": "needs_input",
	"proposed": "proposed", "unsupported": "unsupported", "unrecognized": "unsupported", "denied": "denied", "declined": "denied",
	"cancelled": "cancelled", "failed": "failed"}

// ToResult turns an outcome into a QueryResult.
func ToResult(o *Outcome, sessionID string) *QueryResult {
	r := &QueryResult{Status: statusOf[o.Kind], Question: o.Question, Options: o.Options, Command: o.Command,
		SessionID: sessionID, Outcome: o}
	if r.Status == "" {
		r.Status = "failed"
	}
	r.OK = r.Status == "done" || r.Status == "answered"
	if o.Plan != nil {
		r.Operation = o.Plan.Operation
		if r.Command == "" {
			r.Command = o.Plan.Command
		}
	}
	res := o.Result
	if res == nil && o.Program != nil {
		for i := len(o.Program.Steps) - 1; i >= 0; i-- {
			if o.Program.Steps[i].Result != nil {
				res = o.Program.Steps[i].Result
				break
			}
		}
	}
	if res != nil {
		r.Rows, r.Columns = res.Rows, res.Columns
		if len(r.Rows) == 0 && len(res.Files) > 0 {
			for _, f := range res.Files {
				r.Rows = append(r.Rows, Row{"path": f.Path, "size": f.Size, "modified": f.Modified})
			}
			r.Columns = []string{"path", "size", "modified"}
		}
		if len(r.Rows) > 0 && len(r.Columns) == 0 {
			for k := range r.Rows[0] {
				r.Columns = append(r.Columns, k)
			}
		}
		if a := res.Answer; a != nil {
			switch {
			case a.Number != nil:
				r.Value = *a.Number
			case a.Count != nil:
				r.Value = *a.Count
			case a.Exists != nil:
				r.Value = *a.Exists
			}
		}
	}
	if r.Value == nil && o.Kind == "answer" && o.Message != "" {
		r.Value = o.Message
	}
	switch {
	case o.Message != "":
		r.Text = o.Message
	case o.Question != "":
		r.Text = o.Question
	case res != nil && res.Note != "" && (len(r.Rows) == 0 || r.Value != nil):
		r.Text = res.Note
	case res != nil && res.Text != "":
		r.Text = res.Text
	case len(r.Rows) > 0:
		r.Text = table(r.Columns, r.Rows)
	default:
		r.Text = o.Reason
	}
	return r
}

func table(cols []string, rows []Row) string {
	cell := func(v any) string {
		switch x := v.(type) {
		case nil:
			return ""
		case string:
			return x
		case map[string]any, []any:
			b, _ := json.Marshal(x)
			return string(b)
		}
		return fmt.Sprint(v)
	}
	w := make([]int, len(cols))
	for i, c := range cols {
		w[i] = len(c)
		for _, r := range rows {
			w[i] = max(w[i], len(cell(r[c])))
		}
	}
	var b strings.Builder
	line := func(vals []string) {
		for i, v := range vals {
			if i > 0 {
				b.WriteString("  ")
			}
			b.WriteString(v + strings.Repeat(" ", w[i]-len(v)))
		}
		b.WriteString("\n")
	}
	var head []string
	for _, c := range cols {
		head = append(head, strings.ToUpper(c))
	}
	line(head)
	for _, r := range rows {
		var vals []string
		for _, c := range cols {
			vals = append(vals, cell(r[c]))
		}
		line(vals)
	}
	out := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	for i := range out {
		out[i] = strings.TrimRight(out[i], " ")
	}
	return strings.Join(out, "\n")
}
