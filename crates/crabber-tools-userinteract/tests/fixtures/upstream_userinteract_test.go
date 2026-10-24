package userinteract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-tools/userinteract"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("SurfaceCLI with defaults succeeds", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceCLI)
		if err != nil {
			t.Fatalf("New(SurfaceCLI) error = %v", err)
		}
		if tool == nil {
			t.Fatal("New(SurfaceCLI) returned nil")
		}
	})

	t.Run("SurfaceMCP with defaults succeeds", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceMCP)
		if err != nil {
			t.Fatalf("New(SurfaceMCP) error = %v", err)
		}
		if tool == nil {
			t.Fatal("New(SurfaceMCP) returned nil")
		}
	})

	t.Run("unknown surface returns error", func(t *testing.T) {
		t.Parallel()
		_, err := userinteract.New(userinteract.Surface("unknown"))
		if err == nil {
			t.Fatal("expected error for unknown surface, got nil")
		}
	})

	t.Run("custom stdin and stderr injected", func(t *testing.T) {
		t.Parallel()
		stdin := strings.NewReader("answer\n\n")
		stderr := &bytes.Buffer{}
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  stdin,
			Stderr: stderr,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Q?"})
		if res.Outcome != userinteract.OutcomeSucceeded {
			t.Fatalf("outcome = %s, want succeeded; error = %+v", res.Outcome, res.Error)
		}
		if !strings.Contains(stderr.String(), "Q?") {
			t.Errorf("stderr %q does not contain prompt", stderr.String())
		}
	})

	t.Run("multiple options returns error", func(t *testing.T) {
		t.Parallel()
		_, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{}, userinteract.Options{})
		if err == nil {
			t.Fatal("expected error for multiple options, got nil")
		}
	})
}

func TestTool_Run_AnswerProvided(t *testing.T) {
	t.Parallel()

	surfaces := []userinteract.Surface{userinteract.SurfaceCLI, userinteract.SurfaceMCP}

	for _, surface := range surfaces {
		t.Run(string(surface), func(t *testing.T) {
			t.Parallel()
			var tool *userinteract.Tool
			var err error
			if surface == userinteract.SurfaceCLI {
				// Inject a panicking reader — if answer in args is used, stdin should never be read.
				tool, err = userinteract.New(surface, userinteract.Options{
					Stdin:  panicReader{},
					Stderr: &bytes.Buffer{},
				})
			} else {
				tool, err = userinteract.New(surface)
			}
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			res := tool.Run(context.Background(), userinteract.Args{
				Question: "What is 2+2?",
				Answer:   "4",
			})
			if res.Outcome != userinteract.OutcomeSucceeded {
				t.Fatalf("outcome = %s, want succeeded; error = %+v", res.Outcome, res.Error)
			}
			if res.Answer != "4" {
				t.Fatalf("answer = %q, want %q", res.Answer, "4")
			}
		})
	}
}

func TestTool_Run_CLI(t *testing.T) {
	t.Parallel()

	t.Run("single-line answer terminated by blank line", func(t *testing.T) {
		t.Parallel()
		stderr := &bytes.Buffer{}
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  strings.NewReader("hello\n\n"),
			Stderr: stderr,
		})
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Say hello"})
		if res.Outcome != userinteract.OutcomeSucceeded {
			t.Fatalf("outcome = %s; error = %+v", res.Outcome, res.Error)
		}
		if res.Answer != "hello" {
			t.Fatalf("answer = %q, want %q", res.Answer, "hello")
		}
	})

	t.Run("multi-line answer joined correctly", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  strings.NewReader("line one\nline two\nline three\n\n"),
			Stderr: &bytes.Buffer{},
		})
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Tell me"})
		if res.Outcome != userinteract.OutcomeSucceeded {
			t.Fatalf("outcome = %s; error = %+v", res.Outcome, res.Error)
		}
		want := "line one\nline two\nline three"
		if res.Answer != want {
			t.Fatalf("answer = %q, want %q", res.Answer, want)
		}
	})

	t.Run("answer terminated by EOF", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  strings.NewReader("eof answer"),
			Stderr: &bytes.Buffer{},
		})
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Q?"})
		if res.Outcome != userinteract.OutcomeSucceeded {
			t.Fatalf("outcome = %s; error = %+v", res.Outcome, res.Error)
		}
		if res.Answer != "eof answer" {
			t.Fatalf("answer = %q, want %q", res.Answer, "eof answer")
		}
	})

	t.Run("question printed to injected stderr", func(t *testing.T) {
		t.Parallel()
		stderr := &bytes.Buffer{}
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  strings.NewReader("\n"),
			Stderr: stderr,
		})
		if err != nil {
			t.Fatal(err)
		}
		tool.Run(context.Background(), userinteract.Args{Question: "Are you there?"})
		if !strings.Contains(stderr.String(), "Are you there?") {
			t.Errorf("stderr %q does not contain prompt", stderr.String())
		}
	})

	t.Run("stdin read error returns io error", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
			Stdin:  errReader{},
			Stderr: &bytes.Buffer{},
		})
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Q?"})
		if res.Outcome != userinteract.OutcomeFailed {
			t.Fatalf("outcome = %s, want failed", res.Outcome)
		}
		if res.Error == nil || res.Error.Category != userinteract.ErrCategoryIO {
			t.Fatalf("error category = %v, want %s", res.Error, userinteract.ErrCategoryIO)
		}
	})
}

func TestTool_Run_MCP(t *testing.T) {
	t.Parallel()

	t.Run("returns OutcomePending immediately", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceMCP)
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "Hello?"})
		if res.Outcome != userinteract.OutcomePending {
			t.Fatalf("outcome = %s, want pending", res.Outcome)
		}
	})

	t.Run("Question field echoed in result", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceMCP)
		if err != nil {
			t.Fatal(err)
		}
		res := tool.Run(context.Background(), userinteract.Args{Question: "What time is it?"})
		if res.Question != "What time is it?" {
			t.Fatalf("Question = %q, want %q", res.Question, "What time is it?")
		}
	})

	t.Run("stdin is NEVER read in MCP mode", func(t *testing.T) {
		t.Parallel()
		// MCP surface tool holds no stdin; construct without options.
		// If any code path reads stdin in MCP mode, it would panic here
		// because the tool holds a nil reader (not a panicReader) — but
		// a nil read would also surface as a crash, so the test relies on
		// the fact that MCP mode must not even attempt a read.
		tool, err := userinteract.New(userinteract.SurfaceMCP)
		if err != nil {
			t.Fatal(err)
		}
		// This must not panic. If it does, MCP mode is reading stdin.
		res := tool.Run(context.Background(), userinteract.Args{Question: "Q?"})
		if res.Outcome != userinteract.OutcomePending {
			t.Fatalf("outcome = %s, want pending", res.Outcome)
		}
	})

	t.Run("no write to stderr in MCP mode", func(t *testing.T) {
		t.Parallel()
		tool, err := userinteract.New(userinteract.SurfaceMCP)
		if err != nil {
			t.Fatal(err)
		}
		// Tool holds no stderr in MCP mode — if it tried to write, it would
		// panic on a nil writer. This must complete without panic.
		res := tool.Run(context.Background(), userinteract.Args{Question: "Q?"})
		if res.Outcome != userinteract.OutcomePending {
			t.Fatalf("outcome = %s, want pending", res.Outcome)
		}
	})
}

func TestTool_Run_Validation(t *testing.T) {
	t.Parallel()

	tool, err := userinteract.New(userinteract.SurfaceCLI, userinteract.Options{
		Stdin:  strings.NewReader(""),
		Stderr: &bytes.Buffer{},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("empty question returns validation error", func(t *testing.T) {
		t.Parallel()
		res := tool.Run(context.Background(), userinteract.Args{Question: ""})
		if res.Outcome != userinteract.OutcomeFailed {
			t.Fatalf("outcome = %s, want failed", res.Outcome)
		}
		if res.Error == nil || res.Error.Category != userinteract.ErrCategoryValidation {
			t.Fatalf("error category = %v, want %s", res.Error, userinteract.ErrCategoryValidation)
		}
	})

	t.Run("whitespace-only question returns validation error", func(t *testing.T) {
		t.Parallel()
		res := tool.Run(context.Background(), userinteract.Args{Question: "   "})
		if res.Outcome != userinteract.OutcomeFailed {
			t.Fatalf("outcome = %s, want failed", res.Outcome)
		}
		if res.Error == nil || res.Error.Category != userinteract.ErrCategoryValidation {
			t.Fatalf("error category = %v, want %s", res.Error, userinteract.ErrCategoryValidation)
		}
	})
}

func TestTool_InvokableRun(t *testing.T) {
	t.Parallel()

	tool, err := userinteract.New(userinteract.SurfaceMCP)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("empty argsJSON returns error", func(t *testing.T) {
		t.Parallel()
		_, err := tool.InvokableRun(context.Background(), "")
		if err == nil {
			t.Fatal("expected error for empty argsJSON, got nil")
		}
	})

	t.Run("duplicate keys rejected", func(t *testing.T) {
		t.Parallel()
		_, err := tool.InvokableRun(context.Background(), `{"question":"q","question":"r"}`)
		if err == nil {
			t.Fatal("expected error for duplicate keys, got nil")
		}
	})

	t.Run("valid JSON round-trips through Run", func(t *testing.T) {
		t.Parallel()
		out, err := tool.InvokableRun(context.Background(), `{"question":"Hello?"}`)
		if err != nil {
			t.Fatalf("InvokableRun error = %v", err)
		}
		if out == "" {
			t.Fatal("expected non-empty output")
		}
	})
}

func TestResult_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	raw := `{"outcome":"pending","question":"Are you there?"}`
	var res userinteract.Result
	if err := (&res).UnmarshalJSON([]byte(raw)); err != nil {
		t.Fatalf("UnmarshalJSON error = %v", err)
	}
	if string(res.RawJSON) != raw {
		t.Fatalf("RawJSON = %s, want %s", res.RawJSON, raw)
	}
	if res.Outcome != userinteract.OutcomePending {
		t.Fatalf("Outcome = %s, want pending", res.Outcome)
	}
	if res.Question != "Are you there?" {
		t.Fatalf("Question = %q, want %q", res.Question, "Are you there?")
	}
}

func TestSchema(t *testing.T) {
	t.Parallel()

	s := userinteract.Schema()
	if len(s) == 0 {
		t.Fatal("Schema() returned empty")
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(s, &obj); err != nil {
		t.Fatalf("Schema() is not valid JSON: %v", err)
	}

	t.Run("additionalProperties is false", func(t *testing.T) {
		t.Parallel()
		ap, ok := obj["additionalProperties"]
		if !ok {
			t.Fatal("additionalProperties not found")
		}
		if ap != false {
			t.Fatalf("additionalProperties = %v, want false", ap)
		}
	})

	t.Run("question is required", func(t *testing.T) {
		t.Parallel()
		req, ok := obj["required"].([]interface{})
		if !ok {
			t.Fatal("required field not found or wrong type")
		}
		found := false
		for _, f := range req {
			if f == "question" {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("question not in required fields")
		}
	})
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		res  userinteract.Result
		want bool
	}{
		{
			name: "succeeded",
			res:  userinteract.Result{Outcome: userinteract.OutcomeSucceeded},
			want: false,
		},
		{
			name: "pending",
			res:  userinteract.Result{Outcome: userinteract.OutcomePending},
			want: false,
		},
		{
			name: "failed/validation",
			res: userinteract.Result{
				Outcome: userinteract.OutcomeFailed,
				Error:   &userinteract.ResultError{Category: userinteract.ErrCategoryValidation},
			},
			want: false,
		},
		{
			name: "failed/io",
			res: userinteract.Result{
				Outcome: userinteract.OutcomeFailed,
				Error:   &userinteract.ResultError{Category: userinteract.ErrCategoryIO},
			},
			want: false,
		},
		{
			name: "failed/unknown",
			res: userinteract.Result{
				Outcome: userinteract.OutcomeFailed,
				Error:   &userinteract.ResultError{Category: userinteract.ErrCategoryUnknown},
			},
			want: false, // intentional divergence from shell/urlfetch; see ADR 0006
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.res.IsRetryable(); got != tt.want {
				t.Fatalf("IsRetryable() = %t, want %t", got, tt.want)
			}
		})
	}
}

// panicReader panics on any Read call — used to assert stdin is never read.
type panicReader struct{}

func (panicReader) Read(_ []byte) (int, error) {
	panic("userinteract: Read called — must not read stdin when answer is provided")
}

// errReader always returns an error from Read.
type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errFakeReadError
}

var errFakeReadError = fmt.Errorf("fake read error")
