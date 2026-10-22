package applypatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-tools/result"
)

func writeFixture(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func readFixture(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", rel, err)
	}
	return string(data)
}

func TestRunAppliesAddUpdateMoveAndDelete(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "old.txt", "hello\nold\nbye\n")
	writeFixture(t, root, "move.txt", "name\n")
	writeFixture(t, root, "delete.txt", "remove\n")
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	patch := `*** Begin Patch
*** Add File: new/added.txt
+first
+second
*** Update File: old.txt
@@
 hello
-old
+new
 bye
*** Update File: move.txt
*** Move to: moved.txt
@@
-name
+renamed
*** Delete File: delete.txt
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	if res.Outcome != result.OutcomeSucceeded {
		t.Fatalf("Outcome = %q err=%+v files=%+v", res.Outcome, res.Error, res.Files)
	}
	if res.Partial {
		t.Fatal("Partial = true on success")
	}
	if readFixture(t, root, "new/added.txt") != "first\nsecond\n" {
		t.Fatalf("added content mismatch")
	}
	if readFixture(t, root, "old.txt") != "hello\nnew\nbye\n" {
		t.Fatalf("updated content mismatch")
	}
	if readFixture(t, root, "moved.txt") != "renamed\n" {
		t.Fatalf("moved content mismatch")
	}
	if _, err := os.Stat(filepath.Join(root, "move.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("move source still exists or stat failed unexpectedly: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "delete.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete target still exists or stat failed unexpectedly: %v", err)
	}
}

func TestRunPreflightRejectsAndDoesNotWrite(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "target.txt", "alpha\n")
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Add File: created.txt
+created
*** Update File: target.txt
@@
-missing
+changed
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	assertCategory(t, res, ErrCategoryConflict)
	if _, err := os.Stat(filepath.Join(root, "created.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created.txt exists after failed preflight or stat failed unexpectedly: %v", err)
	}
	if readFixture(t, root, "target.txt") != "alpha\n" {
		t.Fatalf("target changed despite failed preflight")
	}
	if res.Partial {
		t.Fatalf("Partial = true for preflight failure")
	}
}

func TestRunRejectsLineHunkSuffixMatch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "target.txt", "prefixfoo\n")
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Update File: target.txt
@@
-foo
+bar
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	assertCategory(t, res, ErrCategoryConflict)
	if readFixture(t, root, "target.txt") != "prefixfoo\n" {
		t.Fatalf("target changed despite non-line-anchored context")
	}
}

func TestRunRejectsDuplicateTargetsAndPathEscapes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dup := `*** Begin Patch
*** Add File: same.txt
+one
*** Add File: same.txt
+two
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: dup}), ErrCategoryValidation)

	escape := `*** Begin Patch
*** Add File: ../outside.txt
+nope
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: escape}), ErrCategoryPathEscape)
}

func TestRunRejectsParentChildTargetsBeforeWriting(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Add File: foo
+plain file
*** Add File: foo/bar.txt
+nested file
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	assertCategory(t, res, ErrCategoryValidation)
	if _, err := os.Stat(filepath.Join(root, "foo")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foo was written despite preflight failure or stat failed unexpectedly: %v", err)
	}
}

func TestRunRejectsExistingAddMissingUpdateAndMissingDelete(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "exists.txt", "exists\n")
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	existingAdd := `*** Begin Patch
*** Add File: exists.txt
+new
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: existingAdd}), ErrCategoryValidation)

	missingUpdate := `*** Begin Patch
*** Update File: missing.txt
@@
-old
+new
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: missingUpdate}), ErrCategoryNotFound)

	missingDelete := `*** Begin Patch
*** Delete File: missing.txt
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: missingDelete}), ErrCategoryNotFound)
}

func TestRunRejectsSymlinkDeleteSourceWithoutRemovingTarget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "real.txt", "real\n")
	if err := os.Symlink(filepath.Join(root, "real.txt"), filepath.Join(root, "link.txt")); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatalf("Symlink: %v", err)
	}
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Delete File: link.txt
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	assertCategory(t, res, ErrCategoryUnsupported)
	if readFixture(t, root, "real.txt") != "real\n" {
		t.Fatalf("real target changed")
	}
	if _, err := os.Lstat(filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("link was removed despite rejection: %v", err)
	}
}

func TestRunDeletesBinaryFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bin.dat"), []byte{'b', 0, 'n'}, 0o600); err != nil {
		t.Fatalf("write binary fixture: %v", err)
	}
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Delete File: bin.dat
*** End Patch
`
	res := tool.Run(context.Background(), Args{PatchText: patch})
	if res.Outcome != result.OutcomeSucceeded {
		t.Fatalf("Outcome = %q err=%+v", res.Outcome, res.Error)
	}
	if _, err := os.Stat(filepath.Join(root, "bin.dat")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("binary file still exists or stat failed unexpectedly: %v", err)
	}
}

func TestRunRejectsSymlinkEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := t.TempDir()
	writeFixture(t, outside, "outside.txt", "outside\n")
	if err := os.Symlink(filepath.Join(outside, "outside.txt"), filepath.Join(root, "link.txt")); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatalf("Symlink: %v", err)
	}
	tool, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	patch := `*** Begin Patch
*** Update File: link.txt
@@
-outside
+changed
*** End Patch
`
	assertCategory(t, tool.Run(context.Background(), Args{PatchText: patch}), ErrCategoryPathEscape)
}

func TestSchemaInvokableAndRawJSON(t *testing.T) {
	t.Parallel()

	a := Schema()
	b := Schema()
	a[0] = ' '
	if b[0] == ' ' {
		t.Fatal("Schema returned aliased slices")
	}
	var schema struct {
		AdditionalProperties bool                       `json:"additionalProperties"`
		Properties           map[string]json.RawMessage `json:"properties"`
		Required             []string                   `json:"required"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatalf("schema parse: %v", err)
	}
	if schema.AdditionalProperties {
		t.Fatal("additionalProperties = true")
	}
	if _, ok := schema.Properties["patch_text"]; !ok {
		t.Fatal("patch_text property missing")
	}

	var nilTool *Tool
	if _, err := nilTool.InvokableRun(context.Background(), `{"patch_text":"a","patch_text":"b"}`); err == nil ||
		!strings.Contains(err.Error(), "duplicate top-level key") {
		t.Fatalf("duplicate key err = %v", err)
	}

	raw := []byte(`{"outcome":"succeeded","files":[],"future":"field"}`)
	var got Result
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Contains(got.RawJSON, []byte("future")) {
		t.Fatalf("RawJSON = %s", got.RawJSON)
	}
}

func assertCategory(t *testing.T, res Result, want string) {
	t.Helper()
	if res.Outcome != result.OutcomeFailed {
		t.Fatalf("Outcome = %q, want failed", res.Outcome)
	}
	if res.Error == nil || res.Error.Category != want {
		t.Fatalf("Error = %+v, want %s", res.Error, want)
	}
}
