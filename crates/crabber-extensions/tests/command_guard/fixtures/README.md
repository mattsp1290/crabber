# Reproducing the command analysis evidence

Reference: `eino-agent-extensions@5389549b1f156013a0f82bc936ce4f10edb1ce9f`.
Parser: `mvdan.cc/sh/v3@v3.14.1` (BSD-3-Clause). Rust adds no dependency.
Generated on 2026-10-06 with Go 1.26; generation left the reference checkout
clean. The Rust tests require neither Go nor the reference checkout.

`corpus-input.json` is the immutable input list for the outcome pin and Go
fixture: both dialects for the reference matrices and the plan's adversarial
scripts, followed by 5,000 seeded generated scripts in both dialects. The
last 10,000 rows are checked against the shared Rust generator, using xorshift64
seed `0x5eed_c0de`, 1–24 concatenated tokens from its closed alphabet. The
execution canaries use seed `0xca7a_7e11` (the plan's `0xca7a_ry` was not a
valid hexadecimal literal). `adversarial.json` holds the hand-picked scripts.

`ported-corpus.json` contains all 599 expanded invocations from these reference
tests, including rule overrides and wrapper-depth settings:
`TestCommandCorpus`, `TestBashGrammar`, `TestExactWordDecoding`,
`TestOpaqueBuiltinInventoryDirectAndWrapped`, `TestWrapperAcceptedForms`,
`TestWrapperUnsupportedForms`, `TestNestedShellGrammar`,
`TestShellSpecialVariableTargets`, `TestExtendedGlobsAreOpaque`.
The selected original tests also passed during capture. Rust replays these
with the exact expected outcome classes. The production deny rules in the
main differential fixture remain `blocked` and `git push`.

## Main fixture generation

Resolve `COMMAND_GUARD_REFERENCE_DIR`, or use the repository's sibling
`../eino-agent-extensions`. Verify `git rev-parse HEAD` equals the reference
SHA above. Use a scratch directory outside both repositories. Write the Go
source below to `zz_corpus_dump_test.go` in that directory, and write an
`overlay.json` with this shape (absolute paths):

```json
{"Replace":{"<reference>/commandguard/zz_corpus_dump_test.go":"<scratch>/zz_corpus_dump_test.go"}}
```

Run from the reference root, with Go 1.26 or newer:

```sh
COMMAND_GUARD_CORPUS_IN=<absolute-path>/corpus-input.json \
COMMAND_GUARD_CORPUS_OUT=<scratch>/reference-rows.json \
go test -overlay <scratch>/overlay.json ./commandguard -run '^TestDumpCorpus$' -count=1
```

Replace only `rows` in `go-reference-corpus.json` with the generated JSON
array; retain its reference/parser/rules/limits header. Confirm
`git -C <reference> status --short` is empty. Run the differential test and
investigate every unexpected result before updating any deviation entry.

```go
package commandguard
import ("context"; "encoding/json"; "os"; "testing")
func dumpOutcome(o outcome) string {
 switch o { case abstain:return "abstain"; case ruleMatch:return "rule-match"; case invalidCommand:return "invalid-command"; case unanalysable:return "unanalysable-command"; case analysisLimit:return "analysis-limit" }; return "unexpected"
}
func TestDumpCorpus(t *testing.T) {
 in,out:=os.Getenv("COMMAND_GUARD_CORPUS_IN"),os.Getenv("COMMAND_GUARD_CORPUS_OUT")
 if in==""||out=="" {t.Skip("corpus paths unset")}
 data,err:=os.ReadFile(in); if err!=nil {t.Fatal(err)}
 var rows []struct {Dialect Dialect `json:"dialect"`; Script string `json:"script"`; Outcome string `json:"outcome,omitempty"`}
 if err=json.Unmarshal(data,&rows);err!=nil{t.Fatal(err)}
 p,err:=canonicalize(testOptions());if err!=nil{t.Fatal(err)}
 for i:=range rows {a:=analysis{ctx:context.Background(),policy:&p,parse:parseScript}; rows[i].Outcome=dumpOutcome(a.script(rows[i].Script,rows[i].Dialect,0,0))}
 data,err=json.MarshalIndent(rows,"","  ");if err!=nil{t.Fatal(err)}
 if err=os.WriteFile(out,data,0600);err!=nil{t.Fatal(err)}
}
```

## Reference test matrix capture

To recreate `ported-corpus.json`, copy `commandguard/parser_test.go` into the
scratch directory, add `os` to its imports, and replace the final
`return a.script(s, d, 0, 0)` in its `analyze` helper with:

```go
result := a.script(s, d, 0, 0)
if path := os.Getenv("COMMAND_GUARD_CAPTURE"); path != "" {
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil { t.Fatal(err) }
    row := struct {
        Script string `json:"script"`
        Dialect Dialect `json:"dialect"`
        Rules []Rule `json:"rules"`
        WrapperDepth int `json:"wrapper_depth"`
        Outcome string `json:"outcome"`
    }{s, d, o.Rules, o.Limits.MaxWrapperDepth, dumpOutcome(result)}
    if err := json.NewEncoder(f).Encode(row); err != nil { t.Fatal(err) }
    f.Close()
}
return result
```

Add the copied `parser_test.go` to the overlay's Replace map alongside the
Go generator above (which supplies `dumpOutcome`). Set
`COMMAND_GUARD_CAPTURE=<scratch>/ported.jsonl` to a new empty file, then run
`go test -overlay <scratch>/overlay.json ./commandguard -run` with an anchored
alternation of the nine test names listed above and `-count=1`.
Read each JSON line and serialize their array to `ported-corpus.json`.
The original tests must pass; they establish the captured expected classes.

JSON extraction additionally bounds aggregate key/string bytes by
`max_analysis_bytes` before NUL scans, using a separate extraction budget.
A node-only walk would otherwise scan a single arbitrarily large sibling string;
this conservative `json-bytes` boundary is covered by input tests.

## Evidence and deliberate differences

The Rust fixture policy uses a depth limit of 16 within the measured hard cap
32. Go uses its original 64. Syntax-depth accounting differs, so non-byte
reference analysis-limit cases require a Rust denial, rather than the exact
class. All other non-deviation abstentions and rule matches agree exactly.
`KNOWN_STRICTER` entries include an exact dialect/script and a documented
`DEVIATION_KEYS` key; stale or unrecognized entries fail the test.

Real-shell syntax checks found reference abstentions on numeric redirect
targets immediately followed by another redirect operator, and POSIX
executable words like `sh[eval` that Bash-as-sh interprets as incomplete
array syntax. These are denied under `redirect-target-fd` and
`posix-indexed-word`. Comments ending in backslash still end at the newline:
Rust finds `blocked` where the reference abstains, so this deviation expects
`RuleMatch`, rather than weakening it to an opaque denial.

The parser marks any unquoted literal part containing `{` unknown, including
unmatched or escaped braces: inspection of the pinned `SplitBraces` source
showed it returns true for any such part, even when no valid expansion exists.
The plan's narrower description of that function was inaccurate. Quoted
braces remain known. CR remains a word byte. Tabs in `<<-` bodies are stripped
before parsing, including within quotes, using the same parser cursor.

The Unix suites discover Bash 5+ and `/bin/sh`; absent shells are reported as
skips unless `COMMAND_GUARD_REQUIRE_SHELLS=1`. Every execution uses an empty
environment, a temporary workspace, synthetic guarded executables and wrapper
stubs, and a two-second kill/reap watchdog. Synthetic `blocked` and `git push`
write bounded marker files and exit unsuccessfully; no provider credentials
are used. Implicit PS4 proofs use Bash 5 under the name `sh`: this machine's
dash does not expand an inherited PS4's external command substitution.
Syntax scripts are passed after `-c --`, protecting leading-option command
text from interpretation as shell options. Shell tests are empirical evidence
for this corpus, not a universal proof of shell compatibility.

Unquoted heredoc continuations are conservatively unanalysable: Bash and dash
disagree on closing delimiters assembled across lines. The parser-v2 identity
records this boundary and delimiter/for-target word-byte checks. The reference
fixtures retain exact results with individually justified stricter entries;
quoted heredoc bodies remain data, verified with synthetic shell canaries.
