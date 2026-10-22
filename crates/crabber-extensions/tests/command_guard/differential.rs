use super::*;
use serde_json::Value;

const DEVIATION_KEYS: &[&str] = &[
    "redirect-target-fd",
    "posix-indexed-word",
    "parser",
    "raw-input",
    "bindings",
    "limits",
    "outcomes",
    "hash",
    "identifiers",
    "cr",
    "comment-continuation",
    "heredoc-in-substitution",
    "heredoc-line",
    "heredoc-continuation",
    "heredoc-delimiter",
    "brace-expansion",
    "posix-ansi-quote",
    "ansi-quote-backslash",
    "reserved-out-of-place",
    "posix-time-coproc",
    "posix-arith-subshell",
    "posix-extended-test",
    "posix-bracket-arith",
    "posix-append",
    "select",
    "fd-variable-redirect",
    "deny-class",
    "extglob",
    "cancellation",
    "guard-layer",
];
const KNOWN_STRICTER: &[(Dialect, &str, &str)] = &[
    (
        Dialect::Posix,
        "cat <<EOF\nfoo\\\nEOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Bash,
        "cat <<EOF\nfoo\\\nEOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Posix,
        "cat <<EOF\nE\\\nOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Bash,
        "cat <<EOF\nE\\\nOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Posix,
        "cat <<EOF\n\\\nEOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Bash,
        "cat <<EOF\n\\\nEOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Posix,
        "cat <<EOF\nE\\\nO\\\nF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Bash,
        "cat <<EOF\nE\\\nO\\\nF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Posix,
        "cat <<-EOF\n\tE\\\nOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (
        Dialect::Bash,
        "cat <<-EOF\n\tE\\\nOF\nblocked\nEOF\n",
        "heredoc-continuation",
    ),
    (Dialect::Posix, "sh[eval", "posix-indexed-word"),
    (Dialect::Posix, "printf[", "posix-indexed-word"),
    (Dialect::Posix, "2>&1>>statustime${x}", "redirect-target-fd"),
    (Dialect::Bash, "2>&1>>statustime${x}", "redirect-target-fd"),
    (
        Dialect::Bash,
        "-v\\1<A=b&2>&1<<<then\\\nfi2>&1\t",
        "redirect-target-fd",
    ),
    (
        Dialect::Posix,
        "esac!\"sudo$\"--echo>>2>&1-v",
        "redirect-target-fd",
    ),
    (
        Dialect::Bash,
        "esac!\"sudo$\"--echo>>2>&1-v",
        "redirect-target-fd",
    ),
    (Dialect::Bash, "2>&1casetest$x&>2>&1", "redirect-target-fd"),
    (
        Dialect::Posix,
        "echo a # c \\\nblocked",
        "comment-continuation",
    ),
    (
        Dialect::Bash,
        "echo a # c \\\nblocked",
        "comment-continuation",
    ),
    (
        Dialect::Posix,
        "cat <<EOF \"a\nb\"\nx\nEOF\n",
        "heredoc-line",
    ),
    (
        Dialect::Bash,
        "cat <<EOF \"a\nb\"\nx\nEOF\n",
        "heredoc-line",
    ),
    (
        Dialect::Posix,
        "cat <<EOF $(echo\n)\nx\nEOF\n",
        "heredoc-line",
    ),
    (
        Dialect::Bash,
        "cat <<EOF $(echo\n)\nx\nEOF\n",
        "heredoc-line",
    ),
    (
        Dialect::Posix,
        "echo $(cat <<EOF\nx\nEOF\n)",
        "heredoc-in-substitution",
    ),
    (
        Dialect::Bash,
        "echo $(cat <<EOF\nx\nEOF\n)",
        "heredoc-in-substitution",
    ),
    (Dialect::Posix, "((blocked))", "posix-arith-subshell"),
    (Dialect::Posix, "a;((x=$(blocked)))", "posix-arith-subshell"),
    (Dialect::Posix, "[[ -n x ]]; blocked", "posix-extended-test"),
    (Dialect::Posix, "echo $[x]", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "echo $[1+$(blocked)]",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "$'blocked'", "posix-ansi-quote"),
    (Dialect::Posix, "git $'push'", "posix-ansi-quote"),
    (
        Dialect::Posix,
        "echo $'\\''\nblocked\necho '",
        "ansi-quote-backslash",
    ),
    (
        Dialect::Posix,
        "echo $'\\'; blocked #'",
        "ansi-quote-backslash",
    ),
    (
        Dialect::Bash,
        "echo $'\\'; blocked #'",
        "ansi-quote-backslash",
    ),
    (Dialect::Posix, "! time blocked", "posix-time-coproc"),
    (Dialect::Posix, "a+=1 echo ok", "posix-append"),
    (Dialect::Bash, "echo {x}> f", "fd-variable-redirect"),
    (Dialect::Posix, "time blocked", "posix-time-coproc"),
    (Dialect::Posix, "time -p blocked", "posix-time-coproc"),
    (Dialect::Posix, "coproc blocked", "posix-time-coproc"),
    (Dialect::Bash, "select x in a; do blocked; done", "select"),
    (
        Dialect::Posix,
        "echo `cat <<EOF\nx\nEOF\n`",
        "heredoc-in-substitution",
    ),
    (
        Dialect::Bash,
        "echo `cat <<EOF\nx\nEOF\n`",
        "heredoc-in-substitution",
    ),
    (Dialect::Posix, "in", "reserved-out-of-place"),
    (Dialect::Bash, "in", "reserved-out-of-place"),
    (Dialect::Posix, "$[case--status", "posix-bracket-arith"),
    (Dialect::Posix, "[push$[", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "x$' !gitpush&>\"eval$(;;~env&>esac(>>-n$'",
        "posix-ansi-quote",
    ),
    (Dialect::Posix, "-p>$[testcommand!", "posix-bracket-arith"),
    (Dialect::Posix, "time", "posix-time-coproc"),
    (
        Dialect::Posix,
        "commanduntil$\"printfecho$\"git",
        "posix-ansi-quote",
    ),
    (Dialect::Posix, "do$[", "posix-bracket-arith"),
    (Dialect::Posix, ">>{>>bash$[*", "posix-bracket-arith"),
    (
        Dialect::Posix,
        ">>for\"||EOF#&&$x(($\"$[--",
        "posix-bracket-arith",
    ),
    (
        Dialect::Posix,
        "push]$'timeout<<<1|&-p-ntest$'",
        "posix-ansi-quote",
    ),
    (
        Dialect::Posix,
        "\rcase\r$[EOF$'$'echo-vdoneforcase",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "$[echo\\\n[", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "\" <<'EOF'command-p-e$[$\"",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "fi$[", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "2>&1{tooldone~dodonex#$[]\\",
        "posix-bracket-arith",
    ),
    (
        Dialect::Posix,
        "eval$\"push)sudo||;&\"forenvcommand$[A=b \rEOF\"$\"-p",
        "posix-bracket-arith",
    ),
    (
        Dialect::Posix,
        "sudo$[fiprintf2>&1x#",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "$[fipush", "posix-bracket-arith"),
    (Dialect::Posix, "--\t$[${x}", "posix-bracket-arith"),
    (Dialect::Posix, "\\\nexec$''", "posix-ansi-quote"),
    (
        Dialect::Posix,
        "$[1test---pfor;commandA=b",
        "posix-bracket-arith",
    ),
    (
        Dialect::Posix,
        "gitesac<$[EOF||statusesacstatusfiiftimeout~if&&-n-n",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "-e$[do||if1", "posix-bracket-arith"),
    (Dialect::Posix, "eval$[eval2>&1", "posix-bracket-arith"),
    (Dialect::Posix, "$[", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "incasefor\r-p>1$[timetimeout-ethendo",
        "posix-bracket-arith",
    ),
    (
        Dialect::Posix,
        "thentimeout-e!timedogit$[",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "$[untilcase\n", "posix-bracket-arith"),
    (Dialect::Posix, "$[\r!", "posix-bracket-arith"),
    (Dialect::Posix, "in$[-v", "posix-bracket-arith"),
    (Dialect::Posix, "]done[[do|env\t$[#", "posix-bracket-arith"),
    (Dialect::Posix, ">$x$[", "posix-bracket-arith"),
    (
        Dialect::Posix,
        "!$[}command;until2>&1&&2>&1donewhilewhile\r'timedo@(timeoutwhile-c'",
        "posix-bracket-arith",
    ),
    (Dialect::Posix, "echo$[execfor", "posix-bracket-arith"),
    (Dialect::Posix, "1&in", "reserved-out-of-place"),
    (Dialect::Bash, "1&in", "reserved-out-of-place"),
    (
        Dialect::Posix,
        "time;commandA=b*sudogitblocked",
        "posix-time-coproc",
    ),
    (
        Dialect::Posix,
        "timeouteval||$\"!>casewhileechofi@( -c-n\"\n\\\n>exec\rbash$x*",
        "posix-ansi-quote",
    ),
];

#[test]
fn every_reference_row_is_matched_or_stricter() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/go-reference-corpus.json")).unwrap();
    assert_eq!(
        fixture["reference"],
        "eino-agent-extensions@5389549b1f156013a0f82bc936ce4f10edb1ce9f"
    );
    let inputs: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/corpus-input.json")).unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(inputs.len(), 10888);
    assert_eq!(rows.len(), inputs.len());
    for (input, row) in inputs.iter().zip(rows) {
        assert_eq!(input["dialect"], row["dialect"]);
        assert_eq!(input["script"], row["script"]);
    }
    assert_eq!(fixture["parser"], "mvdan.cc/sh/v3@v3.14.1");
    assert_eq!(
        fixture["rules"],
        serde_json::json!([
            {"id":"blocked","executable":"blocked","arg_prefix":[]},
            {"id":"git-push","executable":"git","arg_prefix":["push"]}
        ])
    );
    assert_eq!(fixture["limits"]["MaxCommandBytes"], 4096);
    assert_eq!(fixture["limits"]["MaxAnalysisBytes"], 8192);
    for (d, script, key) in KNOWN_STRICTER {
        assert!(DEVIATION_KEYS.contains(key));
        let dialect = if *d == Dialect::Bash { "bash" } else { "posix" };
        assert!(
            fixture["rows"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["dialect"] == dialect
                    && r["script"] == *script
                    && matches!(r["outcome"].as_str(), Some("abstain" | "rule-match"))),
            "stale stricter entry"
        );
    }
    let p = policy();
    let mut failures = vec![];
    for row in fixture["rows"].as_array().unwrap() {
        let d = if row["dialect"] == "bash" {
            Dialect::Bash
        } else {
            Dialect::Posix
        };
        let script = row["script"].as_str().unwrap();
        let got = p.analyze_script(script, d);
        let reference = row["outcome"].as_str().unwrap();
        let deviation = KNOWN_STRICTER
            .iter()
            .find(|(dialect, text, _)| *dialect == d && *text == script)
            .map(|(_, _, key)| *key);
        let pass = if deviation == Some("comment-continuation") {
            got == Outcome::RuleMatch
        } else if deviation.is_some() {
            matches!(got, Outcome::Unanalysable | Outcome::InvalidCommand)
        } else {
            match reference {
                "abstain" => got == Outcome::Abstain,
                "rule-match" => got == Outcome::RuleMatch,
                "invalid-command" | "unanalysable-command" => got.denies(),
                "analysis-limit" => {
                    if script.len() > p.limits().max_command_bytes {
                        got == Outcome::AnalysisLimit
                    } else {
                        got.denies()
                    }
                }
                _ => panic!("invalid fixture outcome"),
            }
        };
        if !pass {
            failures.push(format!("{d:?} {script:?}: {got:?}, reference {reference}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
