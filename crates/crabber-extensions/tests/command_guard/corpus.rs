use super::*;
use serde_json::Value;

fn outcome(s: &str) -> Outcome {
    match s {
        "abstain" => Outcome::Abstain,
        "rule-match" => Outcome::RuleMatch,
        "invalid-command" => Outcome::InvalidCommand,
        "unanalysable-command" => Outcome::Unanalysable,
        "analysis-limit" => Outcome::AnalysisLimit,
        _ => panic!("invalid fixture outcome: {s}"),
    }
}
#[test]
fn ported_reference_tests() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/ported-corpus.json")).unwrap();
    assert_eq!(rows.len(), 599);
    let mut failures = vec![];
    for row in rows {
        let mut o = options();
        o.limits.max_wrapper_depth = row["wrapper_depth"].as_u64().unwrap() as usize;
        o.rules = row["rules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| Rule {
                id: r["ID"].as_str().unwrap().into(),
                executable: r["Executable"].as_str().unwrap().into(),
                arg_prefix: r["ArgPrefix"]
                    .as_array()
                    .map(|v| v.iter().map(|s| s.as_str().unwrap().into()).collect())
                    .unwrap_or_default(),
            })
            .collect();
        let dialect = if row["dialect"] == "bash" {
            Dialect::Bash
        } else {
            Dialect::Posix
        };
        let script = row["script"].as_str().unwrap();
        let want = outcome(row["outcome"].as_str().unwrap());
        let got = Policy::new(o).unwrap().analyze_script(script, dialect);
        if got != want {
            failures.push(format!(
                "{dialect:?} {script:?}: {got:?}, expected {want:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn escaped_backslash_does_not_continue_a_line() {
    for s in [
        format!("echo {}\nblocked", "\\".repeat(2)),
        format!("echo ok {}\nblocked", "\\".repeat(2)),
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(policy().analyze_script(&s, d), Outcome::RuleMatch, "{s:?}");
        }
    }
}
#[test]
fn tabbed_heredoc_removes_tabs_inside_quotes() {
    let s = "cat <<-EOF\n$(\n'\n\tblocked'\n)\nEOF\n";
    // Tabs are removed even within quoted lines; the newline stays part of the
    // executable in this first control. A quote opened before the newline also
    // keeps that newline, so use a leading tab followed by the quote for the
    // executable check below.
    assert_eq!(policy().analyze_script(s, Dialect::Bash), Outcome::Abstain);
    let s = "cat <<-EOF\n$(\n\t'blocked'\n)\n\tEOF\n";
    assert_eq!(
        policy().analyze_script(s, Dialect::Bash),
        Outcome::RuleMatch
    );
}

#[test]
fn heredoc_continuations_fail_closed_and_quoted_bodies_stay_literal() {
    for s in [
        "cat <<EOF\nE\\\nOF\nblocked\nEOF\n",
        "cat <<EOF\n\\\nEOF\nblocked\nEOF\n",
        "cat <<EOF\nE\\\nO\\\nF\nblocked\nEOF\n",
        "cat <<-EOF\n\tE\\\nOF\nblocked\nEOF\n",
        "cat <<EOF\nfoo\\\nEOF\nblocked\nEOF\n",
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(
                policy().analyze_script(s, d),
                Outcome::Unanalysable,
                "{s:?}"
            );
        }
    }
    for s in [
        "cat <<'EOF'\nE\\\nOF\nblocked\nEOF\n",
        "cat <<-\"EOF\"\n\tE\\\nOF\nblocked\nEOF\n",
        "cat <<EOF\nE\\\\\nOF\nblocked\nEOF\n",
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(policy().analyze_script(s, d), Outcome::Abstain, "{s:?}");
        }
    }
}
