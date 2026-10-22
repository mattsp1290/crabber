use super::*;
fn limits() -> super::super::Limits {
    super::super::Limits {
        max_bindings: 8,
        max_rules: 32,
        max_rule_bytes: 2048,
        max_prefix_args: 16,
        max_json_depth: 16,
        max_json_nodes: 256,
        max_command_bytes: 4096,
        max_analysis_bytes: 8192,
        max_ast_nodes: 2048,
        max_ast_depth: 16,
        max_words: 512,
        max_word_bytes: 4096,
        max_wrapper_depth: 8,
        max_in_flight: 4,
    }
}
#[test]
fn grammar_productions() {
    let l = limits();
    for script in [
        "",
        "# comment",
        "echo ok",
        "A=b >f echo x",
        "! echo x | cat && tool || tool &",
        "(echo x)",
        "{ echo x; }",
        "if true; then echo x; elif false; then tool; else tool; fi",
        "while true; do tool; done",
        "until false; do tool; done",
        "for x in a b; do echo $x; done",
        "for x; do echo $x; done",
        "case x in (x|y) tool;; z) tool;& esac",
        "echo $(case x in x) tool;; esac)",
        "echo <(tool)",
        "cat <<EOF\n$(tool)\nEOF\n",
        "echo a # c \\\nblocked",
        "echo \"$(echo ')')\"",
        "echo `echo \\`tool\\``",
    ] {
        let mut b = Budget::new(&l);
        assert!(
            parse(script, Dialect::Bash, Context::Top, 0, &mut b).is_ok(),
            "{script:?}"
        );
    }
}
#[test]
fn error_classes() {
    let l = limits();
    for (script, expected) in [
        ("echo \"", Outcome::InvalidCommand),
        ("echo |", Outcome::InvalidCommand),
        ("(echo", Outcome::InvalidCommand),
        ("if true; echo x; fi", Outcome::InvalidCommand),
        ("$((x))", Outcome::Unanalysable),
        ("${x:-y}", Outcome::Unanalysable),
        ("function f { echo x; }", Outcome::Unanalysable),
        ("f() { echo x; }", Outcome::Unanalysable),
        ("A=(x)", Outcome::Unanalysable),
        ("! ! tool", Outcome::InvalidCommand),
        ("time tool", Outcome::Unanalysable),
        ("echo $(cat <<EOF\nx\nEOF\n)", Outcome::Unanalysable),
    ] {
        let mut b = Budget::new(&l);
        assert_eq!(
            parse(script, Dialect::Bash, Context::Top, 0, &mut b).err(),
            Some(expected),
            "{script:?}"
        );
    }
}
#[test]
fn ast_preserves_branches_and_quote_structure() {
    let l = limits();
    let mut b = Budget::new(&l);
    let ast = parse(
        "if echo $(tool); then git 'push'; else echo \"$x\"; fi",
        Dialect::Posix,
        Context::Top,
        0,
        &mut b,
    )
    .unwrap();
    let command = &ast.items[0].and_or.first.commands[0];
    let CommandKind::If { arms, else_ } = &command.kind else {
        panic!("expected if");
    };
    assert_eq!(arms.len(), 1);
    assert!(else_.is_some());
    let CommandKind::Simple { words, .. } = &arms[0].0.items[0].and_or.first.commands[0].kind
    else {
        panic!("expected simple");
    };
    assert!(matches!(words[1].parts[0], WordPart::CmdSubst(_)));
}
