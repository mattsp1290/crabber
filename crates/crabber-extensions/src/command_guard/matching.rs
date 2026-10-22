use super::{Outcome, builtins, walk::Analysis, words::Word};
impl Analysis<'_> {
    pub(super) fn command(&mut self, words: &[Word], depth: usize, wrappers: usize) -> Outcome {
        let Some((exe, args)) = words.split_first() else {
            return Outcome::Unanalysable;
        };
        if !exe.known || exe.text.is_empty() || exe.text.ends_with('/') {
            return Outcome::Unanalysable;
        }
        let name = exe.text.rsplit('/').next().unwrap_or("");
        if let Some(indexes) = self.policy.by_basename.get(name) {
            for index in indexes {
                let Some(rule) = self.policy.rules.get(*index) else {
                    return Outcome::Unanalysable;
                };
                let mut matches = true;
                for (i, want) in rule.arg_prefix.iter().enumerate() {
                    let Some(arg) = args.get(i) else {
                        matches = false;
                        break;
                    };
                    if !arg.known {
                        return Outcome::Unanalysable;
                    }
                    if arg.text != *want {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    return Outcome::RuleMatch;
                }
            }
        }
        if let Err(outcome) = builtins::check(name, args) {
            return outcome;
        }
        self.wrapper(name, args, depth, wrappers)
    }
}
