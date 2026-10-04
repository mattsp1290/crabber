use std::io::{Read, Write};

pub const MODE_ENV: &str = "CRABBER_RESULT_TRANSFORM_CHILD";
pub const READY: &str = "crabber reduction ready";
pub const TEST_NAME: &str = "result_transform_support::child::reduction_child";

#[test]
fn reduction_child() {
    if std::env::var(MODE_ENV).as_deref() != Ok("reduce") {
        return;
    }
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{READY}").unwrap();
    stdout.flush().unwrap();
    drop(stdout);
    let mut buffer = [0; 256];
    while std::io::stdin().read(&mut buffer).unwrap() != 0 {}
}
