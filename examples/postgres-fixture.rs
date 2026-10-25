//! Optional identity evidence when a demo is launched by the fixture runner.

use sqlx::Connection;

pub async fn verify(url: &str) {
    let Ok(expected) = std::env::var("CRABBER_TEST_POSTGRES_CHILD_DATABASE") else {
        return;
    };
    let parts: Vec<_> = expected.split('_').collect();
    assert!(
        parts.len() == 3
            && parts[0] == "ct"
            && parts[1].len() == 32
            && parts[2].len() == 16
            && parts[1..].iter().all(|part| part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))),
        "invalid opaque demo database identity"
    );
    let mut connection = sqlx::PgConnection::connect(url)
        .await
        .unwrap_or_else(|_| panic!("cannot connect to demo database for identity evidence"));
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap_or_else(|_| panic!("cannot inspect demo database identity"));
    connection
        .close()
        .await
        .unwrap_or_else(|_| panic!("cannot close demo identity connection"));
    assert!(actual == expected, "demo attached to a different database");
    println!("attached PostgreSQL demo database={expected}");
}
