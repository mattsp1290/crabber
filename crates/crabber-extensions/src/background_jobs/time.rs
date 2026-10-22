use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn rfc3339_utc(time: SystemTime) -> String {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    };
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let days = seconds.div_euclid(86400);
    let daytime = seconds.rem_euclid(86400);
    // Gregorian civil date from days since Unix epoch (400-year eras).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{fraction:09}Z",
        daytime / 3600,
        daytime / 60 % 60,
        daytime % 60
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn rfc3339_utc_pins() {
        assert_eq!(rfc3339_utc(UNIX_EPOCH), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH + Duration::from_secs(951782400)),
            "2000-02-29T00:00:00.000000000Z"
        );
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH + Duration::new(2147483647, 1)),
            "2038-01-19T03:14:07.000000001Z"
        );
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH - Duration::from_nanos(1)),
            "1969-12-31T23:59:59.999999999Z"
        );
    }
}
