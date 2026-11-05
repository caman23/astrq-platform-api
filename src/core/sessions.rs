//! Trading calendar and session detection (config-driven, Vietnam time).

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::Tz;
use std::collections::HashSet;

use crate::core::config::{SessionWindow, SessionsConfig};
use crate::core::models::Session;

#[derive(Debug, Clone)]
pub struct SessionCalendar {
    pub tz: Tz,
    pub enforce_trading_days: bool,
    holidays: HashSet<NaiveDate>,
    windows: Vec<SessionWindow>,
}

impl SessionCalendar {
    pub fn new(cfg: &SessionsConfig) -> Self {
        let mut windows = cfg.schedule.clone();
        windows.sort_by_key(|w| w.start);
        Self {
            tz: cfg.timezone.parse().unwrap_or(chrono_tz::Asia::Ho_Chi_Minh),
            enforce_trading_days: cfg.enforce_trading_days,
            holidays: cfg.holidays.iter().copied().collect(),
            windows,
        }
    }

    pub fn local(&self, ts: DateTime<Utc>) -> DateTime<Tz> {
        ts.with_timezone(&self.tz)
    }

    pub fn trading_date(&self, ts: DateTime<Utc>) -> NaiveDate {
        self.local(ts).date_naive()
    }

    pub fn is_trading_day(&self, d: NaiveDate) -> bool {
        if !self.enforce_trading_days {
            return true;
        }
        !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) && !self.holidays.contains(&d)
    }

    pub fn session_at(&self, ts: DateTime<Utc>) -> Session {
        let loc = self.local(ts);
        if !self.is_trading_day(loc.date_naive()) {
            return Session::Closed;
        }
        let t = loc.time();
        if let Some(first) = self.windows.first() {
            if t < first.start.0 {
                return Session::PreOpen;
            }
        }
        self.windows
            .iter()
            .find(|w| w.start.0 <= t && t < w.end.0)
            .map(|w| w.session)
            .unwrap_or(Session::Closed)
    }

    /// Combine a local date and time-of-day into a UTC instant.
    pub fn at_local(&self, d: NaiveDate, t: NaiveTime) -> DateTime<Utc> {
        self.tz
            .from_local_datetime(&d.and_time(t))
            .earliest()
            .expect("Vietnam has no DST gaps")
            .with_timezone(&Utc)
    }

    /// End of the last configured session window (e.g. 15:00).
    pub fn close_time(&self) -> NaiveTime {
        self.windows
            .iter()
            .map(|w| w.end.0)
            .max()
            .unwrap_or(NaiveTime::from_hms_opt(15, 0, 0).unwrap())
    }

    pub fn next_trading_day(&self, d: NaiveDate) -> NaiveDate {
        let mut n = d + Duration::days(1);
        while !self.is_trading_day(n) {
            n += Duration::days(1);
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vn(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        chrono_tz::Asia::Ho_Chi_Minh
            .with_ymd_and_hms(2026, 10, 21, h, m, s)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn session_detection() {
        let cal = SessionCalendar::new(&SessionsConfig::default());
        assert_eq!(cal.session_at(vn(8, 59, 59)), Session::PreOpen);
        assert_eq!(cal.session_at(vn(9, 0, 0)), Session::Ato);
        assert_eq!(cal.session_at(vn(9, 15, 0)), Session::ContinuousAm);
        assert_eq!(cal.session_at(vn(11, 30, 0)), Session::Lunch);
        assert_eq!(cal.session_at(vn(13, 0, 0)), Session::ContinuousPm);
        assert_eq!(cal.session_at(vn(14, 30, 0)), Session::Atc);
        assert_eq!(cal.session_at(vn(14, 50, 0)), Session::PutThrough);
        assert_eq!(cal.session_at(vn(15, 0, 0)), Session::Closed);
    }

    #[test]
    fn weekend_and_holiday() {
        let cfg = SessionsConfig {
            holidays: vec![NaiveDate::from_ymd_opt(2026, 10, 21).unwrap()],
            ..Default::default()
        };
        let cal = SessionCalendar::new(&cfg);
        assert_eq!(cal.session_at(vn(10, 0, 0)), Session::Closed);
        assert!(!cal.is_trading_day(NaiveDate::from_ymd_opt(2026, 10, 24).unwrap()));
        let relaxed = SessionCalendar::new(&SessionsConfig {
            enforce_trading_days: false,
            ..cfg
        });
        assert_eq!(relaxed.session_at(vn(10, 0, 0)), Session::ContinuousAm);
    }
}
