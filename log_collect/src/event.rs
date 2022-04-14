use std::time::SystemTime;
use bytes::{BytesMut, BufMut};
use crate::config::EventTimeConfig;
use chrono::{DateTime, Local};
use chrono::prelude::*;
use std::error;
use log::info;

#[derive(Debug)]
pub struct Event {
    pub event_time: i64,
    pub file_name: String,
    pub offset: u64,
    pub buf: BytesMut,
}

impl Event {
    pub fn len(&self) -> u64 {
        self.buf.len() as u64
    }
    pub fn merge(&mut self, line: BytesMut) {
        self.buf.put(line)
    }
}

pub fn parse_event_time(buf: &BytesMut, config: &EventTimeConfig) -> Option<i64> {
    let start = config.start_pos as usize;
    let end = (config.start_pos + config.len) as usize;
    if buf.len() > (config.start_pos + config.len) as usize {
        let v = buf[start..end].to_vec();
        if let Ok(s) = String::from_utf8(v) {
            if let Ok(dt) = Utc.datetime_from_str(&s, &config.fmt) {
                return Some(dt.timestamp_millis());
            }
        }
    }
    None
}

pub fn parse_event_time_or_now(buf: &BytesMut, config: &EventTimeConfig) -> i64 {
    parse_event_time(buf, config)
        .map_or(Local::now().timestamp_millis(), |v| v)
}

#[cfg(test)]
mod tests {
    use chrono::prelude::*;

    #[test]
    fn test_dt_parse() {
        println!("{:?}", Utc.datetime_from_str("2014-11-28 12:00:09", "%Y-%m-%d %H:%M:%S"))
    }
}