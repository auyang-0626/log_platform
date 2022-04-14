use std::time::SystemTime;
use bytes::BytesMut;
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
    pub fn force_parse(buf: BytesMut, file_name: String, offset: u64, config: &EventTimeConfig) -> Event {
        let event_time: i64 = match parse_event_time(&buf, config) {
            None => Local::now().timestamp_millis(),
            Some(v) => v,
        };
        Event {
            event_time,
            file_name,
            offset,
            buf,
        }
    }

    pub fn parse(buf: BytesMut, file_name: String, offset: u64, config: &EventTimeConfig) -> Option<Event> {
        match parse_event_time(&buf, config) {
            None => None,
            Some(v) => Some(Event {
                event_time: v,
                file_name,
                offset,
                buf,
            }),
        }
    }
    pub fn len(&self) -> u64 {
        self.buf.len() as u64
    }
}

pub fn parse_event_time(buf: &BytesMut, config: &EventTimeConfig) -> Option<i64> {
    let start = config.start_pos as usize;
    let end = (config.start_pos + config.len) as usize;
    if buf.len() > (config.start_pos + config.len) as usize {
        let v = buf[start..end].to_vec();
        if let Ok(s) = String::from_utf8(v) {
            if let Ok(dt) =Utc.datetime_from_str(&s, &config.fmt) {
                return Some(dt.timestamp_millis());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {

    use chrono::prelude::*;

    #[test]
    fn test_dt_parse(){
        println!("{:?}",Utc.datetime_from_str("2014-11-28 12:00:09", "%Y-%m-%d %H:%M:%S"))
    }
}