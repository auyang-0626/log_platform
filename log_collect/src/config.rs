use serde::{Deserialize, Serialize};

/// 配置文件对应的实体
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    // scan path,support glob grammar
    pub path: String,
    // 每次轮训完所有文件的休眠时间
    #[serde(default = "default_interval")]
    pub interval: u32,

    pub event_time: EventTimeConfig,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct EventTimeConfig {
    pub start_pos: u64,
    pub len: u64,
    pub fmt: String,
}

fn default_interval() -> u32 {
    10
}