use serde::{Deserialize,Serialize};

/// 配置文件对应的实体
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config{

    // scan path,support glob grammar
    pub path:String,
    // 每次轮训完所有文件的休眠时间
    #[serde(default = "default_interval")]
    pub interval:u32,
}

fn default_interval() -> u32{
    10
}