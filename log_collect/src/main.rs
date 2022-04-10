mod init;
mod config;
mod collect;

extern crate serde;

use log::{info, warn,debug};
use crate::config::Config;
use crate::collect::Collect;


#[tokio::main]
async fn main() {
    init::init();

    let file = std::fs::File::open("/home/yang/CLionProjects/log_platform/log_collect/config.yaml")
        .expect("open config failed,please check path!");

    let config: Config = serde_yaml::from_reader(file)
        .expect("parse config failed,please check!");

    debug!("config:{:?}",config);

    Collect::new(config).start().await;

}
