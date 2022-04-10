use log::LevelFilter;

pub fn init(){
    env_logger::builder().filter_level(LevelFilter::Debug).init();
}