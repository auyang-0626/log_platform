use std::collections::HashMap;
use std::fs::metadata;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::SystemTime;

use glob::glob;
use log::{debug, info, warn};
use tokio::time::Duration;

use crate::config::Config;
use std::sync::Arc;
use std::borrow::BorrowMut;


pub struct Collect {
    config: Config,
    files: Vec<Arc<FileInfo>>,
}

impl Collect {
    pub fn new(config: Config) -> Collect {
        Collect {
            config,
            files: Vec::new(),
        }
    }

    pub async fn start(&mut self) {
        info!("collect will start....");
        loop {
            let paths = self.scan_file();
            self.read_files(paths).await;
            // when read files finished, sleep specify sec，avoid cpu waste
            tokio::time::sleep(Duration::from_secs(self.config.interval as u64)).await;
        }
    }

    async fn read_files(&mut self, paths: Vec<PathBuf>) {
        let mut files = Vec::new();

        // inode --> FileInfo, convenient to determine whether it already exists
        let mut file_map = HashMap::new();
        self.files.iter().map(|f| file_map.insert(f.inode, f));

        for path in paths {
            if let Ok(m) = metadata(path.clone()) {
                let last_write_time = m.modified().unwrap_or(SystemTime::now());

                let file_info = match file_map.get(&m.ino()) {
                    None => FileInfo {
                        path,
                        inode: m.ino(),
                        last_write_time,
                        read_pos: 0,
                        read_time: SystemTime::UNIX_EPOCH,
                    },
                    Some(f) => FileInfo {
                        path,
                        inode: m.ino(),
                        last_write_time,
                        read_pos: f.read_pos,
                        read_time: f.read_time,
                    }
                };

                files.push(Arc::new(file_info));
            }
        }

        self.files = files;

        debug!("will read files:{:?}", self.files);

        // parallel read file task
        let mut tasks = Vec::new();
        for file in &self.files {
            let f = file.clone();
            let task = tokio::spawn(async move {
                f.read().await
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await;
        }
    }

    fn scan_file(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();

        for entry in glob(&self.config.path).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => {
                    debug!("find file {:?}", path);
                    paths.push(path);
                }
                Err(e) => warn!("scan file error! {:?}", e),
            }
        }

        paths
    }
}

#[derive(Debug)]
struct FileInfo {
    path: PathBuf,
    inode: u64,

    last_write_time: SystemTime,
    read_pos: u64,
    read_time: SystemTime,
}

impl FileInfo {
    async fn read(&mut self) {
        info!("start read {:?}", self.path);
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("read finished :{:?}",self.path);

    }
}