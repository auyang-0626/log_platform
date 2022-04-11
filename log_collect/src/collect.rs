use std::collections::HashMap;
use std::fs::metadata;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::SystemTime;

use glob::glob;
use log::{debug, info, warn, error};
use tokio::time::Duration;

use crate::config::Config;
use std::sync::{Arc};
use std::borrow::BorrowMut;
use tokio::sync::Mutex;
use std::io::{Write, Error, SeekFrom};
use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use std::rc::Rc;


pub struct Collect {
    config: Config,
    files: Vec<Arc<Mutex<FileInfo>>>,
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
        let mut files: Vec<Arc<Mutex<FileInfo>>> = Vec::new();

        // inode --> FileInfo, convenient to determine whether it already exists
        let mut file_map = HashMap::new();
        for mf in &self.files {
            let f = mf.lock().await;
            file_map.insert(f.inode, mf.clone());
        }

        for path in paths {
            if let Ok(m) = metadata(path.clone()) {
                let last_write_time = m.modified().unwrap_or(SystemTime::now());

                let file_info = match file_map.get(&m.ino()) {
                    None => Arc::new(Mutex::new(FileInfo {
                        path,
                        inode: m.ino(),
                        last_write_time,
                        read_pos: 0,
                        read_time: SystemTime::UNIX_EPOCH,
                    })),
                    Some(f) => {
                        let mut mf = f.lock().await;
                        // update path, avoid rename
                        mf.path = path;
                        f.clone()
                    }
                };

                files.push(file_info);
            }
        }

        self.files = files;

        debug!("will read files:{:?}", self.files);

        // parallel read file task
        let mut tasks = Vec::new();
        for file in &self.files {
            let f = file.clone();
            let task = tokio::spawn(async move {
                f.lock().await.read().await;
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
pub struct FileInfo {
    path: PathBuf,
    inode: u64,

    last_write_time: SystemTime,
    read_pos: u64,
    read_time: SystemTime,

    // buf: Option<BytesMut>,
}

// 允许的最大的消息
const MAX_CAPACITY: usize = 1024;

impl FileInfo {
    async fn read(&mut self) {
        if self.last_write_time.lt(&self.read_time) {
            info!("ignore [{:?}] because last_write_time({:?}) < read_time({:?})", self.path, self.last_write_time, self.read_time);

            // if self.buf.is_some() {
            //     if let Ok(s) = self.last_write_time.elapsed() {
            //         // file idle ge 60, free buf
            //         if s.as_secs() > 60 {
            //             self.buf = None;
            //         }
            //     }
            // }
            return;
        }
        info!("start read {:?}", self.path);

        match File::open(self.path.clone()).await {
            Ok(mut f) => {
                if self.read_pos > 0 {
                    f.seek(SeekFrom::Start(self.read_pos)).await;
                }
                let mut buf = BytesMut::with_capacity(MAX_CAPACITY);

                if let Ok(n) = f.read_buf(&mut buf).await {
                    info!("read size :{},capacity:{}", n, buf.capacity());
                }
            }
            Err(e) => { error!("open file failed,{:?}", e) }
        }


        info!("read finished :{:?}", self.path);
    }
}

pub fn read_line(buf: &BytesMut) -> Option<String> {
    for (i, b) in buf.iter().enumerate() {
        if *b == b'\n' {
            // buf.split()
        }
    }
    None
}