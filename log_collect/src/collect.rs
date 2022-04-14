use std::collections::HashMap;
use std::fs::metadata;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::SystemTime;

use glob::glob;
use log::{debug, info, warn, error};
use tokio::time::Duration;

use crate::config::{Config, EventTimeConfig};
use std::sync::{Arc};
use std::borrow::BorrowMut;
use tokio::sync::Mutex;
use std::io::{Write, Error, SeekFrom};
use bytes::{BytesMut, BufMut};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use std::rc::Rc;
use crate::event::Event;


pub struct Collect {
    config: Arc<Config>,
    files: Vec<Arc<Mutex<FileInfo>>>,
}

impl Collect {
    pub fn new(config: Config) -> Collect {
        Collect {
            config: Arc::new(config),
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
                        delay_submit: false,
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
            let c = self.config.clone();
            let task = tokio::spawn(async move {
                f.lock().await.read(c).await;
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

    delay_submit: bool,
}

// 允许的最大的消息
const MAX_CAPACITY: usize = 102400;

impl FileInfo {
    async fn read(&mut self, config: Arc<Config>) {
        if self.last_write_time.lt(&self.read_time) {
            info!("ignore [{:?}] because last_write_time({:?}) < read_time({:?})", self.path, self.last_write_time, self.read_time);
            return;
        }
        info!("start read {:?}", self.path);

        match File::open(self.path.clone()).await {
            Ok(mut f) => {
                if self.read_pos > 0 {
                    f.seek(SeekFrom::Start(self.read_pos)).await;
                }
                let mut buf = BytesMut::with_capacity(MAX_CAPACITY);
                let mut pos = self.read_pos;

                let file_name = self.path.file_name()
                    .map_or(String::from("none"), |f| f.to_str().map_or(String::from("none"), |s| s.to_string()));

                let mut event = None;
                while let Ok(n) = f.read_buf(&mut buf).await {
                    if n == 0 {
                        break;
                    }

                    while let Some(line) = read_line(&mut buf) {
                        match event {
                            None => {
                                event = Some(Event {
                                    event_time: crate::event::parse_event_time_or_now(&line, &config.event_time),
                                    file_name: file_name.clone(),
                                    offset: 0,
                                    buf: line,
                                });
                            }
                            Some(mut e) => {
                                if let Some(timestamp) = crate::event::parse_event_time(&line, &config.event_time) {
                                    pos = pos + e.len();
                                    self.read_pos = pos;
                                    submit_event(e).await;
                                    event = Some(Event {
                                        event_time: timestamp,
                                        file_name: file_name.clone(),
                                        offset: pos,
                                        buf: line,
                                    });
                                } else {
                                    e.merge(line);
                                    event = Some(e)
                                }
                            }
                        }
                    }
                }
                if let Some(e) = event {
                    if self.delay_submit {
                        pos = pos + e.len();
                        self.read_pos = pos;
                        submit_event(e).await;
                        self.delay_submit = false;
                    } else {
                        self.delay_submit = true;
                    }
                } else {
                    self.delay_submit = false;
                }
            }
            Err(e) => { error!("open file failed,{:?}", e) }
        }


        info!("read finished :{:?}", self.path);
    }
}

const DEFAULT_POS: i64 = -1;

pub fn read_line(buf: &mut BytesMut) -> Option<BytesMut> {
    let mut pos = DEFAULT_POS;
    for (i, b) in buf.iter().enumerate() {
        if *b == b'\n' {
            pos = i as i64;
            break;
        }
    }
    if pos > DEFAULT_POS {
        Some(buf.split_to((pos + 1) as usize))
    } else if buf.len() >= MAX_CAPACITY {
        Some(buf.split_to(buf.len()))
    } else {
        None
    }
}

pub async fn submit_event(event: Event) {
    info!("submit:{:?}", event)
}