//! Pending I/O 管理器
//!
//! 目标：为 `Status::Pending + complete_pending()` 提供一个最小可用的真实 I/O 推进机制。
//! 当前阶段主要用于“读盘完成 -> 允许上层重试 read() 成功”的链路。

use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::thread;

use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;

use crate::address::Address;
use crate::device::StorageDevice;

#[derive(Debug)]
pub(crate) enum IoRequest {
    ReadBytes {
        thread_id: usize,
        thread_tag: u64,
        address: Address,
        len: usize,
    },
    Shutdown,
}

#[derive(Debug)]
pub(crate) enum IoCompletion {
    ReadBytesDone {
        thread_id: usize,
        thread_tag: u64,
        address: Address,
        result: io::Result<Vec<u8>>,
    },
}

/// Pending I/O 管理器（后台线程执行 async I/O，并通过 channel 回传完成事件）
pub(crate) struct PendingIoManager<D: StorageDevice> {
    tx: Sender<IoRequest>,
    rx: Receiver<IoCompletion>,
    /// 防止重复提交同一地址的读请求（粒度：Address）
    inflight_reads: Mutex<HashSet<u64>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    _marker: std::marker::PhantomData<D>,
}

impl<D: StorageDevice> PendingIoManager<D> {
    pub(crate) fn new(device: Arc<D>) -> Self {
        let (tx, req_rx) = unbounded::<IoRequest>();
        let (comp_tx, rx) = unbounded::<IoCompletion>();

        let worker = thread::spawn(move || {
            // 独立 runtime：不依赖调用方是否运行在 tokio runtime 内
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = comp_tx.send(IoCompletion::ReadBytesDone {
                        thread_id: 0,
                        thread_tag: 0,
                        address: Address::INVALID,
                        result: Err(io::Error::other(e.to_string())),
                    });
                    return;
                }
            };

            while let Ok(req) = req_rx.recv() {
                match req {
                    IoRequest::ReadBytes {
                        thread_id,
                        thread_tag,
                        address,
                        len,
                    } => {
                        let mut buf = vec![0u8; len];
                        let result = rt.block_on(async {
                            let n = device.read(address.control(), &mut buf).await?;
                            if n != len {
                                return Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    format!("short read: expected {len}, got {n}"),
                                ));
                            }
                            Ok(buf)
                        });

                        let _ = comp_tx.send(IoCompletion::ReadBytesDone {
                            thread_id,
                            thread_tag,
                            address,
                            result,
                        });
                    }
                    IoRequest::Shutdown => break,
                }
            }
        });

        Self {
            tx,
            rx,
            inflight_reads: Mutex::new(HashSet::new()),
            worker: Mutex::new(Some(worker)),
            _marker: std::marker::PhantomData,
        }
    }

    /// 提交一次读请求（若该地址已在 inflight，则不重复提交）
    ///
    /// 返回值：
    /// - `true`：本次成功提交（调用方应增加 pending 计数）
    /// - `false`：已存在 inflight（调用方不应增加 pending 计数）
    pub(crate) fn submit_read_bytes(
        &self,
        thread_id: usize,
        thread_tag: u64,
        address: Address,
        len: usize,
    ) -> bool {
        let mut inflight = self.inflight_reads.lock();
        if !inflight.insert(address.control()) {
            return false;
        }
        drop(inflight);

        // 如果发送失败，说明 worker 已退出；视作未提交（允许上层重试再次提交）
        if self
            .tx
            .send(IoRequest::ReadBytes {
                thread_id,
                thread_tag,
                address,
                len,
            })
            .is_err()
        {
            let mut inflight = self.inflight_reads.lock();
            inflight.remove(&address.control());
            return false;
        }

        true
    }

    /// 尝试拉取一批完成事件（非阻塞）
    pub(crate) fn drain_completions(&self) -> Vec<IoCompletion> {
        let mut out = Vec::new();
        while let Ok(c) = self.rx.try_recv() {
            // 读完成（无论成功/失败）都认为 inflight 结束，允许重试再次提交
            let IoCompletion::ReadBytesDone { address, .. } = &c;
            let mut inflight = self.inflight_reads.lock();
            inflight.remove(&address.control());
            out.push(c);
        }
        out
    }
}

impl<D: StorageDevice> Drop for PendingIoManager<D> {
    fn drop(&mut self) {
        let _ = self.tx.send(IoRequest::Shutdown);
        if let Some(worker) = self.worker.lock().take() {
            let _ = worker.join();
        }
    }
}
