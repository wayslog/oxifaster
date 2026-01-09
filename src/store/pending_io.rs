//! Pending I/O manager
//!
//! Goal: provide a minimal, real I/O progress mechanism for
//! `Status::Pending + complete_pending()`.
//! At the moment this is mainly used for the "disk read completes -> caller retries read()" path.

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

/// Pending I/O manager (runs async I/O in a background thread and reports completions via channels).
pub(crate) struct PendingIoManager<D: StorageDevice> {
    tx: Sender<IoRequest>,
    rx: Receiver<IoCompletion>,
    /// Prevent submitting duplicate reads for the same address.
    inflight_reads: Mutex<HashSet<u64>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    _marker: std::marker::PhantomData<D>,
}

impl<D: StorageDevice> PendingIoManager<D> {
    pub(crate) fn new(device: Arc<D>) -> Self {
        let (tx, req_rx) = unbounded::<IoRequest>();
        let (comp_tx, rx) = unbounded::<IoCompletion>();

        let worker = thread::spawn(move || {
            // Dedicated runtime: do not depend on the caller being inside a Tokio runtime.
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

    /// Submit a read request.
    ///
    /// If the address is already inflight, this is a no-op.
    ///
    /// Returns:
    /// - `true`: the request was newly submitted (caller should increment pending count)
    /// - `false`: a request is already inflight (caller should not increment pending count)
    pub(crate) fn submit_read_bytes(
        &self,
        thread_id: usize,
        thread_tag: u64,
        address: Address,
        len: usize,
    ) -> bool {
        {
            let mut inflight = self.inflight_reads.lock();
            if !inflight.insert(address.control()) {
                return false;
            }
        }

        // If send fails, the worker has exited; treat as "not submitted" and allow retry.
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
            self.inflight_reads.lock().remove(&address.control());
            return false;
        }

        true
    }

    /// Drain a batch of completion events (non-blocking).
    pub(crate) fn drain_completions(&self) -> Vec<IoCompletion> {
        let mut out = Vec::new();
        while let Ok(c) = self.rx.try_recv() {
            // A completion (success or failure) ends inflight, allowing a resubmission.
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
