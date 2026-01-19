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
use crate::record::RecordInfo;

#[derive(Debug)]
pub(crate) enum IoRequest {
    ReadBytes {
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
        address: Address,
        len: usize,
    },
    /// Read a single varlen record starting at `address`.
    ///
    /// This performs a small header read first to discover the record length, then reads the full
    /// record bytes. The returned completion payload contains the full record bytes.
    ReadVarLenRecord {
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
        address: Address,
    },
    Shutdown,
}

#[derive(Debug)]
pub(crate) enum IoCompletion {
    ReadBytesDone {
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
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
    pub(crate) fn new(device: Arc<D>, page_size: usize) -> Self {
        let (tx, req_rx) = unbounded::<IoRequest>();
        let (comp_tx, rx) = unbounded::<IoCompletion>();

        const U32_BYTES: usize = std::mem::size_of::<u32>();
        const RECORD_INFO_SIZE: usize = std::mem::size_of::<RecordInfo>();
        const VARLEN_LENGTHS_SIZE: usize = 2 * U32_BYTES;
        const VARLEN_HEADER_SIZE: usize = RECORD_INFO_SIZE + VARLEN_LENGTHS_SIZE;
        const KEY_LEN_OFFSET: usize = RECORD_INFO_SIZE;
        const VAL_LEN_OFFSET: usize = RECORD_INFO_SIZE + U32_BYTES;

        let varlen_read_guess =
            page_size.clamp(VARLEN_HEADER_SIZE, 4096usize.max(VARLEN_HEADER_SIZE));
        let worker = thread::spawn(move || {
            // Dedicated runtime: do not depend on the caller being inside a Tokio runtime.
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = comp_tx.send(IoCompletion::ReadBytesDone {
                        thread_id: 0,
                        thread_tag: 0,
                        ctx_id: 0,
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
                        ctx_id,
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
                            ctx_id,
                            address,
                            result,
                        });
                    }
                    IoRequest::ReadVarLenRecord {
                        thread_id,
                        thread_tag,
                        ctx_id,
                        address,
                    } => {
                        // Read a reasonable guess first to avoid the 2-IOP path for small records.
                        // If the record does not fit, fall back to an exact-sized read.
                        let first_result = rt.block_on(async {
                            let mut buf = vec![0u8; varlen_read_guess];
                            let n = device.read(address.control(), &mut buf).await?;
                            buf.truncate(n);
                            Ok(buf)
                        });

                        let result = match first_result {
                            Ok(mut buf) => (|| -> io::Result<Vec<u8>> {
                                if buf.len() < VARLEN_HEADER_SIZE {
                                    return Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        format!(
                                            "short read: expected at least {VARLEN_HEADER_SIZE}, got {}",
                                            buf.len()
                                        ),
                                    ));
                                }

                                let key_len = u32::from_le_bytes(
                                    buf[KEY_LEN_OFFSET..KEY_LEN_OFFSET + U32_BYTES]
                                        .try_into()
                                        .unwrap(),
                                ) as usize;
                                let value_len = u32::from_le_bytes(
                                    buf[VAL_LEN_OFFSET..VAL_LEN_OFFSET + U32_BYTES]
                                        .try_into()
                                        .unwrap(),
                                ) as usize;
                                let total = VARLEN_HEADER_SIZE
                                    .checked_add(key_len)
                                    .and_then(|n| n.checked_add(value_len))
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "record size overflow",
                                        )
                                    })?;

                                // Bound the allocation to 1GiB to avoid pathological allocations on corruption.
                                if total > (1 << 30) {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("record too large: {total} bytes"),
                                    ));
                                }

                                if total <= buf.len() {
                                    buf.truncate(total);
                                    return Ok(buf);
                                }

                                let bytes_read = buf.len();
                                buf.resize(total, 0);
                                rt.block_on(async {
                                    let remainder = &mut buf[bytes_read..];
                                    let remainder_len = remainder.len();
                                    let offset = address.control()
                                        + u64::try_from(bytes_read).map_err(|_| {
                                            io::Error::new(
                                                io::ErrorKind::InvalidData,
                                                "read offset overflow",
                                            )
                                        })?;
                                    let n = device.read(offset, remainder).await?;
                                    if n != remainder_len {
                                        return Err(io::Error::new(
                                            io::ErrorKind::UnexpectedEof,
                                            format!(
                                                "short read: expected {total}, got {}",
                                                bytes_read + n
                                            ),
                                        ));
                                    }
                                    Ok(buf)
                                })
                            })(),
                            Err(e) => Err(e),
                        };

                        let _ = comp_tx.send(IoCompletion::ReadBytesDone {
                            thread_id,
                            thread_tag,
                            ctx_id,
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
        ctx_id: u64,
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
                ctx_id,
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

    pub(crate) fn submit_read_varlen_record(
        &self,
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
        address: Address,
    ) -> bool {
        {
            let mut inflight = self.inflight_reads.lock();
            if !inflight.insert(address.control()) {
                return false;
            }
        }

        if self
            .tx
            .send(IoRequest::ReadVarLenRecord {
                thread_id,
                thread_tag,
                ctx_id,
                address,
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
