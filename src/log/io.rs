//! I/O helpers for FasterLog.

use std::io;
use std::sync::Arc;

use tokio::runtime::RuntimeFlavor;

use crate::device::StorageDevice;

pub(crate) struct IoExecutor {
    handle: Option<tokio::runtime::Handle>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl IoExecutor {
    pub(crate) fn new() -> io::Result<Self> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => Ok(Self {
                handle: Some(handle),
                runtime: None,
            }),
            Err(_) => Ok(Self {
                handle: None,
                runtime: Some(tokio::runtime::Runtime::new()?),
            }),
        }
    }

    pub(crate) fn block_on<T>(
        &self,
        fut: impl std::future::Future<Output = io::Result<T>>,
    ) -> io::Result<T> {
        if let Some(handle) = &self.handle {
            if tokio::runtime::Handle::try_current().is_ok() {
                return match handle.runtime_flavor() {
                    RuntimeFlavor::MultiThread => {
                        tokio::task::block_in_place(|| handle.block_on(fut))
                    }
                    RuntimeFlavor::CurrentThread => Err(io::Error::other(
                        "blocking log I/O is not supported on a current-thread Tokio runtime",
                    )),
                    _ => Err(io::Error::other(
                        "unsupported Tokio runtime flavor for blocking log I/O",
                    )),
                };
            }
            return handle.block_on(fut);
        }
        if let Some(runtime) = &self.runtime {
            return runtime.block_on(fut);
        }
        Err(io::Error::other("missing runtime handle"))
    }
}

pub(crate) fn block_on_device<T>(
    _device: &Arc<impl StorageDevice>,
    fut: impl std::future::Future<Output = io::Result<T>>,
) -> io::Result<T> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
            RuntimeFlavor::CurrentThread => Err(io::Error::other(
                "blocking log I/O is not supported on a current-thread Tokio runtime",
            )),
            _ => Err(io::Error::other(
                "unsupported Tokio runtime flavor for blocking log I/O",
            )),
        },
        Err(_) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(fut)
        }
    }
}
