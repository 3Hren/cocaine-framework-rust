use std::io::Error;

use futures::Async;

use tokio_core::net::TcpStream;

#[cfg(unix)]
mod unix {
    use std::io::Error;
    use std::os::unix::io::RawFd;
    use std::ptr;

    use libc;

    pub fn sendmsg(fd: RawFd, iov: &[&[u8]]) -> Result<usize, Error> {
        #[cfg(target_os = "linux")]
        let len = iov.len() as usize;
        #[cfg(target_os = "macos")]
        let len = iov.len() as i32;

        let msghdr = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iov.as_ptr() as *mut libc::iovec,
            msg_iovlen: len,
            msg_control: ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };

        let rc = unsafe {
            libc::sendmsg(fd, &msghdr, 0)
        };

        if rc < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }
}

/// Allow I/O writers to be interest of an event loop.
pub trait PollWrite {
    /// Tests to see if this source is ready to be written to or not.
    fn poll_write(&self) -> Async<()>;
}

impl PollWrite for TcpStream {
    fn poll_write(&self) -> Async<()> {
        TcpStream::poll_write(self)
    }
}

/// A `SendAll` tries to send multiple byte buffers at a time.
pub trait SendAll {
    /// Attempts to write multiple byte buffers into this sender.
    fn send_all(&mut self, iov: &[&[u8]]) -> Result<usize, Error>;
}

#[cfg(unix)]
impl SendAll for TcpStream {
    fn send_all(&mut self, iov: &[&[u8]]) -> Result<usize, Error> {
        use std::os::unix::io::AsRawFd;

        unix::sendmsg(self.as_raw_fd(), iov)
    }
}

#[cfg(windows)]
impl SendAll for TcpStream {
    fn send_all(&mut self, iov: &[&[u8]]) -> Result<usize, Error> {
        use std::io::Write;

        let mut nsend = 0;
        for buf in iov {
            let len = self.write(buf)?;
            nsend += len;

            if len != buf.len() {
                break;
            }
        }

        Ok(nsend)
    }
}
