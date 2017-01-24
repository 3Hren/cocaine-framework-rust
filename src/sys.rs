use std::io::Error;
use std::os::unix::io::RawFd;

use libc;
use nix::sys::socket;

pub fn sendmsg(fd: RawFd, iov: &[&[u8]]) -> Result<usize, Error> {
    #[cfg(target_os = "linux")]
    let len = iov.len() as usize;
    #[cfg(target_os = "macos")]
    let len = iov.len() as i32;

    let mhdr = libc::msghdr {
        msg_name: 0 as *mut libc::c_void,
        msg_namelen: 0,
        msg_iov: iov.as_ptr() as *mut libc::iovec,
        msg_iovlen: len,
        msg_control: 0 as *mut libc::c_void,
        msg_controllen: 0,
        msg_flags: 0,
    };

    let rc = unsafe {
        libc::sendmsg(fd, &mhdr, socket::MsgFlags::empty().bits())
    };

    if rc < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(rc as usize)
    }
}
