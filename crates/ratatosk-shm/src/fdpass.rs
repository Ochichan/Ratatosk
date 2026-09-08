//! Passing a file descriptor over a Unix domain socket with `SCM_RIGHTS`.
//!
//! One message carries a small payload plus (optionally) exactly one fd. The
//! receiver rejects messages carrying more than one descriptor and closes any
//! descriptor it did not expect, so a hostile peer cannot fill our fd table.

use std::{
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};

use tokio::{io::Interest, net::UnixStream};

/// Send `payload` and `fd` together in a single datagram-like message.
pub async fn send_with_fd(sock: &UnixStream, payload: &[u8], fd: RawFd) -> io::Result<()> {
    let sent = sock
        .async_io(Interest::WRITABLE, || {
            sendmsg_fd(sock.as_raw_fd(), payload, fd)
        })
        .await?;
    if sent != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short write while sending the shared-memory descriptor",
        ));
    }
    Ok(())
}

/// Receive one message into `buf`, returning the byte count and the fd if
/// exactly one was attached.
pub async fn recv_with_fd(
    sock: &UnixStream,
    buf: &mut [u8],
) -> io::Result<(usize, Option<OwnedFd>)> {
    sock.async_io(Interest::READABLE, || recvmsg_fd(sock.as_raw_fd(), buf))
        .await
}

fn sendmsg_fd(sock: RawFd, payload: &[u8], fd: RawFd) -> io::Result<usize> {
    // SAFETY: all structs are plain C structs zero-initialised and then filled
    // with pointers to buffers that outlive the call.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: payload.as_ptr().cast_mut().cast(),
            iov_len: payload.len(),
        };
        let space = libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) as usize;
        let mut cmsg_buf = vec![0u8; space];

        let mut msg: libc::msghdr = mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr().cast();
        set_controllen(&mut msg, space);

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::other("CMSG_FIRSTHDR failed"));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        set_cmsg_len(
            cmsg,
            libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as usize,
        );
        std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>(), fd);

        let n = libc::sendmsg(sock, &msg, 0);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }
}

/// Upper bound on descriptors a single `SCM_RIGHTS` message can carry
/// (Linux `SCM_MAX_FD`; macOS allows fewer). The receive buffer is sized for
/// this many so the kernel never has to truncate the control payload.
const MAX_PASSED_FDS: usize = 253;

fn recvmsg_fd(sock: RawFd, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    // SAFETY: as for `sendmsg_fd`. The control buffer is sized for the maximum
    // number of descriptors one message can carry, so a well-behaved kernel
    // never truncates it. Every descriptor that arrives is taken into an
    // `OwnedFd` (and closed if unwanted) so a hostile peer cannot fill our fd
    // table. The parsing below never trusts `cmsg_len` beyond the bytes the
    // kernel actually wrote (`msg_controllen`): macOS keeps the sender's
    // `cmsg_len` even when it truncates, so bounding by `msg_controllen` is
    // what prevents an out-of-bounds read of the control buffer.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        let space = libc::CMSG_SPACE((MAX_PASSED_FDS * mem::size_of::<RawFd>()) as u32) as usize;
        let mut cmsg_buf = vec![0u8; space];

        let mut msg: libc::msghdr = mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr().cast();
        set_controllen(&mut msg, space);

        let n = libc::recvmsg(sock, &mut msg, cloexec_flag());
        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        let truncated = msg.msg_flags & libc::MSG_CTRUNC != 0;
        let control_base = cmsg_buf.as_ptr() as usize;
        let control_len = controllen(&msg);
        let mut received: Option<OwnedFd> = None;
        let mut extra = 0usize;
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            let header_off = (cmsg as usize).saturating_sub(control_base);
            if header_off + mem::size_of::<libc::cmsghdr>() > control_len {
                break;
            }
            let claimed_len = (*cmsg).cmsg_len as usize;
            // Bound by what the kernel actually wrote, never by the sender's claim.
            let usable_len = claimed_len.min(control_len - header_off);
            if (*cmsg).cmsg_level == libc::SOL_SOCKET
                && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                && usable_len >= libc::CMSG_LEN(0) as usize
            {
                let data_len = usable_len - libc::CMSG_LEN(0) as usize;
                let count = data_len / mem::size_of::<RawFd>();
                for i in 0..count {
                    let raw =
                        std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>().add(i));
                    if raw < 0 {
                        continue;
                    }
                    let owned = OwnedFd::from_raw_fd(raw);
                    set_cloexec(&owned);
                    if received.is_none() {
                        received = Some(owned);
                    } else {
                        // Unwanted descriptors are closed right here by drop.
                        drop(owned);
                        extra += 1;
                    }
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }

        if truncated {
            // Whatever we could not see was still installed by some kernels
            // (macOS); nothing more can be done than refusing the peer.
            drop(received);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ancillary data truncated (too many descriptors)",
            ));
        }
        if extra > 0 {
            drop(received);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer sent more than one file descriptor",
            ));
        }
        Ok((n as usize, received))
    }
}

fn set_cloexec(fd: &OwnedFd) {
    // Linux receives with MSG_CMSG_CLOEXEC already; other platforms need the
    // explicit flag so the descriptor is not inherited by children we spawn.
    #[cfg(not(target_os = "linux"))]
    {
        // SAFETY: valid owned descriptor; F_SETFD only changes the close-on-exec flag.
        unsafe {
            libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    #[cfg(target_os = "linux")]
    {
        let _ = fd;
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn controllen(msg: &libc::msghdr) -> usize {
    msg.msg_controllen
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn controllen(msg: &libc::msghdr) -> usize {
    msg.msg_controllen as usize
}

#[cfg(target_os = "linux")]
fn cloexec_flag() -> libc::c_int {
    libc::MSG_CMSG_CLOEXEC
}

#[cfg(not(target_os = "linux"))]
fn cloexec_flag() -> libc::c_int {
    0
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn set_controllen(msg: &mut libc::msghdr, len: usize) {
    msg.msg_controllen = len;
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
unsafe fn set_controllen(msg: &mut libc::msghdr, len: usize) {
    msg.msg_controllen = len as libc::socklen_t;
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn set_cmsg_len(cmsg: *mut libc::cmsghdr, len: usize) {
    // SAFETY: caller guarantees `cmsg` points into a valid control buffer.
    unsafe { (*cmsg).cmsg_len = len };
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
unsafe fn set_cmsg_len(cmsg: *mut libc::cmsghdr, len: usize) {
    // SAFETY: caller guarantees `cmsg` points into a valid control buffer.
    unsafe { (*cmsg).cmsg_len = len as libc::socklen_t };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};

    #[tokio::test]
    async fn passes_a_descriptor_between_socket_ends() {
        let (a, b) = UnixStream::pair().expect("socketpair");
        let mut file = tempfile::tempfile().expect("tempfile");
        file.write_all(b"hello").expect("write");
        file.rewind().expect("rewind");

        send_with_fd(&a, b"hdr", file.as_raw_fd())
            .await
            .expect("send");
        let mut buf = [0u8; 16];
        let (n, fd) = recv_with_fd(&b, &mut buf).await.expect("recv");
        assert_eq!(&buf[..n], b"hdr");
        let fd = fd.expect("descriptor attached");
        let mut received = std::fs::File::from(fd);
        let mut contents = String::new();
        received
            .read_to_string(&mut contents)
            .expect("read via passed fd");
        assert_eq!(contents, "hello");
    }

    /// Number of descriptors in our table that refer to the given inodes. Other
    /// tests run in parallel and open unrelated descriptors, so a global fd count
    /// would be racy; counting only the inodes we passed is not.
    fn open_fds_referring_to(inodes: &[(u64, u64)]) -> usize {
        (0..1024)
            .filter(|fd| {
                // SAFETY: `stat` is a plain out-parameter; fstat on an arbitrary
                // small integer either fills it or fails with EBADF.
                let mut st: libc::stat = unsafe { mem::zeroed() };
                if unsafe { libc::fstat(*fd, &mut st) } != 0 {
                    return false;
                }
                inodes.contains(&(st.st_dev as u64, st.st_ino as u64))
            })
            .count()
    }

    fn inode_of(file: &std::fs::File) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata().expect("metadata");
        (meta.dev(), meta.ino())
    }

    /// A hostile peer attaching several descriptors must be rejected and must
    /// not leave any of them open in our table.
    #[tokio::test]
    async fn multiple_descriptors_are_rejected_without_leaking() {
        let (a, b) = UnixStream::pair().expect("socketpair");
        let files: Vec<std::fs::File> =
            (0..3).map(|_| tempfile::tempfile().expect("tmp")).collect();
        let raw: Vec<RawFd> = files.iter().map(|f| f.as_raw_fd()).collect();
        let inodes: Vec<(u64, u64)> = files.iter().map(inode_of).collect();
        let before = open_fds_referring_to(&inodes);
        assert_eq!(before, 3);

        // Hand-roll a sendmsg with three fds.
        unsafe {
            let payload = b"hdr";
            let mut iov = libc::iovec {
                iov_base: payload.as_ptr().cast_mut().cast(),
                iov_len: payload.len(),
            };
            let space = libc::CMSG_SPACE((3 * mem::size_of::<RawFd>()) as u32) as usize;
            let mut cmsg_buf = vec![0u8; space];
            let mut msg: libc::msghdr = mem::zeroed();
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cmsg_buf.as_mut_ptr().cast();
            set_controllen(&mut msg, space);
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            set_cmsg_len(
                cmsg,
                libc::CMSG_LEN((3 * mem::size_of::<RawFd>()) as u32) as usize,
            );
            for (i, fd) in raw.iter().enumerate() {
                std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>().add(i), *fd);
            }
            a.writable().await.expect("writable");
            let sent = libc::sendmsg(a.as_raw_fd(), &msg, 0);
            assert_eq!(sent, 3, "sendmsg with 3 fds");
        }

        let mut buf = [0u8; 16];
        let error = recv_with_fd(&b, &mut buf).await.expect_err("must reject");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            open_fds_referring_to(&inodes),
            before,
            "received descriptors must all be closed"
        );
    }

    #[tokio::test]
    async fn plain_message_without_descriptor() {
        let (a, b) = UnixStream::pair().expect("socketpair");
        a.writable().await.expect("writable");
        a.try_write(b"x").expect("write");
        let mut buf = [0u8; 4];
        let (n, fd) = recv_with_fd(&b, &mut buf).await.expect("recv");
        assert_eq!(n, 1);
        assert!(fd.is_none());
    }
}
