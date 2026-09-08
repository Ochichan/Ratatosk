//! Connection establishment over a dedicated Unix domain socket.
//!
//! ```text
//! client → server   HELLO  = magic(8) | requested_ring_bytes(u32 LE)   (12 bytes)
//! server → client   READY  = magic(8) | ring_bytes(u32 LE) | status(u32 LE) (16 bytes)
//!                           + the segment fd as SCM_RIGHTS when status == 0
//! ```
//!
//! After READY the same socket carries doorbell bytes and signals peer death
//! by EOF. The socket is never used for RESP bytes.

use std::{io, os::fd::AsRawFd, path::Path, time::Duration};

use tokio::net::UnixStream;

use crate::{
    fdpass::{recv_with_fd, send_with_fd},
    layout::{self, DEFAULT_RING_BYTES, MAX_RING_BYTES},
    segment::Segment,
    stream::{Role, ShmStream},
};

const HELLO_LEN: usize = 12;
const READY_LEN: usize = 16;
const STATUS_OK: u32 = 0;
const STATUS_REJECTED: u32 = 1;

/// Server-side policy for accepting shared-memory sessions.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Ring size used when the client requests 0.
    pub default_ring_bytes: u32,
    /// Upper bound on what a client may request.
    pub max_ring_bytes: u32,
    /// Spin iterations before parking (keep small on the server).
    pub spin_iters: u32,
    /// Refuse clients whose peer uid differs from ours.
    pub require_same_uid: bool,
    /// Bound on the whole handshake.
    pub handshake_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            default_ring_bytes: DEFAULT_RING_BYTES,
            max_ring_bytes: MAX_RING_BYTES,
            spin_iters: 2000,
            require_same_uid: true,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

/// Client-side connection options.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Requested ring size (0 = server default).
    pub ring_bytes: u32,
    /// Spin iterations before parking. Larger values trade CPU for latency.
    pub spin_iters: u32,
    pub handshake_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            ring_bytes: 0,
            spin_iters: 20_000,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

fn protocol_error(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Complete the server side of the handshake on an accepted control socket.
pub async fn accept_shm_session(
    control: UnixStream,
    config: &ServerConfig,
) -> io::Result<ShmStream> {
    tokio::time::timeout(config.handshake_timeout, accept_inner(control, config))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared-memory handshake timed out"))?
}

async fn accept_inner(control: UnixStream, config: &ServerConfig) -> io::Result<ShmStream> {
    if config.require_same_uid {
        let cred = control.peer_cred()?;
        // SAFETY: getuid has no preconditions and cannot fail.
        let own_uid = unsafe { libc::getuid() };
        if cred.uid() != own_uid {
            let _ = send_ready(&control, 0, STATUS_REJECTED, None).await;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "shared-memory peer uid {} does not match server uid {own_uid}",
                    cred.uid()
                ),
            ));
        }
    }

    let mut hello = [0u8; HELLO_LEN];
    read_exact_no_fd(&control, &mut hello).await?;
    if hello[..8] != layout::MAGIC.to_le_bytes() {
        let _ = send_ready(&control, 0, STATUS_REJECTED, None).await;
        return Err(protocol_error("shared-memory HELLO has a bad magic"));
    }
    let requested = u32::from_le_bytes([hello[8], hello[9], hello[10], hello[11]]);
    let ring_bytes = if requested == 0 {
        config.default_ring_bytes
    } else {
        requested.min(config.max_ring_bytes)
    };
    let ring_bytes = match layout::validate_ring_bytes(ring_bytes) {
        Ok(v) => v,
        Err(error) => {
            let _ = send_ready(&control, 0, STATUS_REJECTED, None).await;
            return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
        }
    };

    let segment = Segment::create(ring_bytes)?;
    // Capture our private ring indices *before* the peer can see the segment.
    let stream = ShmStream::new(segment, control, Role::Server, config.spin_iters);
    let fd = stream.segment().fd().as_raw_fd();
    send_ready(stream.control(), ring_bytes, STATUS_OK, Some(fd)).await?;
    Ok(stream)
}

async fn send_ready(
    control: &UnixStream,
    ring_bytes: u32,
    status: u32,
    fd: Option<i32>,
) -> io::Result<()> {
    let mut ready = [0u8; READY_LEN];
    ready[..8].copy_from_slice(&layout::MAGIC.to_le_bytes());
    ready[8..12].copy_from_slice(&ring_bytes.to_le_bytes());
    ready[12..16].copy_from_slice(&status.to_le_bytes());
    match fd {
        Some(fd) => send_with_fd(control, &ready, fd).await,
        None => {
            let mut written = 0;
            while written < ready.len() {
                control.writable().await?;
                match control.try_write(&ready[written..]) {
                    Ok(n) => written += n,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }
    }
}

async fn read_exact_no_fd(control: &UnixStream, buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let (n, fd) = recv_with_fd(control, &mut buf[filled..]).await?;
        if fd.is_some() {
            return Err(protocol_error("unexpected file descriptor in handshake"));
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed during shared-memory handshake",
            ));
        }
        filled += n;
    }
    Ok(())
}

/// Connect to a server's shared-memory control socket and complete the handshake.
pub async fn connect_shm(path: &Path, config: &ClientConfig) -> io::Result<ShmStream> {
    tokio::time::timeout(config.handshake_timeout, connect_inner(path, config))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared-memory handshake timed out"))?
}

async fn connect_inner(path: &Path, config: &ClientConfig) -> io::Result<ShmStream> {
    let control = UnixStream::connect(path).await?;
    let mut hello = [0u8; HELLO_LEN];
    hello[..8].copy_from_slice(&layout::MAGIC.to_le_bytes());
    hello[8..12].copy_from_slice(&config.ring_bytes.to_le_bytes());
    let mut written = 0;
    while written < hello.len() {
        control.writable().await?;
        match control.try_write(&hello[written..]) {
            Ok(n) => written += n,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }

    let mut ready = [0u8; READY_LEN];
    let mut filled = 0;
    let mut fd = None;
    while filled < READY_LEN {
        let (n, got) = recv_with_fd(&control, &mut ready[filled..]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed during shared-memory handshake",
            ));
        }
        filled += n;
        if got.is_some() {
            if fd.is_some() {
                return Err(protocol_error("server sent more than one descriptor"));
            }
            fd = got;
        }
    }
    if ready[..8] != layout::MAGIC.to_le_bytes() {
        return Err(protocol_error("shared-memory READY has a bad magic"));
    }
    let status = u32::from_le_bytes([ready[12], ready[13], ready[14], ready[15]]);
    if status != STATUS_OK {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "server rejected the shared-memory session",
        ));
    }
    let fd = fd.ok_or_else(|| protocol_error("server did not attach the segment descriptor"))?;
    let segment = Segment::from_fd(fd)?;
    let ring_bytes = u32::from_le_bytes([ready[8], ready[9], ready[10], ready[11]]);
    if segment.ring_bytes() != ring_bytes {
        return Err(protocol_error(
            "READY ring size disagrees with the segment header",
        ));
    }
    Ok(ShmStream::new(
        segment,
        control,
        Role::Client,
        config.spin_iters,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn end_to_end_handshake_over_a_real_socket() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("s.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let server_cfg = ServerConfig {
            spin_iters: 0,
            ..ServerConfig::default()
        };
        let accept = tokio::spawn(async move {
            let (control, _) = listener.accept().await.expect("accept");
            accept_shm_session(control, &server_cfg).await
        });
        let client_cfg = ClientConfig {
            ring_bytes: 8192,
            spin_iters: 0,
            ..ClientConfig::default()
        };
        let mut client = connect_shm(&path, &client_cfg).await.expect("connect");
        let mut server = accept.await.expect("join").expect("accept handshake");
        assert_eq!(client.ring_bytes(), 8192);
        assert_eq!(server.ring_bytes(), 8192);

        client.write_all(b"PING\r\n").await.expect("write");
        let mut buf = [0u8; 16];
        let n = server.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"PING\r\n");
        server.write_all(b"+PONG\r\n").await.expect("write");
        let n = client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"+PONG\r\n");
    }

    #[tokio::test]
    async fn bad_hello_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("s.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let accept = tokio::spawn(async move {
            let (control, _) = listener.accept().await.expect("accept");
            accept_shm_session(control, &ServerConfig::default()).await
        });
        let control = UnixStream::connect(&path).await.expect("connect");
        control.writable().await.expect("writable");
        control.try_write(b"NOTMAGIC0000").expect("write");
        let error = accept.await.expect("join").expect_err("must reject");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
