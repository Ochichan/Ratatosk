use std::net::IpAddr;

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

/// A byte stream the client session loop can drive: TCP, Unix socket, or the
/// experimental shared-memory stream. Peer death is observed through ordinary
/// reads (EOF), including while a blocking command is parked — the session
/// keeps reading pipelined bytes into its input buffer during the wait, like
/// Redis fills `querybuf` for a blocked client.
pub trait SessionStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> SessionStream for T {}

#[derive(Debug, Clone)]
pub enum PeerKind {
    Tcp,
    Unix,
    /// Shared-memory session (control socket + rings); local like `Unix`.
    Shm,
}

/// Transport facts captured once at accept time; the session loop never asks the socket again.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub kind: PeerKind,
    /// Redis `CLIENT LIST` addr= value. TCP: "ip:port". Unix: "<socket-path>:0".
    pub addr: Bytes,
    /// Redis `CLIENT LIST` laddr= value. TCP: "ip:port". Unix: "<socket-path>:0".
    pub laddr: Bytes,
    /// Human-readable remote for tracing spans / logs (TCP: "ip:port"; Unix: "unix:<path>").
    pub remote_display: String,
    /// Peer IP for the connection rate limiter; None for Unix.
    pub peer_ip: Option<IpAddr>,
}

impl ConnInfo {
    pub fn from_tcp(stream: &TcpStream) -> Self {
        let peer_addr = stream.peer_addr().ok();
        let local_addr = stream.local_addr().ok();

        Self {
            kind: PeerKind::Tcp,
            addr: peer_addr
                .map(|addr| Bytes::from(addr.to_string()))
                .unwrap_or_else(|| Bytes::from_static(b"127.0.0.1:0")),
            laddr: local_addr
                .map(|addr| Bytes::from(addr.to_string()))
                .unwrap_or_else(|| Bytes::from_static(b"127.0.0.1:0")),
            remote_display: peer_addr
                .map(|addr| addr.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            peer_ip: peer_addr.map(|addr| addr.ip()),
        }
    }

    /// Shared-memory session accepted on `socket_path`. Reported like a Unix
    /// socket client (`addr=<path>:0`, flag `U`) so Redis clients that parse
    /// `CLIENT LIST` keep working; the tracing display says `shm:`.
    #[cfg(unix)]
    pub fn from_shm(socket_path: &std::path::Path) -> Self {
        let socket_addr = Bytes::from(format!("{}:0", socket_path.display()));

        Self {
            kind: PeerKind::Shm,
            addr: socket_addr.clone(),
            laddr: socket_addr,
            remote_display: format!("shm:{}", socket_path.display()),
            peer_ip: None,
        }
    }

    #[cfg(unix)]
    pub fn from_unix(socket_path: &std::path::Path) -> Self {
        let socket_addr = Bytes::from(format!("{}:0", socket_path.display()));

        Self {
            kind: PeerKind::Unix,
            addr: socket_addr.clone(),
            laddr: socket_addr,
            remote_display: format!("unix:{}", socket_path.display()),
            peer_ip: None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::path::Path;

    #[cfg(unix)]
    use super::{ConnInfo, PeerKind};

    #[cfg(unix)]
    #[test]
    fn unix_connection_info_uses_socket_path_for_client_addresses() {
        let info = ConnInfo::from_unix(Path::new("/tmp/r.sock"));

        assert!(matches!(info.kind, PeerKind::Unix));
        assert_eq!(&info.addr[..], b"/tmp/r.sock:0");
        assert_eq!(&info.laddr[..], b"/tmp/r.sock:0");
        assert_eq!(info.remote_display, "unix:/tmp/r.sock");
        assert_eq!(info.peer_ip, None);
    }
}
