//! `ShmStream`: an `AsyncRead + AsyncWrite` byte stream over the shared rings.
//!
//! Wake-ups travel over the control Unix socket as single "doorbell" bytes;
//! EOF on that socket means the peer is gone. Both directions of the stream
//! wait on the *readable* side of the control socket, so a `ShmStream` must be
//! driven from a single task (which is how the server session loop uses it —
//! it never splits the stream).
//!
//! Waiting policy is hybrid: spin `spin_iters` times, then arm the parked
//! flag and sleep on the doorbell. The server should keep `spin_iters` small
//! because a spinning `poll_*` occupies a runtime worker.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, Interest, ReadBuf},
    net::UnixStream,
};

use crate::{
    ring::{Consumer, Corrupt, Park, Producer, RingView},
    segment::Segment,
};

/// Which side of the connection this stream is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Produces into `s2c`, consumes `c2s`.
    Server,
    /// Produces into `c2s`, consumes `s2c`.
    Client,
}

enum Drain {
    Drained,
    Eof,
}

pub struct ShmStream {
    segment: Segment,
    control: UnixStream,
    role: Role,
    producer: Producer,
    consumer: Consumer,
    spin_iters: u32,
    peer_gone: bool,
    read_parked: bool,
    write_parked: bool,
}

impl std::fmt::Debug for ShmStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmStream")
            .field("role", &self.role)
            .field("ring_bytes", &self.segment.ring_bytes())
            .field("peer_gone", &self.peer_gone)
            .finish()
    }
}

impl ShmStream {
    pub fn new(segment: Segment, control: UnixStream, role: Role, spin_iters: u32) -> Self {
        let (producer, consumer) = match role {
            Role::Server => (Producer::new(&segment.s2c()), Consumer::new(&segment.c2s())),
            Role::Client => (Producer::new(&segment.c2s()), Consumer::new(&segment.s2c())),
        };
        Self {
            segment,
            control,
            role,
            producer,
            consumer,
            spin_iters,
            peer_gone: false,
            read_parked: false,
            write_parked: false,
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn ring_bytes(&self) -> u32 {
        self.segment.ring_bytes()
    }

    /// The control socket (for peer credentials or diagnostics).
    pub fn control(&self) -> &UnixStream {
        &self.control
    }

    /// The mapped segment. Exposed for diagnostics and hostile-peer tests;
    /// production code never needs it.
    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    fn inbound(&self) -> RingView<'_> {
        inbound_view(&self.segment, self.role)
    }

    fn outbound(&self) -> RingView<'_> {
        outbound_view(&self.segment, self.role)
    }

    /// Read and discard queued doorbell bytes until the socket would block.
    fn drain_doorbell(&mut self) -> io::Result<Drain> {
        let mut scratch = [0u8; 64];
        loop {
            match self.control.try_read(&mut scratch) {
                Ok(0) => return Ok(Drain::Eof),
                Ok(_) => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(Drain::Drained);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    /// Wait for a doorbell (or peer death) while a parked flag is armed.
    /// Returns `Poll::Pending` with the waker registered on the control socket.
    fn poll_doorbell(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.control.poll_read_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => match self.drain_doorbell() {
                Ok(Drain::Drained) => Poll::Ready(Ok(())),
                Ok(Drain::Eof) => {
                    self.peer_gone = true;
                    Poll::Ready(Ok(()))
                }
                Err(error) => Poll::Ready(Err(error)),
            },
        }
    }

    /// Resolve with `true` once the peer has closed the control socket.
    /// Doorbell bytes are drained on the way so a parked session does not spin.
    pub async fn peer_closed(&self) -> io::Result<bool> {
        let mut scratch = [0u8; 64];
        loop {
            let ready = self.control.ready(Interest::READABLE).await?;
            if ready.is_read_closed() {
                return Ok(true);
            }
            loop {
                match self.control.try_read(&mut scratch) {
                    Ok(0) => return Ok(true),
                    Ok(_) => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

/// Send one doorbell byte with a direct non-blocking `send(2)`.
///
/// This deliberately bypasses tokio's `try_write`: that helper consults the
/// cached *writable* readiness and returns `WouldBlock` without a syscall when
/// the socket has never been polled for writability, which would silently drop
/// the very first wake-up and leave the peer parked forever. A full socket
/// buffer means the peer already has wake-ups queued, and a closed socket is
/// discovered by the read side, so both failure modes are safe to ignore.
fn ring_doorbell(control: &UnixStream) {
    use std::os::fd::AsRawFd;
    let byte = [1u8];
    // SAFETY: valid socket descriptor owned by `control`; the buffer is a live
    // stack array of length 1; flags are plain constants.
    let _ = unsafe {
        libc::send(
            control.as_raw_fd(),
            byte.as_ptr().cast(),
            1,
            libc::MSG_DONTWAIT | nosignal_flag(),
        )
    };
}

#[cfg(target_os = "linux")]
fn nosignal_flag() -> libc::c_int {
    libc::MSG_NOSIGNAL
}

#[cfg(not(target_os = "linux"))]
fn nosignal_flag() -> libc::c_int {
    // Rust's std ignores SIGPIPE process-wide at startup, so EPIPE is returned
    // instead of a signal on platforms without MSG_NOSIGNAL.
    0
}

fn inbound_view(segment: &Segment, role: Role) -> RingView<'_> {
    match role {
        Role::Server => segment.c2s(),
        Role::Client => segment.s2c(),
    }
}

fn outbound_view(segment: &Segment, role: Role) -> RingView<'_> {
    match role {
        Role::Server => segment.s2c(),
        Role::Client => segment.c2s(),
    }
}

fn corrupt(_: Corrupt) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "shared-memory ring corrupted by peer; closing connection",
    )
}

impl AsyncRead for ShmStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.read_parked {
            this.consumer.unpark(&this.inbound());
            this.read_parked = false;
        }
        loop {
            // 1. Try to copy out whatever is available.
            let popped = {
                let view = inbound_view(&this.segment, this.role);
                let dst = buf.initialize_unfilled();
                match this.consumer.pop(&view, dst) {
                    Ok(n) => {
                        if n > 0 && this.consumer.should_ring(&view) {
                            ring_doorbell(&this.control);
                        }
                        n
                    }
                    Err(error) => return Poll::Ready(Err(corrupt(error))),
                }
            };
            if popped > 0 {
                buf.advance(popped);
                return Poll::Ready(Ok(()));
            }
            if this.peer_gone {
                return Poll::Ready(Ok(())); // EOF
            }

            // 2. Spin briefly.
            let mut got_data = false;
            for _ in 0..this.spin_iters {
                std::hint::spin_loop();
                match this.consumer.has_data(&this.inbound()) {
                    Ok(true) => {
                        got_data = true;
                        break;
                    }
                    Ok(false) => {}
                    Err(error) => return Poll::Ready(Err(corrupt(error))),
                }
            }
            if got_data {
                continue;
            }

            // 3. Park.
            match this.consumer.park_for_data(&this.inbound()) {
                Err(error) => return Poll::Ready(Err(corrupt(error))),
                Ok(Park::Proceed) => continue,
                Ok(Park::Sleep) => match this.poll_doorbell(cx) {
                    Poll::Pending => {
                        this.read_parked = true;
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(())) => {
                        this.consumer.unpark(&this.inbound());
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        this.consumer.unpark(&this.inbound());
                        return Poll::Ready(Err(error));
                    }
                },
            }
        }
    }
}

impl AsyncWrite for ShmStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_parked {
            this.producer.unpark(&this.outbound());
            this.write_parked = false;
        }
        if src.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if this.peer_gone {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "shared-memory peer is gone",
                )));
            }

            let pushed = {
                let view = outbound_view(&this.segment, this.role);
                match this.producer.push(&view, src) {
                    Ok(n) => {
                        if n > 0 && this.producer.should_ring(&view) {
                            ring_doorbell(&this.control);
                        }
                        n
                    }
                    Err(error) => return Poll::Ready(Err(corrupt(error))),
                }
            };
            if pushed > 0 {
                return Poll::Ready(Ok(pushed));
            }

            let mut got_space = false;
            for _ in 0..this.spin_iters {
                std::hint::spin_loop();
                match this.producer.has_space(&this.outbound()) {
                    Ok(true) => {
                        got_space = true;
                        break;
                    }
                    Ok(false) => {}
                    Err(error) => return Poll::Ready(Err(corrupt(error))),
                }
            }
            if got_space {
                continue;
            }

            match this.producer.park_for_space(&this.outbound()) {
                Err(error) => return Poll::Ready(Err(corrupt(error))),
                Ok(Park::Proceed) => continue,
                Ok(Park::Sleep) => match this.poll_doorbell(cx) {
                    Poll::Pending => {
                        this.write_parked = true;
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(())) => {
                        this.producer.unpark(&this.outbound());
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        this.producer.unpark(&this.outbound());
                        return Poll::Ready(Err(error));
                    }
                },
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Every `poll_write` publishes immediately; there is no buffered state.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().control).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn pair(ring_bytes: u32, spin_iters: u32) -> (ShmStream, ShmStream) {
        let segment = Segment::create(ring_bytes).expect("segment");
        let dup = segment.fd().try_clone_to_owned().expect("dup");
        let peer_segment = Segment::from_fd(dup).expect("reopen");
        let (a, b) = UnixStream::pair().expect("socketpair");
        (
            ShmStream::new(segment, a, Role::Server, spin_iters),
            ShmStream::new(peer_segment, b, Role::Client, spin_iters),
        )
    }

    #[tokio::test]
    async fn round_trip_small_messages_both_directions() {
        let (mut server, mut client) = pair(4096, 0);
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("write");
        let mut buf = [0u8; 64];
        let n = server.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"*1\r\n$4\r\nPING\r\n");
        server.write_all(b"+PONG\r\n").await.expect("write");
        let n = client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"+PONG\r\n");
    }

    #[tokio::test]
    async fn large_transfer_exercises_full_ring_backpressure() {
        let (mut server, mut client) = pair(4096, 0);
        let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();

        let writer = tokio::spawn(async move {
            client.write_all(&payload).await.expect("write all");
            client
        });
        let mut received = vec![0u8; expected.len()];
        server.read_exact(&mut received).await.expect("read exact");
        assert_eq!(received, expected);
        let _client = writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn peer_drop_yields_eof_then_broken_pipe() {
        let (mut server, client) = pair(4096, 0);
        drop(client);
        let mut buf = [0u8; 8];
        let n = server.read(&mut buf).await.expect("read after peer drop");
        assert_eq!(n, 0, "EOF expected");
        let error = server.write_all(b"x").await.expect_err("write must fail");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn corrupted_index_closes_the_stream() {
        let (mut server, client) = pair(4096, 0);
        // Client scribbles an impossible tail into the c2s ring.
        client
            .segment
            .c2s()
            .tail
            .store(u64::MAX / 2, std::sync::atomic::Ordering::SeqCst);
        let mut buf = [0u8; 8];
        let error = server.read(&mut buf).await.expect_err("corrupt ring");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn peer_closed_resolves_on_drop_and_ignores_doorbells() {
        let (server, client) = pair(4096, 0);
        // A stray doorbell must not resolve peer_closed.
        ring_doorbell(&client.control);
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), server.peer_closed()).await;
        assert!(
            waited.is_err(),
            "peer_closed must not resolve on a doorbell"
        );
        drop(client);
        assert!(server.peer_closed().await.expect("peer_closed"));
    }
}
