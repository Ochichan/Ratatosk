//! Hostile-peer test: the client scribbles random bytes over the shared
//! segment (header, control words, data) while the server is serving an echo
//! loop. The server must never panic or misbehave; it may only observe
//! garbage bytes, a corruption error, or EOF.
#![cfg(all(unix, not(loom)))]

use std::{sync::atomic::Ordering, time::Duration};

use ratatosk_shm::{ClientConfig, ServerConfig, accept_shm_session, connect_shm};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

async fn echo_server(listener: UnixListener) -> std::io::Result<u64> {
    let (control, _) = listener.accept().await?;
    let cfg = ServerConfig {
        spin_iters: 10,
        handshake_timeout: Duration::from_secs(2),
        ..ServerConfig::default()
    };
    let mut stream = accept_shm_session(control, &cfg).await?;
    let mut buf = [0u8; 256];
    let mut echoed = 0u64;
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(echoed);
        }
        stream.write_all(&buf[..n]).await?;
        echoed += n as u64;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_corruption_never_panics_the_server() {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    for round in 0..40u32 {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("h.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let server = tokio::spawn(echo_server(listener));

        let cfg = ClientConfig {
            ring_bytes: 4096,
            spin_iters: 10,
            handshake_timeout: Duration::from_secs(2),
        };
        let mut client = connect_shm(&path, &cfg).await.expect("connect");

        // Exchange a little traffic first so both rings have state.
        client.write_all(b"warmup").await.expect("write");
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"warmup");

        // Corrupt: pick a target region and scribble.
        let seg = client.segment();
        let choice = rng.next() % 4;
        match choice {
            0 => {
                // Control words of the c2s ring (server consumes): tail/parked.
                let view = seg.c2s();
                view.tail.store(rng.next(), Ordering::SeqCst);
                view.producer_parked
                    .store(rng.next() as u32, Ordering::SeqCst);
            }
            1 => {
                // Control words of the s2c ring (server produces): head/parked.
                let view = seg.s2c();
                view.head.store(rng.next(), Ordering::SeqCst);
                view.consumer_parked
                    .store(rng.next() as u32, Ordering::SeqCst);
            }
            2 => {
                // Random data bytes in both rings.
                for _ in 0..64 {
                    let view = if rng.next() % 2 == 0 {
                        seg.c2s()
                    } else {
                        seg.s2c()
                    };
                    let idx = (rng.next() as usize) % view.data.len();
                    view.data[idx].store(rng.next() as u8, Ordering::SeqCst);
                }
            }
            _ => {
                // The words the *server* owns (it keeps private copies and never
                // re-reads them, so this must not confuse the server itself; the
                // client only hurts its own view).
                seg.c2s().head.store(rng.next(), Ordering::SeqCst);
                seg.s2c().tail.store(rng.next(), Ordering::SeqCst);
            }
        }

        // Keep talking; whatever happens must be an error/EOF, not a hang or panic.
        let talk = tokio::time::timeout(Duration::from_secs(3), async {
            for _ in 0..50 {
                if client.write_all(b"ping-after-corruption").await.is_err() {
                    return;
                }
                let mut sink = [0u8; 64];
                match client.read(&mut sink).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await;
        drop(client);

        let server_result = tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap_or_else(|_| panic!("round {round}: server hung after corruption {choice}"))
            .unwrap_or_else(|e| panic!("round {round}: server task panicked: {e}"));
        // Either the server saw corruption / EOF (Err or Ok) — both acceptable.
        let _ = server_result;
        let _ = talk;
    }
}
