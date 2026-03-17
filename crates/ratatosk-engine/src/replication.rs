use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReplicationMode {
    #[default]
    Master,
    Replica {
        master_host: Bytes,
        master_port: i64,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaClientState {
    pub listening_port: Option<i64>,
    pub ip_address: Option<Bytes>,
    pub capabilities: HashSet<Bytes>,
    pub ack_offset: i64,
    pub ack_time_ms: Option<i64>,
    pub handshake_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaClientInfo {
    pub client_id: i64,
    pub listening_port: i64,
    pub ip_address: Bytes,
    pub ack_offset: i64,
    pub lag_seconds: i64,
    pub state: &'static str,
}

#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub(crate) mode: ReplicationMode,
    pub(crate) primary_replid: Bytes,
    pub(crate) master_repl_offset: i64,
    pub(crate) replicas: HashMap<i64, ReplicaClientState>,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Master,
            primary_replid: generate_replid(),
            master_repl_offset: 0,
            replicas: HashMap::new(),
        }
    }
}

impl ReplicationState {
    pub(crate) fn replica_entry_mut(&mut self, client_id: i64) -> &mut ReplicaClientState {
        self.replicas.entry(client_id).or_default()
    }

    pub(crate) fn reset_as_master(&mut self) {
        self.mode = ReplicationMode::Master;
        self.primary_replid = generate_replid();
    }
}

fn generate_replid() -> Bytes {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut buf = [0u8; 40];
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in &mut buf {
        *b = HEX[rng.gen_range(0..16)];
    }
    Bytes::copy_from_slice(&buf)
}
