use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Context;
use fly_io::{
    network::Network,
    service::{LinearStore, SequentialStore, Storage},
    Event,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

type Topic = String;
type Offset = usize;
type Entry = usize;
type Log = Vec<Entry>;
type CommitOffsets = HashMap<String, Offset>;

struct StorageKey {}
impl StorageKey {
    fn log(topic: &str) -> String {
        format!("{}/log", topic)
    }

    fn commit() -> String {
        "commits".to_string()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KafkaPayload {
    Send {
        key: Topic,
        msg: usize,
    },
    SendOk {
        offset: Offset,
    },
    Poll {
        offsets: HashMap<Topic, Offset>,
    },
    PollOk {
        msgs: HashMap<Topic, Vec<(Offset, Entry)>>,
    },
    CommitOffsets {
        offsets: HashMap<Topic, Offset>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<Topic>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<Topic, Offset>,
    },
}

#[derive(Clone)]
struct KafkaNode {
    linear_store: LinearStore,
    sequential_store: SequentialStore,
    pub cas_failures: Arc<RwLock<usize>>,
    pub total_appends: Arc<RwLock<usize>>,
}

impl KafkaNode {
    pub fn new(node_id: String) -> Self {
        Self {
            linear_store: LinearStore::new(node_id.clone()),
            sequential_store: SequentialStore::new(node_id.clone()),
            cas_failures: Arc::new(RwLock::new(0)),
            total_appends: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn read_or_create<T, STORAGE>(
        &self,
        key: String,
        storage: &STORAGE,
        network: &Network,
    ) -> anyhow::Result<T>
    where
        T: Send + Serialize + DeserializeOwned + Default + Clone,
        STORAGE: Storage<()> + Sync,
    {
        if let Ok(value) = storage.read::<T>(key.clone(), network).await {
            return Ok(value);
        };

        let value = T::default();
        if storage
            .compare_and_store(key, value.clone(), value.clone(), network)
            .await
            .is_ok()
        {
            return Ok(value);
        };

        panic!("could not read or set value");
    }

    async fn append_entry(
        &mut self,
        topic: String,
        entry: Entry,
        network: &Network,
    ) -> anyhow::Result<Offset> {
        let key = StorageKey::log(&topic);

        *self.total_appends.write().unwrap() += 1;
        loop {
            let mut log = self
                .read_or_create::<Log, _>(key.clone(), &self.linear_store, network)
                .await
                .context("reading log")?;

            let offset = log.len();
            log.push(entry);

            if self
                .linear_store
                .compare_and_store(key.clone(), log[..log.len() - 1].to_vec(), log, network)
                .await
                .is_ok()
            {
                return Ok(offset);
            }

            *self.cas_failures.write().unwrap() += 1;
        }
    }

    async fn select_entries(
        &self,
        topic: String,
        requested_offset: Offset,
        network: &Network,
    ) -> Option<Vec<(Offset, Entry)>> {
        let Ok(log) = self
            .linear_store
            .read::<Log>(StorageKey::log(&topic), network)
            .await
        else {
            return None;
        };

        if log.len() <= requested_offset {
            return None;
        }

        let n_logs = std::cmp::min(3, log.len() - requested_offset);
        let selected = log[requested_offset..requested_offset + n_logs]
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, entry)| (requested_offset + i, entry))
            .collect::<Vec<_>>();

        Some(selected)
    }
}

impl Drop for KafkaNode {
    fn drop(&mut self) {
        let cas_failures = *self.cas_failures.read().unwrap();
        let total_appends = *self.total_appends.read().unwrap();
        eprintln!(
            "CAS FAILURES: {} / TOTAL APPENDS: {}",
            cas_failures, total_appends
        );
    }
}

#[async_trait::async_trait]
impl fly_io::Node<KafkaPayload> for KafkaNode {
    fn from_init(init: fly_io::protocol::Init, _network: &Network) -> Self {
        Self::new(init.node_id)
    }

    async fn step(
        &mut self,
        event: Event<KafkaPayload, ()>,
        network: &Network,
    ) -> anyhow::Result<()> {
        match event {
            Event::Storage(_) => {}
            Event::Injected(_) => {}
            Event::Message(message) => {
                let mut reply = message.into_reply();
                if let Some(payload) = match reply.body.payload {
                    KafkaPayload::Send { key, msg } => {
                        let offset = self
                            .append_entry(key, msg, network)
                            .await
                            .context("adding message")?;

                        Some(KafkaPayload::SendOk { offset })
                    }
                    KafkaPayload::SendOk { .. } => None,
                    KafkaPayload::Poll { offsets } => {
                        let mut result = HashMap::new();
                        for (topic, requested_offset) in offsets.into_iter() {
                            if let Some(selected) = self
                                .select_entries(topic.clone(), requested_offset, network)
                                .await
                            {
                                result.insert(topic, selected);
                            }
                        }
                        Some(KafkaPayload::PollOk { msgs: result })
                    }
                    KafkaPayload::PollOk { .. } => None,
                    KafkaPayload::CommitOffsets { offsets } => {
                        self.sequential_store
                            .write(StorageKey::commit(), offsets, network)?;
                        Some(KafkaPayload::CommitOffsetsOk)
                    }
                    KafkaPayload::CommitOffsetsOk => None,
                    KafkaPayload::ListCommittedOffsets { keys } => {
                        let commits = self
                            .read_or_create::<CommitOffsets, _>(
                                StorageKey::commit(),
                                &self.sequential_store,
                                network,
                            )
                            .await
                            .context("reading commits")?;

                        let commits = commits
                            .into_iter()
                            .filter(|(topic, _)| keys.contains(topic))
                            .collect();

                        Some(KafkaPayload::ListCommittedOffsetsOk { offsets: commits })
                    }
                    KafkaPayload::ListCommittedOffsetsOk { .. } => None,
                } {
                    reply.body.payload = payload;
                    network.send(reply).context("sending reply")?;
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::server::Server::new().serve::<KafkaNode, KafkaPayload>()
}
