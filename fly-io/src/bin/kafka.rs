use std::collections::HashMap;

use anyhow::{anyhow, Context};
use fly_io::{
    network::Network,
    protocol::{Body, Message},
    service::{LinearStore, StorageType},
    Event,
};
use serde::{Deserialize, Serialize};

type Topic = String;
type Offset = usize;
type Entry = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KafkaPayload {
    // RPC
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
    // Storage node payloads
    Read {
        key: String,
    },
    ReadOk {
        value: StorageType,
    },
    Write {
        key: String,
        value: StorageType,
    },
    WriteOk,
    Cas {
        key: String,
        from: StorageType,
        to: StorageType,
        create_if_not_exists: Option<bool>,
    },
    CasOk,
    Error {
        code: usize,
        text: String,
    },
}

#[derive(Clone)]
struct KafkaNode {
    node_id: String,
}

impl KafkaNode {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    fn message<P>(&self, payload: P) -> Message<P> {
        Message {
            src: self.node_id.clone(),
            dst: LinearStore::address(),
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    pub async fn write(
        &self,
        key: String,
        value: StorageType,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<()> {
        let message = self.message(KafkaPayload::Cas {
            key,
            from: value.clone(),
            to: value,
            create_if_not_exists: Some(true),
        });
        let response = network.request(message).await.context("writing key")?;
        let Event::Message(message) = response else {
            return Err(anyhow!("write event did not return a message"));
        };

        let KafkaPayload::CasOk = message.body.payload else {
            return Err(anyhow!(
                "write response was not write_ok, found {:?}",
                message.body.payload
            ));
        };

        Ok(())
    }

    pub async fn read(
        &self,
        key: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Message<KafkaPayload>> {
        let message = self.message(KafkaPayload::Read { key: key.clone() });
        let response = network.request(message).await.context("reading key")?;
        let Event::Message(message) = response else {
            return Err(anyhow!("read event did not return a message"));
        };
        Ok(message)
    }

    pub async fn read_or_create(
        &self,
        key: String,
        network: &Network<KafkaPayload>,
        default: Option<StorageType>,
    ) -> anyhow::Result<StorageType> {
        let message = self.message(KafkaPayload::Read { key: key.clone() });
        let response = network.request(message).await.context("reading key")?;
        let Event::Message(message) = response else {
            return Err(anyhow!("read event did not return a message"));
        };

        if let KafkaPayload::Error { .. } = &message.body.payload {
            let value = default.unwrap_or(StorageType::Uint(0));
            self.write(key, value.clone(), network)
                .await
                .context("writing default value in read_or_create")?;
            Ok(value)
        } else if let KafkaPayload::ReadOk { value } = message.body.payload {
            Ok(value)
        } else {
            Err(anyhow!(
                "read response was not read_ok, found {:?}",
                message.body.payload
            ))
        }
    }

    async fn read_uint(
        &self,
        key: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<usize> {
        let stored = self
            .read_or_create(key, network, Some(StorageType::Uint(0)))
            .await
            .context("reading uint")?;
        let StorageType::Uint(value) = stored else {
            panic!("expected uint, found {:?}", stored);
        };

        Ok(value)
    }

    async fn read_array(
        &self,
        key: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Vec<Entry>> {
        let stored = self
            .read_or_create(key, network, Some(StorageType::Array(Vec::new())))
            .await
            .context("reading tuple array")?;
        let StorageType::Array(value) = stored else {
            panic!("expected tuple array, found {:?}", stored);
        };

        Ok(value)
    }

    pub async fn read_commit(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<usize> {
        self.read_uint(format!("{}/commit", topic), network).await
    }

    pub async fn write_commit(
        &self,
        topic: String,
        from: usize,
        to: usize,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<()> {
        let message = self.message(KafkaPayload::Cas {
            key: format!("{}/commit", topic),
            from: StorageType::Uint(from),
            to: StorageType::Uint(to),
            create_if_not_exists: Some(true),
        });
        let event = network.request(message).await.context("cas for commit")?;
        let Event::Message(message) = event else {
            return Err(anyhow!("event not a message in cas response"));
        };
        let KafkaPayload::CasOk = message.body.payload else {
            return Err(anyhow!("expected cas_ok, found {:?}", message.body.payload));
        };

        Ok(())
    }

    pub async fn read_log(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Vec<Entry>> {
        self.read_array(format!("{}/log", topic), network).await
    }

    async fn append_entry(
        &self,
        topic: String,
        entry: Entry,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Offset> {
        let key = format!("{}/log", topic.clone());
        loop {
            let mut current = self
                .read_array(key.clone(), network)
                .await
                .context("reading log")?;
            let offset = current.len();
            current.push(entry);
            let message = self.message(KafkaPayload::Cas {
                key: key.clone(),
                from: StorageType::Array(current[..current.len() - 1].to_vec()),
                to: StorageType::Array(current),
                create_if_not_exists: Some(true),
            });
            let response = network
                .request(message)
                .await
                .context("adding entry to log")?;
            let Event::Message(message) = response else {
                continue;
            };

            if let KafkaPayload::CasOk = message.body.payload {
                return Ok(offset);
            }
        }
    }

    async fn initialize_log(
        &self,
        topic: String,
        entry: Entry,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<usize> {
        self.write(
            format!("{}/log", topic),
            StorageType::Array(vec![entry]),
            network,
        )
        .await
        .context("initializing log")?;
        Ok(0)
    }

    pub async fn add_message(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
        entry: Entry,
    ) -> anyhow::Result<usize> {
        let message = self
            .read(format!("{}/log", topic), network)
            .await
            .context("reading log")?;

        if let KafkaPayload::Error { .. } = message.body.payload {
            if let Ok(offset) = self.initialize_log(topic.clone(), entry, network).await {
                return Ok(offset);
            }
        }

        self.append_entry(topic, entry, network)
            .await
            .context("appending entry to log")
    }
}

#[async_trait::async_trait]
impl fly_io::Node<KafkaPayload> for KafkaNode {
    fn from_init(
        init: fly_io::protocol::Init,
        _tx: std::sync::mpsc::Sender<fly_io::Event<KafkaPayload, ()>>,
        _network: &mut Network<KafkaPayload>,
    ) -> Self {
        Self::new(init.node_id)
    }

    async fn step(
        &mut self,
        event: Event<KafkaPayload, ()>,
        network: &mut Network<KafkaPayload, ()>,
    ) -> anyhow::Result<()> {
        match event {
            Event::Injected(..) => {}
            Event::Message(message) => {
                let mut reply = message.into_reply();
                if let Some(payload) = match reply.body.payload {
                    KafkaPayload::Send { key, msg } => {
                        let offset = self
                            .add_message(key, network, msg)
                            .await
                            .context("adding message")?;
                        Some(KafkaPayload::SendOk { offset })
                    }
                    KafkaPayload::SendOk { .. } => None,
                    KafkaPayload::Poll { offsets } => {
                        let mut result = HashMap::new();
                        for (topic, offset) in offsets.into_iter() {
                            let log = self
                                .read_log(topic.clone(), network)
                                .await
                                .context("reading log")?;

                            if log.len() <= offset {
                                continue;
                            }

                            let n_logs = std::cmp::min(3, log.len() - offset);
                            let selected = log[offset..offset + n_logs]
                                .iter()
                                .cloned()
                                .enumerate()
                                .map(|(i, entry)| (offset + i, entry))
                                .collect::<Vec<_>>();

                            eprintln!(
                                "topic={}, n_logs={}, start={}, offset={}, last_offset={:?}",
                                topic.clone(),
                                n_logs,
                                offset,
                                offset,
                                log.last()
                            );

                            result.insert(topic, selected);
                        }
                        Some(KafkaPayload::PollOk { msgs: result })
                    }
                    KafkaPayload::PollOk { .. } => None,
                    KafkaPayload::CommitOffsets { offsets } => {
                        for (topic, offset_to_commit) in offsets.into_iter() {
                            let current_commit = self
                                .read_commit(topic.clone(), network)
                                .await
                                .context("reading current commit")?;
                            if current_commit < offset_to_commit {
                                self.write_commit(topic, current_commit, offset_to_commit, network)
                                    .await
                                    .context("writing new commit")?;
                            }
                        }
                        Some(KafkaPayload::CommitOffsetsOk)
                    }
                    KafkaPayload::CommitOffsetsOk => None,
                    KafkaPayload::ListCommittedOffsets { keys } => {
                        let mut commits = HashMap::new();
                        for key in keys.into_iter() {
                            let commit = self
                                .read_commit(key.clone(), network)
                                .await
                                .context("reading current commit")?;
                            commits.insert(key, commit);
                        }
                        Some(KafkaPayload::ListCommittedOffsetsOk { offsets: commits })
                    }
                    KafkaPayload::ListCommittedOffsetsOk { .. } => None,
                    KafkaPayload::Read { .. } => None,
                    KafkaPayload::ReadOk { .. } => None,
                    KafkaPayload::Write { .. } => None,
                    KafkaPayload::WriteOk { .. } => None,
                    KafkaPayload::Cas { .. } => None,
                    KafkaPayload::CasOk { .. } => None,
                    KafkaPayload::Error { .. } => None,
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
    fly_io::server::Server::<KafkaPayload>::new().serve::<KafkaNode>()
}
