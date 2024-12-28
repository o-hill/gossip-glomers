use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Context};
use fly_io::{
    network::Network,
    protocol::{Body, Message},
    service::{LinearStore, SequentialStore},
    Event,
};
use serde::{Deserialize, Serialize};

type Topic = String;
type Offset = usize;
type Entry = usize;

struct StorageKey {}
impl StorageKey {
    fn log(topic: &str, offset: Offset) -> String {
        format!("{}/log/{}", topic, offset)
    }

    fn offset(topic: &str) -> String {
        format!("{}/offset", topic)
    }

    fn commit(topic: &str) -> String {
        format!("{}/commit", topic)
    }
}

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
        value: Entry,
    },
    Write {
        key: String,
        value: Entry,
    },
    WriteOk,
    Cas {
        key: String,
        from: Entry,
        to: Entry,
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
    class_leader: usize,
    n_classes: usize,
    entries: HashMap<Topic, HashMap<Offset, Entry>>,
    pub cas_failures: Arc<RwLock<usize>>,
    pub total_offset_writes: Arc<RwLock<usize>>,
}

impl KafkaNode {
    pub fn new(node_id: String, class_leader: usize, n_classes: usize) -> Self {
        Self {
            node_id,
            class_leader,
            n_classes,
            entries: HashMap::new(),
            cas_failures: Arc::new(RwLock::new(0)),
            total_offset_writes: Arc::new(RwLock::new(0)),
        }
    }

    fn message<P>(&self, dst: String, payload: P) -> Message<P> {
        Message {
            src: self.node_id.clone(),
            dst,
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
        from: Option<Entry>,
        to: Entry,
        dst: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<()> {
        let message = self.message(
            dst,
            KafkaPayload::Cas {
                key,
                from: from.unwrap_or(to),
                to,
                create_if_not_exists: Some(true),
            },
        );
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

    pub async fn read<T>(
        &self,
        key: String,
        dst: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<T> {
        let message = self.message(dst, KafkaPayload::Read { key: key.clone() });
        let response = network.request(message).await.context("reading key")?;
        let Event::Message(message) = response else {
            return Err(anyhow!("read event did not return a message"));
        };

        match message.body.payload {
            KafkaPayload::Error { .. } => Err(anyhow!("error reading key")),
            KafkaPayload::ReadOk { value } => 
        } 
        Ok(message)
    }

    pub async fn read_or_create(
        &self,
        key: String,
        dst: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Entry> {
        let message = self.message(dst.clone(), KafkaPayload::Read { key: key.clone() });
        let response = network.request(message).await.context("reading key")?;
        let Event::Message(message) = response else {
            return Err(anyhow!("read event did not return a message"));
        };

        if let KafkaPayload::Error { .. } = &message.body.payload {
            let value = 0;
            self.write(key, None, value, dst, network)
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

    pub async fn read_commit(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<usize> {
        self.read_or_create(
            StorageKey::commit(&topic),
            SequentialStore::address(),
            network,
        )
        .await
    }

    pub async fn read_offset(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Option<Offset>> {
        let message = self
            .read(
                StorageKey::offset(&topic),
                SequentialStore::address(),
                network,
            )
            .await
            .context("reading current offset")?;

        if let KafkaPayload::ReadOk { value } = &message.body.payload {
            return Ok(Some(*value));
        }

        Ok(None)
    }

    pub async fn read_or_create_offset(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Offset> {
        self.read_or_create(
            StorageKey::offset(&topic),
            SequentialStore::address(),
            network,
        )
        .await
    }

    /// The stored offset is one greater than the latest entry.
    pub async fn get_next_offset(
        &self,
        topic: String,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Offset> {
        *self.total_offset_writes.write().unwrap() += 1;
        loop {
            let current = self
                .read_or_create_offset(topic.clone(), network)
                .await
                .context("reading current offset")?;
            let next = current + 1;
            if self
                .write(
                    StorageKey::offset(&topic),
                    Some(current),
                    next,
                    SequentialStore::address(),
                    network,
                )
                .await
                .is_ok()
            {
                return Ok(current);
            }

            *self.cas_failures.write().unwrap() += 1;
        }
    }

    pub async fn write_commit(
        &self,
        topic: String,
        from: usize,
        to: usize,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<()> {
        let message = self.message(
            SequentialStore::address(),
            KafkaPayload::Cas {
                key: StorageKey::commit(&topic),
                from,
                to,
                create_if_not_exists: Some(true),
            },
        );
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
        &mut self,
        topic: String,
        network: &Network<KafkaPayload>,
        offset: Offset,
    ) -> anyhow::Result<Option<Entry>> {
        let cached = self.entries.entry(topic.clone()).or_default().get(&offset);
        if let Some(entry) = cached {
            return Ok(Some(*entry));
        }

        let response = self
            .read(
                StorageKey::log(&topic, offset),
                LinearStore::address(),
                network,
            )
            .await
            .context("reading log entry")?;

        match &response.body.payload {
            KafkaPayload::ReadOk { value } => {
                self.entries
                    .entry(topic)
                    .or_default()
                    .insert(offset, *value);

                Ok(Some(*value))
            }
            KafkaPayload::Error { .. } => Ok(None),
            _ => panic!("expected read_ok, found {:?}", response.body.payload),
        }
    }

    async fn append_entry(
        &mut self,
        topic: String,
        entry: Entry,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Offset> {
        let offset = self
            .get_next_offset(topic.clone(), network)
            .await
            .context("getting next available offset")?;

        let key = StorageKey::log(&topic, offset);
        let message = self.message(
            LinearStore::address(),
            KafkaPayload::Write { key, value: entry },
        );
        let response = network
            .request(message)
            .await
            .context("adding entry to log")?;

        let Event::Message(message) = response else {
            panic!("non-message event received in append");
        };

        self.entries.entry(topic).or_default().insert(offset, entry);

        if let KafkaPayload::WriteOk = message.body.payload {
            return Ok(offset);
        }

        Err(anyhow!("appending failed"))
    }

    fn get_class(&self, topic: &str) -> usize {
        topic.parse::<usize>().expect("topic was not uint") % self.n_classes
    }

    fn is_in_class(&self, topic: &str) -> bool {
        let class = self.get_class(topic);
        class == self.class_leader
    }

    async fn send_to_class_leader(
        &mut self,
        key: String,
        msg: usize,
        network: &Network<KafkaPayload>,
    ) -> anyhow::Result<Offset> {
        let class = self.get_class(&key);
        let dst = format!("n{}", class);
        let message = self.message(
            dst,
            KafkaPayload::Send {
                key: key.clone(),
                msg,
            },
        );
        let response = network
            .request(message)
            .await
            .context("requesting response from class leader")?;

        let Event::Message(message) = response else {
            return Err(anyhow!("class leader returned a bad event"));
        };

        if let KafkaPayload::SendOk { offset } = &message.body.payload {
            self.entries.entry(key).or_default().insert(*offset, msg);
            return Ok(*offset);
        };

        panic!("expected send_ok, found {:?}", message.body.payload)
    }
}

impl Drop for KafkaNode {
    fn drop(&mut self) {
        let cas_failures = *self.cas_failures.read().unwrap();
        let total_appends = *self.total_offset_writes.read().unwrap();
        eprintln!(
            "CAS FAILURES: {} / TOTAL OFFSET WRITES: {}",
            cas_failures, total_appends
        );
    }
}

#[async_trait::async_trait]
impl fly_io::Node<KafkaPayload> for KafkaNode {
    fn from_init(
        init: fly_io::protocol::Init,
        _tx: std::sync::mpsc::Sender<fly_io::Event<KafkaPayload, ()>>,
        _network: &mut Network<KafkaPayload>,
    ) -> Self {
        let class_leader = init
            .node_ids
            .iter()
            .position(|nid| *nid == init.node_id)
            .unwrap();
        Self::new(init.node_id, class_leader, init.node_ids.len())
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
                        let offset = if self.is_in_class(&key) {
                            self.append_entry(key, msg, network)
                                .await
                                .context("adding message")?
                        } else {
                            self.send_to_class_leader(key, msg, network)
                                .await
                                .context("sending send request to class leader")?
                        };

                        Some(KafkaPayload::SendOk { offset })
                    }
                    KafkaPayload::SendOk { .. } => None,
                    KafkaPayload::Poll { offsets } => {
                        let mut result = HashMap::new();
                        for (topic, requested_offset) in offsets.into_iter() {
                            let current_offset = self
                                .read_offset(topic.clone(), network)
                                .await
                                .context("reading current offset")?;

                            if current_offset.is_none()
                                || current_offset.unwrap() <= requested_offset
                            {
                                continue;
                            }

                            let mut offset = requested_offset;
                            let mut entry = None;
                            while entry.is_none() {
                                entry = self
                                    .read_log(topic.clone(), network, offset)
                                    .await
                                    .context("reading offset from log")?;

                                // Maybe there's a better way to do this? Don't increment when entry
                                // is valid because we need the correct offset value in the response.
                                if entry.is_none() {
                                    offset += 1;
                                }
                            }

                            eprintln!(
                                "topic={}, n_logs={}, start={}, offset={}, last_offset={:?}",
                                topic.clone(),
                                1,
                                requested_offset,
                                offset,
                                offset // current_offset
                            );

                            result.insert(topic, vec![(offset, entry.unwrap())]);
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
