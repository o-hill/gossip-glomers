use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Context;
use fly_io::{network::Network, Event};
use serde::{Deserialize, Serialize};

type Topic = String;
type Offset = usize;
type Log = (Offset, usize);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KafkaPayload {
    // RPC
    Send { key: Topic, msg: usize },
    SendOk { offset: Offset },
    Poll { offsets: HashMap<Topic, Offset> },
    PollOk { msgs: HashMap<Topic, Vec<Log>> },
    CommitOffsets { offsets: HashMap<Topic, Offset> },
    CommitOffsetsOk,
    ListCommittedOffsets { keys: Vec<Topic> },
    ListCommittedOffsetsOk { offsets: HashMap<Topic, Offset> },
}

#[derive(Clone)]
struct KafkaNode {
    node_id: String,
    offset: Arc<RwLock<Offset>>,
    logs: Arc<RwLock<HashMap<Topic, Vec<Log>>>>,
    commits: Arc<RwLock<HashMap<Topic, Offset>>>,
}

impl KafkaNode {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            offset: Arc::new(RwLock::new(0)),
            logs: Arc::new(RwLock::new(HashMap::new())),
            commits: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl fly_io::Node<KafkaPayload> for KafkaNode {
    fn from_init(
        init: fly_io::protocol::Init,
        _tx: std::sync::mpsc::Sender<fly_io::Event<KafkaPayload, ()>>,
        _network: &mut Network<KafkaPayload, ()>,
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
                        let mut offset = self.offset.write().unwrap();
                        let mut logs = self.logs.write().unwrap();
                        let message_offset = *offset;
                        *offset += 1;
                        logs.entry(key).or_default().push((message_offset, msg));
                        Some(KafkaPayload::SendOk {
                            offset: message_offset,
                        })
                    }
                    KafkaPayload::SendOk { .. } => None,
                    KafkaPayload::Poll { offsets } => {
                        let mut logs = self.logs.write().unwrap();
                        let mut result = HashMap::new();
                        for (topic, offset) in offsets.into_iter() {
                            let available = logs.entry(topic.clone()).or_default();
                            let start = available
                                .binary_search_by(|l| l.0.cmp(&offset))
                                .or_else(Ok::<usize, anyhow::Error>)
                                .unwrap();

                            let n_logs = std::cmp::min(3, available.len() - start);
                            eprintln!(
                                "topic={}, n_logs={}, start={}, offset={}, last_offset={:?}",
                                topic.clone(),
                                n_logs,
                                start,
                                offset,
                                available.last()
                            );

                            let selected = available[start..start + n_logs].to_vec();
                            result.insert(topic, selected);
                        }
                        Some(KafkaPayload::PollOk { msgs: result })
                    }
                    KafkaPayload::PollOk { .. } => None,
                    KafkaPayload::CommitOffsets { offsets } => {
                        let mut commits = self.commits.write().unwrap();
                        let mut result = HashMap::new();
                        for (topic, offset_to_commit) in offsets.into_iter() {
                            let current = commits.entry(topic.clone()).or_insert(0);
                            let new = std::cmp::max(*current, offset_to_commit);
                            *current = new;
                            result.insert(topic, new);
                        }
                        Some(KafkaPayload::CommitOffsetsOk)
                    }
                    KafkaPayload::CommitOffsetsOk => None,
                    KafkaPayload::ListCommittedOffsets { keys } => {
                        let commits = self.commits.read().unwrap();
                        let offsets = commits
                            .iter()
                            .filter_map(|(t, o)| {
                                if keys.contains(t) {
                                    Some((t.clone(), *o))
                                } else {
                                    None
                                }
                            })
                            .collect::<HashMap<_, _>>();
                        Some(KafkaPayload::ListCommittedOffsetsOk { offsets })
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
    fly_io::server::Server::<KafkaPayload>::new().serve::<KafkaNode>()
}
