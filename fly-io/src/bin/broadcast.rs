use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::Context;
use fly_io::Message;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

enum InjectedPayload {
    Gossip,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: usize,
    },
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    Gossip {
        seen: HashSet<usize>,
    },
    BroadcastOk,
    ReadOk {
        messages: HashSet<usize>,
    },
    TopologyOk,
}

struct BroadcastNode {
    node_id: String,
    id: usize,
    messages: HashSet<usize>,
    neighborhood: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl fly_io::Node<BroadcastPayload, InjectedPayload> for BroadcastNode {
    fn from_init(
        init: fly_io::Init,
        tx: std::sync::mpsc::Sender<fly_io::Event<BroadcastPayload, InjectedPayload>>,
    ) -> Self {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(450));
            if tx
                .send(fly_io::Event::Injected(InjectedPayload::Gossip))
                .is_err()
            {
                break;
            }
        });

        let mut nodes = init.node_ids.clone();
        nodes.shuffle(&mut rand::thread_rng());
        let neighborhood_size = (nodes.len() / 2) + 1;
        let neighborhood = nodes[..neighborhood_size].to_vec();

        Self {
            node_id: init.node_id,
            id: 1,
            messages: HashSet::new(),
            neighborhood,
            known: init
                .node_ids
                .into_iter()
                .map(|id| (id, HashSet::new()))
                .collect(),
        }
    }

    fn step(
        &mut self,
        input: fly_io::Event<BroadcastPayload, InjectedPayload>,
        mut output: &mut impl std::io::Write,
    ) -> anyhow::Result<()> {
        match input {
            fly_io::Event::Injected(event) => match event {
                InjectedPayload::Gossip => {
                    for neighbor in &self.neighborhood {
                        let known_to_neighbor = &self.known[neighbor];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| known_to_neighbor.contains(m));

                        notify_of.extend(already_known.iter().enumerate().filter_map(|(i, m)| {
                            if i < 10 {
                                Some(m)
                            } else {
                                None
                            }
                        }));

                        let message = Message {
                            src: self.node_id.clone(),
                            dst: neighbor.clone(),
                            body: fly_io::Body {
                                id: None,
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip { seen: notify_of },
                            },
                        };
                        message
                            .send(&mut output)
                            .context(format!("gossip to {}", neighbor))?;
                    }
                }
            },
            fly_io::Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    BroadcastPayload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .unwrap_or_else(|| panic!("sender {} not in known nodes", reply.dst))
                            .extend(seen.clone());

                        self.messages.extend(seen);
                    }
                    BroadcastPayload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        reply.send(&mut output).context("sending broadcast reply")?;
                    }
                    BroadcastPayload::Read => {
                        reply.body.payload = BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(&mut output).context("sending read reply")?;
                    }
                    BroadcastPayload::Topology { mut topology } => {
                        // self.neighborhood = topology
                        //     .remove(&self.node_id)
                        //     .unwrap_or_else(|| panic!("node not in topology {}", self.node_id));

                        reply.body.payload = BroadcastPayload::TopologyOk;
                        reply.send(&mut output).context("sending topology reply")?;
                    }
                    BroadcastPayload::BroadcastOk => {}
                    BroadcastPayload::ReadOk { .. } => {}
                    BroadcastPayload::TopologyOk => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::run::<BroadcastPayload, InjectedPayload, BroadcastNode>()
}
