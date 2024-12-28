use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::Context;
use fly_io::{network::Network, Body, Event, Message};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug)]
struct BroadcastNode {
    node_id: String,
    messages: Arc<RwLock<HashSet<usize>>>,
    neighborhood: Vec<String>,
    known: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
}

#[async_trait::async_trait]
impl fly_io::Node<BroadcastPayload, InjectedPayload> for BroadcastNode {
    fn from_init(
        init: fly_io::protocol::Init,
        network: &fly_io::network::Network<InjectedPayload>,
    ) -> Self {
        let net = network.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(450));
            if net.inject(InjectedPayload::Gossip).is_err() {
                break;
            }
        });

        let mut nodes = init.node_ids.clone();
        nodes.shuffle(&mut rand::thread_rng());
        let neighborhood_size = (nodes.len() / 2) + 1;
        let neighborhood = nodes[..neighborhood_size].to_vec();

        Self {
            node_id: init.node_id,
            messages: Arc::new(RwLock::new(HashSet::new())),
            neighborhood,
            known: Arc::new(RwLock::new(
                init.node_ids
                    .into_iter()
                    .map(|id| (id, HashSet::new()))
                    .collect(),
            )),
        }
    }

    async fn step(
        &mut self,
        input: fly_io::Event<BroadcastPayload, InjectedPayload>,
        network: &Network<InjectedPayload>,
    ) -> anyhow::Result<()> {
        match input {
            Event::Storage(_) => {}
            fly_io::Event::Injected(event) => match event {
                InjectedPayload::Gossip => {
                    for neighbor in &self.neighborhood {
                        let known = self.known.read().unwrap();
                        let messages = self.messages.read().unwrap();
                        let known_to_neighbor = &known[neighbor];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = messages
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
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip { seen: notify_of },
                            },
                        };
                        network
                            .send(message)
                            .context(format!("gossip to {}", neighbor))?;
                    }
                }
            },
            fly_io::Event::Message(input) => {
                let mut reply = input.into_reply();
                match reply.body.payload {
                    BroadcastPayload::Gossip { seen } => {
                        let mut known = self.known.write().unwrap();
                        let mut messages = self.messages.write().unwrap();
                        known
                            .get_mut(&reply.dst)
                            .unwrap_or_else(|| panic!("sender {} not in known nodes", reply.dst))
                            .extend(seen.clone());

                        messages.extend(seen);
                    }
                    BroadcastPayload::Broadcast { message } => {
                        let mut messages = self.messages.write().unwrap();
                        messages.insert(message);
                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        network.send(reply).context("sending broadcast reply")?;
                    }
                    BroadcastPayload::Read => {
                        let messages = self.messages.read().unwrap().clone();
                        reply.body.payload = BroadcastPayload::ReadOk { messages };
                        network.send(reply).context("sending read reply")?;
                    }
                    BroadcastPayload::Topology { topology: _ } => {
                        // self.neighborhood = topology
                        //     .remove(&self.node_id)
                        //     .unwrap_or_else(|| panic!("node not in topology {}", self.node_id));

                        reply.body.payload = BroadcastPayload::TopologyOk;
                        network.send(reply).context("sending topology reply")?;
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
    fly_io::server::Server::<InjectedPayload>::new().serve::<BroadcastNode, BroadcastPayload>()
}
