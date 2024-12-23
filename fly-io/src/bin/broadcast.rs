use std::collections::{HashMap, HashSet};

use anyhow::Context;
use serde::{Deserialize, Serialize};

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
}

impl fly_io::Node<BroadcastPayload> for BroadcastNode {
    fn from_init(init: fly_io::Init) -> Self {
        Self {
            node_id: init.node_id,
            id: 1,
            messages: HashSet::new(),
            neighborhood: Vec::new(),
        }
    }

    fn step(
        &mut self,
        input: fly_io::Message<BroadcastPayload>,
        mut output: &mut impl std::io::Write,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            BroadcastPayload::Broadcast { message } => {
                self.messages.insert(message);
                reply.body.payload = BroadcastPayload::BroadcastOk;
                reply.send(&mut output).context("sending broadcast reply")?;

                for neighbor in &self.neighborhood {
                    let msg = fly_io::Message {
                        src: self.node_id.clone(),
                        dst: neighbor.clone(),
                        body: fly_io::Body {
                            id: None,
                            in_reply_to: None,
                            payload: BroadcastPayload::Broadcast { message },
                        },
                    };
                    msg.send(&mut output)
                        .context("sending broadcast to neighbor node")?;
                }
            }
            BroadcastPayload::Read => {
                reply.body.payload = BroadcastPayload::ReadOk {
                    messages: self.messages.clone(),
                };
                reply.send(&mut output).context("sending read reply")?;
            }
            BroadcastPayload::Topology { mut topology } => {
                self.neighborhood = topology
                    .remove(&self.node_id)
                    .context("node not in topology")?;
                reply.body.payload = BroadcastPayload::TopologyOk;
                reply.send(&mut output).context("sending topology reply")?;
            }
            BroadcastPayload::BroadcastOk => {}
            BroadcastPayload::ReadOk { messages: _ } => {}
            BroadcastPayload::TopologyOk => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::run::<BroadcastPayload, BroadcastNode>()
}
