use std::io::{StdoutLock, Write};

use anyhow::Context;
use gjengset_fly_io::{main_loop, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

pub struct UniqueIdNode {
    node: String,
    id: usize,
}

impl Node<(), Payload> for UniqueIdNode {
    fn from_init(_state: (), init: gjengset_fly_io::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIdNode {
            node: init.node_id,
            id: 1,
        })
    }

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.id);
                reply.body.payload = Payload::GenerateOk { id: guid };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("write trailing newline")?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueIdNode, _>(())
}
