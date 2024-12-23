use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

pub struct EchoNode {
    id: usize,
}

impl fly_io::Node<EchoPayload> for EchoNode {
    fn from_init(_init: fly_io::Init) -> Self {
        EchoNode { id: 1 }
    }

    fn step(
        &mut self,
        input: fly_io::Message<EchoPayload>,
        mut output: &mut impl std::io::Write,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                reply.send(&mut output).context("sending echo_ok message")?;
            }
            EchoPayload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::run::<EchoPayload, EchoNode>()
}
