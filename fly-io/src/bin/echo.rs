use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Clone, Debug)]
pub struct EchoNode {}

#[async_trait::async_trait]
impl fly_io::Node<EchoPayload> for EchoNode {
    fn from_init(
        _init: fly_io::protocol::Init,
        _tx: std::sync::mpsc::Sender<fly_io::Event<EchoPayload>>,
        _network: &mut fly_io::network::Network<EchoPayload>,
    ) -> Self {
        EchoNode {}
    }

    async fn step(
        &mut self,
        input: fly_io::Event<EchoPayload>,
        network: &mut fly_io::network::Network<EchoPayload>,
    ) -> anyhow::Result<()> {
        let fly_io::Event::Message(input) = input else {
            panic!("Echo node received a non-message event");
        };

        let mut reply = input.into_reply();
        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                network.send(reply).context("sending echo_ok message")?;
            }
            EchoPayload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::server::Server::<EchoPayload, ()>::new().serve::<EchoNode>()
}
