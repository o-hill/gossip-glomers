use std::fmt::Debug;

use anyhow::Context;
use serde::de::DeserializeOwned;

use crate::protocol::InitPayload;
use crate::Message;

pub struct Server<IP = ()>
where
    IP: Clone,
{
    network: crate::network::Network<IP>,
}

impl<IP> Default for Server<IP>
where
    IP: Debug + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            network: crate::network::Network::new(),
        }
    }
}

impl<IP> Server<IP>
where
    IP: Debug + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    fn construct_node<NODE, PAYLOAD>(&self, init_msg: Message<InitPayload>) -> anyhow::Result<NODE>
    where
        NODE: crate::Node<PAYLOAD, IP>,
    {
        let InitPayload::Init(init) = init_msg.body.payload.clone() else {
            panic!("first message was not an init");
        };

        let node = NODE::from_init(init, &self.network.clone());

        let mut reply = init_msg.into_reply();
        reply.body.payload = InitPayload::InitOk;
        self.network.send(reply).context("sending init_ok")?;

        Ok(node)
    }

    #[tokio::main]
    pub async fn serve<NODE, PAYLOAD>(&mut self) -> anyhow::Result<()>
    where
        PAYLOAD: DeserializeOwned + Send + 'static,
        NODE: crate::Node<PAYLOAD, IP> + Send + Clone + 'static,
    {
        let init_msg = self
            .network
            .read::<InitPayload>()
            .context("reading init message")?;
        let node: NODE = self
            .construct_node(init_msg)
            .context("constructing node from init message")?;

        let jh = self.network.start_read_thread();

        let mut js = tokio::task::JoinSet::new();
        while let Some(event) = self.network.recv::<PAYLOAD>().await {
            let network = self.network.clone();
            let mut n = node.clone();
            js.spawn(async move { n.step(event, &network).await });
        }

        jh.join()
            .expect("stdin thread panicked")
            .context("stdin thread panicked")?;

        js.join_all().await;

        Ok(())
    }
}
