use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protocol::{InitPayload, Message};

pub struct Server<P, IP>
where
    P: Clone,
    IP: Clone,
{
    network: crate::network::Network<P, IP>,
}

impl<P, IP> Server<P, IP>
where
    P: Send + Clone + Serialize + DeserializeOwned + 'static,
    IP: Send + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            network: crate::network::Network::new(),
        }
    }

    fn construct_node<NODE>(&self, init_msg: Message<InitPayload>) -> anyhow::Result<NODE>
    where
        NODE: crate::Node<P, IP>,
    {
        let InitPayload::Init(init) = init_msg.body.payload.clone() else {
            panic!("first message was not an init");
        };

        let node = NODE::from_init(init, self.network.tx.clone(), &mut self.network.clone());

        let mut reply = init_msg.into_reply();
        reply.body.payload = InitPayload::InitOk;
        self.network.send(reply).context("sending init_ok")?;

        Ok(node)
    }

    #[tokio::main]
    pub async fn serve<NODE>(&mut self) -> anyhow::Result<()>
    where
        NODE: crate::Node<P, IP> + Send + Clone + 'static,
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
        while let Some(event) = self.network.recv().await {
            let mut network = self.network.clone();
            let mut n = node.clone();
            js.spawn(async move { n.step(event, &mut network).await });
        }
        let node_task = tokio::task::spawn(async move {});

        jh.join()
            .expect("stdin thread panicked")
            .context("stdin thread panicked")?;

        node_task.await.expect("node thread crashed");

        Ok(())
    }
}
