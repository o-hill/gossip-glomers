use anyhow::Context;
use fly_io::{network::Network, service::SequentialStore};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum CounterPayload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
}

#[derive(Debug, Clone)]
struct CounterNode {
    node_id: String,
    storage: SequentialStore,
}

impl CounterNode {
    fn storage_key() -> String {
        "value".to_string()
    }

    pub async fn add_to_current_value(
        &self,
        network: &mut Network,
        delta: usize,
    ) -> anyhow::Result<usize> {
        let mut new_value: usize;
        loop {
            let current_value = self
                .storage
                .read(Self::storage_key(), network)
                .await
                .context("reading value from storage")?;

            new_value = current_value + delta;
            if self
                .storage
                .compare_and_store(Self::storage_key(), current_value, new_value, network)
                .await
                .context("adding delta")
                .is_ok()
            {
                return Ok(new_value);
            };
        }
    }
}

#[async_trait::async_trait]
impl fly_io::Node<CounterPayload> for CounterNode {
    fn from_init(init: fly_io::protocol::Init, network: &mut Network) -> Self {
        let result = Self {
            node_id: init.node_id.clone(),
            storage: SequentialStore::new(init.node_id),
        };

        result
            .storage
            .write(Self::storage_key(), 0, network)
            .expect("failed to initialize storage");

        result
    }

    async fn step(
        &mut self,
        event: fly_io::Event<CounterPayload>,
        network: &mut Network,
    ) -> anyhow::Result<()> {
        match event {
            fly_io::Event::Storage(_) => {}
            fly_io::Event::Injected(_) => {}
            fly_io::Event::Message(message) => {
                let mut reply = message.into_reply();
                match reply.body.payload {
                    CounterPayload::Add { delta } => {
                        let _ = self
                            .add_to_current_value(network, delta)
                            .await
                            .context("adding delta to store")?;

                        reply.body.payload = CounterPayload::AddOk;
                        network.send(reply).context("sending add_ok reply")?;
                    }
                    CounterPayload::Read => {
                        let value = self
                            .storage
                            .read(Self::storage_key(), network)
                            .await
                            .context("reading value from storage")?;

                        reply.body.payload = CounterPayload::ReadOk { value };
                        network.send(reply).context("sending read reply")?;
                    }
                    CounterPayload::AddOk => {}
                    CounterPayload::ReadOk { .. } => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::server::Server::new().serve::<CounterNode, CounterPayload>()
}
