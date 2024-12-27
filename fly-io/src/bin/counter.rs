use anyhow::{anyhow, Context};
use fly_io::{
    network::Network,
    service::{SequentialStore, StoragePayload},
    Event,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum CounterPayload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    CasOk,
    WriteOk,
    Error { code: usize, text: String },
}

#[derive(Debug, Clone)]
struct CounterNode {
    node_id: String,
}

impl CounterNode {
    pub async fn read_current_value(
        &self,
        network: &mut Network<CounterPayload>,
    ) -> anyhow::Result<usize> {
        let message = SequentialStore::read_current_value(self.node_id.clone(), network)
            .await
            .context("reading current value from add")?;

        let CounterPayload::ReadOk { value } = message.body.payload else {
            return Err(anyhow!("value request message response was not ReadOk"));
        };

        Ok(value)
    }

    pub async fn add_to_current_value(
        &self,
        network: &mut Network<CounterPayload>,
        delta: usize,
    ) -> anyhow::Result<usize> {
        let mut new_value: usize;
        loop {
            let current_value = self
                .read_current_value(network)
                .await
                .context("reading current value")?;

            new_value = current_value + delta;
            let event = network
                .request(SequentialStore::construct_message::<StoragePayload>(
                    self.node_id.clone(),
                    SequentialStore::compare_and_store(current_value, new_value),
                ))
                .await
                .context("adding delta")?;

            let Event::Message(message) = event else {
                continue;
            };

            if let CounterPayload::CasOk = message.body.payload {
                return Ok::<_, anyhow::Error>(new_value);
            };
        }
    }
}

#[async_trait::async_trait]
impl fly_io::Node<CounterPayload> for CounterNode {
    fn from_init(
        init: fly_io::protocol::Init,
        _tx: std::sync::mpsc::Sender<fly_io::Event<CounterPayload>>,
        network: &mut Network<CounterPayload>,
    ) -> Self {
        let result = Self {
            node_id: init.node_id,
        };

        SequentialStore::initialize(result.node_id.clone(), network)
            .expect("failed to initialize storage");

        result
    }

    async fn step(
        &mut self,
        event: fly_io::Event<CounterPayload>,
        network: &mut Network<CounterPayload>,
    ) -> anyhow::Result<()> {
        match event {
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
                            .read_current_value(network)
                            .await
                            .context("reading current value from read")?;

                        reply.body.payload = CounterPayload::ReadOk { value };
                        network.send(reply).context("sending read reply")?;
                    }
                    CounterPayload::AddOk => {}
                    CounterPayload::ReadOk { .. } => {}
                    CounterPayload::CasOk => {}
                    CounterPayload::WriteOk => {}
                    CounterPayload::Error { .. } => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::server::Server::<CounterPayload>::new().serve::<CounterNode>()
}
