use std::time::Duration;

use anyhow::{anyhow, Context};
use fly_io::{
    network::Network,
    protocol::{Body, Message},
    service::{SequentialStore, StoragePayload},
    Event,
};
use serde::{Deserialize, Serialize};

pub struct SequentialStore;
impl SequentialStore {
    pub fn address() -> String {
        "seq-kv".to_string()
    }

    pub fn key() -> String {
        "value".to_string()
    }

    fn read() -> StoragePayload {
        StoragePayload::Read { key: Self::key() }
    }

    fn compare_and_store(from: usize, to: usize) -> StoragePayload {
        StoragePayload::Cas {
            key: Self::key(),
            from,
            to,
            create_if_not_exists: Some(true),
        }
    }

    fn write(value: usize) -> StoragePayload {
        StoragePayload::Write {
            key: Self::key(),
            value,
        }
    }

    pub fn initialize<P, IP>(node_id: String, network: &mut Network<P, IP>) -> anyhow::Result<()>
    where
        P: Serialize + Clone + Send + DeserializeOwned + 'static,
        IP: Clone + Send + 'static,
    {
        network
            .send(Self::construct_message(node_id, Self::write(0)))
            .context("initializing storage value")
    }

    pub fn construct_message<P>(node_id: String, payload: P) -> Message<P> {
        Message {
            src: node_id,
            dst: Self::address(),
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    pub async fn read_current_value<P, IP>(
        node_id: String,
        network: &mut Network<CounterPayload, InjectedPayload>,
    ) -> anyhow::Result<usize> {
        let event = network
            .request(Self::construct_message(node_id, SequentialStore::read()))
            .await
            .context("fetching current value")?;

        let Event::Message(message) = event else {
            return Err(anyhow!("value request event response was not a message"));
        };

        let CounterPayload::ReadOk { value } = message.body.payload else {
            return Err(anyhow!("value request message response was not ReadOk"));
        };

        Ok(value)
    }

    pub async fn add_to_current_value<P, IP>(
        node_id: String,
        network: &mut Network<P, IP>,
        delta: usize,
    ) -> anyhow::Result<usize>
    where
        P: Serialize + Clone + Send + DeserializeOwned + 'static,
        IP: Clone + Send + 'static,
    {
        let mut new_value: usize;
        let mut request_count = 0;
        loop {
            let current_value = Self::read_current_value(node_id.clone(), network)
                .await
                .context("reading current value from add")?;

            new_value = current_value + delta;
            let event = network
                .request(Self::construct_message(
                    SequentialStore::address(),
                    SequentialStore::compare_and_store(current_value, new_value),
                ))
                .await
                .context("adding delta")?;

            request_count += 1;
            dbg!(request_count);

            let Event::Message(message) = event else {
                continue;
            };

            if let CounterPayload::CasOk = message.body.payload {
                break;
            };
        }

        Ok(new_value)
    }
}

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

#[derive(Clone)]
enum InjectedPayload {
    ReadFromStore,
}

#[derive(Debug, Clone)]
struct CounterNode {
    node_id: String,
}

impl CounterNode {}

#[async_trait::async_trait]
impl fly_io::Node<CounterPayload, InjectedPayload> for CounterNode {
    fn from_init(
        init: fly_io::protocol::Init,
        tx: std::sync::mpsc::Sender<fly_io::Event<CounterPayload, InjectedPayload>>,
        network: &mut Network<CounterPayload, InjectedPayload>,
    ) -> Self {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if tx
                .send(fly_io::Event::Injected(InjectedPayload::ReadFromStore))
                .is_err()
            {
                break;
            }
        });

        let result = Self {
            node_id: init.node_id,
        };

        SequentialStore::initialize(result.node_id.clone(), network)
            .expect("failed to initialize storage");

        result
    }

    async fn step(
        &mut self,
        event: fly_io::Event<CounterPayload, InjectedPayload>,
        network: &mut Network<CounterPayload, InjectedPayload>,
    ) -> anyhow::Result<()> {
        match event {
            fly_io::Event::Injected(payload) => match payload {
                InjectedPayload::ReadFromStore => {}
            },
            fly_io::Event::Message(message) => {
                let mut reply = message.into_reply();
                match reply.body.payload {
                    CounterPayload::Add { delta } => {
                        let _ = SequentialStore::add_to_current_value(
                            self.node_id.clone(),
                            network,
                            delta,
                        )
                        .await
                        .context("adding delta to store")?;

                        reply.body.payload = CounterPayload::AddOk;
                        network.send(reply).context("sending add_ok reply")?;
                    }
                    CounterPayload::Read => {
                        let value =
                            SequentialStore::read_current_value(self.node_id.clone(), network)
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
    fly_io::server::Server::<CounterPayload, InjectedPayload>::new().serve::<CounterNode>()
}
