use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{anyhow, Context};
use fly_io::{
    protocol::{Body, Message},
    Event,
};
use serde::{Deserialize, Serialize};

pub struct SequentialStore;
impl SequentialStore {
    fn address() -> String {
        "seq-kv".to_string()
    }

    fn key() -> String {
        "value".to_string()
    }

    fn read_current_value() -> SequentialStorePayload {
        SequentialStorePayload::Read { key: Self::key() }
    }

    fn compare_and_store(from: usize, to: usize) -> SequentialStorePayload {
        SequentialStorePayload::Cas {
            key: Self::key(),
            from,
            to,
            create_if_not_exists: Some(true),
        }
    }

    fn write(value: usize) -> SequentialStorePayload {
        SequentialStorePayload::Write {
            key: Self::key(),
            value,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum SequentialStorePayload {
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: Option<bool>,
    },
    CasOk,
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
}

#[derive(Clone)]
enum InjectedPayload {
    ReadFromStore,
}

#[derive(Debug, Clone)]
struct CounterNode {
    node_id: String,
    value: Arc<RwLock<usize>>,
}

impl CounterNode {
    fn construct_message<P>(&self, dst: String, payload: P) -> Message<P> {
        Message {
            src: self.node_id.clone(),
            dst,
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    async fn read_current_value(
        &self,
        network: &mut fly_io::network::Network<CounterPayload, InjectedPayload>,
    ) -> anyhow::Result<usize> {
        let event = network
            .request(self.construct_message(
                SequentialStore::address(),
                SequentialStore::read_current_value(),
            ))
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
}

#[async_trait::async_trait]
impl fly_io::Node<CounterPayload, InjectedPayload> for CounterNode {
    fn from_init(
        init: fly_io::protocol::Init,
        tx: std::sync::mpsc::Sender<fly_io::Event<CounterPayload, InjectedPayload>>,
        network: &mut fly_io::network::Network<CounterPayload, InjectedPayload>,
    ) -> Self {
        eprintln!("INITIALIZING");
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
            value: Arc::new(RwLock::new(0)),
        };

        network
            .send(result.construct_message(SequentialStore::address(), SequentialStore::write(0)))
            .expect("failed to write initial value");

        eprintln!("SENT WRITE");

        result
    }

    async fn step(
        &mut self,
        event: fly_io::Event<CounterPayload, InjectedPayload>,
        network: &mut fly_io::network::Network<CounterPayload, InjectedPayload>,
    ) -> anyhow::Result<()> {
        match event {
            fly_io::Event::Injected(payload) => match payload {
                InjectedPayload::ReadFromStore => {}
            },
            fly_io::Event::Message(message) => {
                let mut reply = message.into_reply();
                match reply.body.payload {
                    CounterPayload::Add { delta } => {
                        eprintln!("GETTING CURRENT VALUE");
                        let current_value = self
                            .read_current_value(network)
                            .await
                            .context("reading current value from add")?;

                        let new_value = current_value + delta;
                        network
                            .send(self.construct_message(
                                SequentialStore::address(),
                                SequentialStore::compare_and_store(current_value, new_value),
                            ))
                            .context("adding delta")?;

                        reply.body.payload = CounterPayload::AddOk;
                        network.send(reply).context("sending add_ok reply")?;
                    }
                    CounterPayload::Read => {
                        eprintln!("READING");
                        let value = self
                            .read_current_value(network)
                            .await
                            .context("reading current value from read")?;

                        reply.body.payload = CounterPayload::ReadOk { value };
                        network.send(reply).context("sending read reply")?;
                        eprintln!("HAVE READ");
                    }
                    CounterPayload::AddOk => {}
                    CounterPayload::ReadOk { .. } => {}
                    CounterPayload::CasOk => {}
                    CounterPayload::WriteOk => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::server::Server::<CounterPayload, InjectedPayload>::new().serve::<CounterNode>()
}
