use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};

pub struct SequentialStore;
impl SequentialStore {
    fn address() -> String {
        "seq-kv".to_string()
    }

    fn key() -> String {
        "value".to_string()
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
}

enum InjectedPayload {
    ReadFromStore,
}

struct CounterNode {
    node_id: String,
    id: usize,
    value: usize,
}

impl fly_io::Node<CounterPayload, InjectedPayload> for CounterNode {
    fn from_init(
        init: fly_io::Init,
        tx: std::sync::mpsc::Sender<fly_io::Event<CounterPayload, InjectedPayload>>,
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

        Self {
            node_id: init.node_id,
            id: 0,
            value: 0,
        }
    }

    fn step(
        &mut self,
        event: fly_io::Event<CounterPayload, InjectedPayload>,
        mut output: &mut impl std::io::Write,
    ) -> anyhow::Result<()> {
        match event {
            fly_io::Event::Injected(payload) => match payload {
                InjectedPayload::ReadFromStore => {}
            },
            fly_io::Event::Message(message) => {
                let mut reply = message.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    CounterPayload::Add { delta } => {
                        let add = fly_io::Message {
                            src: self.node_id.clone(),
                            dst: SequentialStore::address(),
                            body: fly_io::Body {
                                id: None,
                                in_reply_to: None,
                                payload: SequentialStorePayload::Cas {
                                    key: SequentialStore::key(),
                                    from: self.value,
                                    to: new,
                                    create_if_not_exists: Some(true),
                                },
                            },
                        };
                        add.send(&mut output).context("sending add to seq-kv")?;
                        self.value = new;
                        reply.body.payload = CounterPayload::AddOk;
                        reply.send(&mut output).context("replying to add")?;
                    }
                    CounterPayload::Read => {
                        reply.body.payload = CounterPayload::ReadOk { value: self.value };
                        reply.send(&mut output).context("sending read reply")?;
                    }
                    CounterPayload::AddOk => {}
                    CounterPayload::ReadOk { value } => {
                        if reply.dst == SequentialStore::address() {
                            self.value = value;
                        }
                    }
                    CounterPayload::CasOk => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fly_io::run::<CounterPayload, InjectedPayload, CounterNode>()
}
