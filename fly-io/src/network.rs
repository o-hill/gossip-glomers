use std::{
    collections::HashMap,
    fmt::Debug,
    io::BufRead,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Context;
use serde::{de::DeserializeOwned, Serialize};
use std::thread::JoinHandle;

use crate::{protocol::Message, Event};

#[derive(Debug, Clone)]
pub struct Network<P, IP = ()> {
    pub tx: std::sync::mpsc::Sender<Event<P, IP>>,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<Event<P, IP>>>>,
    awaiting_responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<Event<P, IP>>>>>,
    message_id: Arc<RwLock<usize>>,
    stdout_lock: Arc<Mutex<()>>,
    stdin_lock: Arc<Mutex<()>>,
}

impl<P, IP> Network<P, IP>
where
    P: Send + Clone + Serialize + DeserializeOwned + 'static,
    IP: Send + Clone + 'static,
{
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            awaiting_responses: Arc::new(RwLock::new(HashMap::new())),
            message_id: Arc::new(RwLock::new(0)),
            stdout_lock: Arc::new(Mutex::new(())),
            stdin_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn read<PAYLOAD>(&mut self) -> anyhow::Result<Message<PAYLOAD>>
    where
        PAYLOAD: DeserializeOwned,
    {
        let _lock = self.stdin_lock.lock().unwrap();

        let stdin = std::io::stdin().lock();
        let mut stdin = stdin.lines();

        let line = stdin
            .next()
            .expect("could not read from stdin")
            .context("failed to read init message from stdin")?;

        let message: Message<PAYLOAD> =
            serde_json::from_str(&line).context("failed to deserialize message")?;

        Ok(message)
    }

    pub fn start_read_thread(&self) -> JoinHandle<anyhow::Result<()>> {
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            for input in stdin.lines() {
                let input = input.context("Maelstrom event could not be read from stdin")?;
                dbg!("RECEIVED {}", input.clone());
                let message: Message<P> = serde_json::from_str(input.as_str())
                    .context("failed to deserialize maelstrom input")?;
                if tx.send(Event::Message(message)).is_err() {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            Ok(())
        })
    }

    pub async fn recv(&mut self) -> Option<Event<P, IP>> {
        let receiver = self.rx.lock().unwrap();

        loop {
            let result = receiver.recv();
            let Ok(event) = result else { return None };

            if let Some(tx) = self.is_response(&event) {
                tx.send(event)
                    .unwrap_or_else(|_| panic!("failed to send event"));
            } else {
                return Some(event);
            }
        }
    }

    fn is_response(
        &self,
        event: &Event<P, IP>,
    ) -> Option<tokio::sync::oneshot::Sender<Event<P, IP>>> {
        if let Event::Message(message) = event {
            if let Some(replying_to) = message.body.in_reply_to {
                dbg!("REPLYING TO", replying_to);
                let request = self
                    .awaiting_responses
                    .write()
                    .unwrap()
                    .remove_entry(&replying_to);

                if let Some(r) = request {
                    dbg!("RESPONDING TO REQUEST", r.0);
                    return Some(r.1);
                }
            }
        }

        None
    }

    pub fn send<PAYLOAD>(&self, mut message: Message<PAYLOAD>) -> anyhow::Result<()>
    where
        PAYLOAD: Serialize + Clone + Debug,
    {
        message.body.id = Some(self.next_message_id());
        dbg!(
            "SENDING {:?}",
            serde_json::to_string(&message).expect("serializing message failed")
        );
        let _lock = self.stdout_lock.lock().unwrap();
        message.send().context("failed to send message")?;
        dbg!("SENT");
        Ok(())
    }

    pub async fn request<PAYLOAD>(
        &self,
        mut message: Message<PAYLOAD>,
    ) -> anyhow::Result<Event<P, IP>>
    where
        PAYLOAD: Serialize + Clone + Debug,
    {
        let _lock = self.stdout_lock.lock().unwrap();
        let id = self.next_message_id();

        message.body.id = Some(id);
        dbg!("REQUESTING {:?}", message.clone());
        message.send().context("failed to request message")?;
        drop(_lock);
        dbg!("REQUESTED");

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.awaiting_responses.write().unwrap().insert(id, tx);

        let response = rx.await.context("failed to receive response")?;
        dbg!("RECEIVED RESPONSE", id);
        Ok(response)
    }

    fn next_message_id(&self) -> usize {
        let mut message_id = self.message_id.write().unwrap();
        let id = *message_id;
        *message_id += 1;
        id
    }
}
