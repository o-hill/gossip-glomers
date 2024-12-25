use std::collections::HashMap;
use std::io::BufRead;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protocol::{InitPayload, Message};
use crate::Event;

#[derive(Debug, Clone)]
pub struct Network<P, IP = ()>
where
    P: Clone,
    IP: Clone,
{
    pub tx: std::sync::mpsc::Sender<Event<P, IP>>,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<Event<P, IP>>>>,
    awaiting_responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<Event<P, IP>>>>>,
    message_id: Arc<RwLock<usize>>,
}

impl<P, IP> Network<P, IP>
where
    P: Clone + Serialize,
    IP: Clone,
{
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            awaiting_responses: Arc::new(RwLock::new(HashMap::new())),
            message_id: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn recv(&mut self) -> Option<Event<P, IP>> {
        let receiver = self.rx.lock().unwrap();

        loop {
            let event = receiver.recv().unwrap();
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
            if let Some(replying_to) = message.body.id {
                let request = self
                    .awaiting_responses
                    .write()
                    .unwrap()
                    .remove_entry(&replying_to);

                if let Some(r) = request {
                    return Some(r.1);
                }
            }
        }

        None
    }

    pub fn send<PAYLOAD>(&self, mut message: Message<PAYLOAD>) -> anyhow::Result<()>
    where
        PAYLOAD: Serialize,
    {
        let mut stdout = std::io::stdout().lock();
        message.body.id = Some(self.next_message_id());
        message
            .send(&mut stdout)
            .context("failed to send message")?;
        Ok(())
    }

    pub async fn request(&self, mut message: Message<P>) -> anyhow::Result<Event<P, IP>> {
        let mut stdout = std::io::stdout().lock();
        let id = self.next_message_id();

        message.body.id = Some(id);
        message
            .send(&mut stdout)
            .context("failed to request message")?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.awaiting_responses.write().unwrap().insert(id, tx);

        rx.await.context("failed to receive response")
    }

    fn next_message_id(&self) -> usize {
        let mut message_id = self.message_id.write().unwrap();
        let id = *message_id;
        *message_id += 1;
        id
    }
}

pub struct Server<P, IP>
where
    P: Clone,
    IP: Clone,
{
    network: Network<P, IP>,
}

impl<P, IP> Server<P, IP>
where
    P: Clone + Serialize + DeserializeOwned + Send + 'static,
    IP: Clone + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            network: Network::new(),
        }
    }

    fn construct_node<NODE>(
        &self,
        tx: std::sync::mpsc::Sender<Event<P, IP>>,
        init_line: String,
    ) -> anyhow::Result<NODE>
    where
        NODE: crate::Node<P, IP>,
    {
        let init_msg: Message<InitPayload> =
            serde_json::from_str(&init_line).context("failed to deserialize init message")?;

        let InitPayload::Init(init) = init_msg.body.payload.clone() else {
            panic!("first message was not an init");
        };

        let node = NODE::from_init(init, tx.clone());

        let mut reply = init_msg.into_reply();
        reply.body.payload = InitPayload::InitOk;
        self.network.send(reply).context("sending init_ok")?;

        Ok(node)
    }

    fn start_read_thread(
        tx: std::sync::mpsc::Sender<Event<P, IP>>,
    ) -> JoinHandle<anyhow::Result<()>> {
        std::thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            for input in stdin.lines() {
                let input = input.context("Maelstrom event could not be read from stdin")?;
                let message: Message<P> = serde_json::from_str(input.as_str())
                    .context("failed to deserialize maelstrom input")?;
                if tx.send(Event::Message(message)).is_err() {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            Ok(())
        })
    }

    #[tokio::main]
    pub async fn serve<NODE>(&mut self) -> anyhow::Result<()>
    where
        NODE: crate::Node<P, IP> + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let stdin = std::io::stdin().lock();
        let mut stdin = stdin.lines();

        let first_line = stdin
            .next()
            .expect("could not read from stdin")
            .context("failed to read init message from stdin")?;

        let mut node: NODE = self
            .construct_node(tx.clone(), first_line)
            .context("constructing node from init message")?;

        drop(stdin);
        let jh = Server::start_read_thread(tx.clone());

        let mut network = self.network.clone();
        let node_task = tokio::task::spawn(async move {
            while let Ok(event) = rx.recv() {
                if node.step(event, &mut network).await.is_err() {
                    break;
                };
            }
        });

        jh.join()
            .expect("stdin thread panicked")
            .context("stdin thread panicked")?;

        node_task.await.expect("node thread crashed");

        Ok(())
    }
}
