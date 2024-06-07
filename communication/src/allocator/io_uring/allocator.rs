//! Zero-copy allocator based on TCP.
use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::io::Write;
use std::net::TcpStream;
use std::rc::Rc;

use bytes::arc::Bytes;

use crate::allocator::canary::Canary;
use crate::allocator::io_uring::bytes_exchange::{BytesPull, MergeQueue, SendEndpoint};
use crate::allocator::io_uring::bytes_slab::BytesSlab;
use crate::allocator::io_uring::push_pull::{Puller, Pusher};
use crate::networking::MessageHeader;
use crate::{Allocate, Data, Message, Pull, Push};

/// Builds an instance of a [IoUringAllocator].
//#[derive(Debug)]
pub struct IoUringBuilder {
    /// Logical worker index, not process index.
    pub index: usize,

    /// Number of total workers.
    pub peers: usize,

    /// Connections to peers.
    pub sockets: Vec<Option<TcpStream>>,
}

impl IoUringBuilder {
    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> IoUringAllocator {
        let mut sends = Vec::with_capacity(self.peers);
        let mut send_queues = Vec::with_capacity(self.peers);
        let mut recvs = Vec::with_capacity(self.peers);

        for socket in self.sockets.iter() {
            if socket.is_some() {
                let send_queue = MergeQueue::new();
                send_queues.push(send_queue.clone());
                let sendpoint = SendEndpoint::new(send_queue);
                sends.push(Rc::new(RefCell::new(sendpoint)));

                let recv_queue = MergeQueue::new();
                recvs.push(recv_queue);
            } else {
                let local_queue = MergeQueue::new();
                let sendpoint = SendEndpoint::new(local_queue.clone());
                sends.push(Rc::new(RefCell::new(sendpoint)));
                recvs.push(local_queue);
            }
        }

        IoUringAllocator {
            index: self.index,
            peers: self.peers,
            canaries: Rc::new(RefCell::new(Vec::new())),
            channel_id_bound: None,
            staged: Vec::new(),
            events: Rc::new(RefCell::new(Default::default())),
            sockets: self.sockets,
            sends,
            send_queues,
            recvs,
            to_local: HashMap::new(),
        }
    }
}

/// A TCP-based allocator for inter-process communication.
pub struct IoUringAllocator {
    index: usize, // number out of peers
    peers: usize, // number of peer allocators (for typed channel allocation).

    staged: Vec<Bytes>, // staging area for incoming Bytes
    events: Rc<RefCell<Vec<usize>>>,
    canaries: Rc<RefCell<Vec<usize>>>,

    channel_id_bound: Option<usize>,

    // sending, receiving, and responding to binary buffers.
    sends: Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>, // sends[x] -> goes to process x.
    send_queues: Vec<MergeQueue>,
    recvs: Vec<MergeQueue>, // recvs[x] <- from process x.
    sockets: Vec<Option<TcpStream>>,
    to_local: HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>, // to worker-local typed pullers.
}

impl Allocate for IoUringAllocator {
    fn index(&self) -> usize {
        self.index
    }

    fn peers(&self) -> usize {
        self.peers
    }

    fn allocate<T: Data>(
        &mut self,
        identifier: usize,
    ) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        // Assume and enforce in-order identifier allocation.
        if let Some(bound) = self.channel_id_bound {
            assert!(bound < identifier);
        }
        self.channel_id_bound = Some(identifier);

        // Result list of boxed pushers.
        let mut pushes = Vec::<Box<dyn Push<Message<T>>>>::new();

        for target_index in 0..self.peers() {
            // message header template.
            let header = MessageHeader {
                channel: identifier,
                source: self.index,
                target: target_index,
                length: 0,
                seqno: 0,
            };

            // Create, box, and stash new process_binary pusher.
            pushes.push(Box::new(Pusher::new(
                header,
                self.sends[target_index].clone(),
            )));
        }

        let channel = self
            .to_local
            .entry(identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());

        let puller = Box::new(CountPuller::new(
            Puller::new(channel, canary),
            identifier,
            self.events().clone(),
        ));

        (pushes, puller)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {
        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped = self
                .to_local
                .remove(&dropped_channel)
                .expect("non-existent channel dropped");
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        std::mem::drop(canaries);

        let mut buffer = BytesSlab::new(20);
        for (target_index, send_queue) in self.sockets.iter_mut().enumerate() {
            if target_index == self.index {
                continue;
            }

            l
        }

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        let mut events = self.events.borrow_mut();

        for mut bytes in self.staged.drain(..) {
            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {
                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {
                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(std::mem::size_of::<MessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push(header.channel);

                    // Ensure that a queue exists.
                    match self.to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if self
                                .channel_id_bound
                                .map(|b| b < header.channel)
                                .unwrap_or(true)
                            {
                                entry
                                    .insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back(peel);
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back(peel);
                        }
                    }
                } else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        let mut stash = Vec::new();
        for (target_index, send_queue) in self.send_queues.iter_mut().enumerate() {
            if target_index == self.index {
                continue;
            }

            assert!(stash.is_empty());

            let writer = self.sockets[target_index]
                .as_mut()
                .expect("we're only sending to remote workers");

            send_queue.drain_into(&mut stash);

            for bytes in stash.drain(..) {
                println!("sending: {}", bytes.len());

                writer
                    .write_all(&bytes[..])
                    .unwrap_or_else(|e| tcp_panic("writing data", e));
            }
        }

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for (index, list) in self.to_local.iter() {
        //     let len = list.borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }

    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        &self.events
    }

    fn await_events(&self, duration: Option<std::time::Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                std::thread::park_timeout(duration);
            } else {
                std::thread::park();
            }
        }
    }
}

fn tcp_panic(context: &'static str, cause: std::io::Error) -> ! {
    // NOTE: some downstream crates sniff out "timely communication error:" from
    // the panic message. Avoid removing or rewording this message if possible.
    // It'd be nice to instead use `panic_any` here with a structured error
    // type, but the panic message for `panic_any` is no good (Box<dyn Any>).
    panic!("timely communication error: {}: {}", context, cause)
}
