//! Zero-copy allocator based on TCP.
use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::io::{AsRawFd, RawFd};
use std::rc::Rc;

use bytes::arc::Bytes;
use io_uring::{opcode, types, IoUring};
use slab::Slab;

use crate::allocator::canary::Canary;
use crate::allocator::io_uring::bytes_exchange::{BytesPull, BytesPush, MergeQueue, SendEndpoint};
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
                send_queues.push(local_queue.clone());
                let sendpoint = SendEndpoint::new(local_queue.clone());
                sends.push(Rc::new(RefCell::new(sendpoint)));
                recvs.push(local_queue);
            }
        }

        let mut ring = IoUring::new(256).expect("can construct uring");
        let mut bufpool = Vec::with_capacity(64);
        let mut buf_alloc = Slab::with_capacity(64);
        let mut token_alloc = Slab::with_capacity(64);

        let socket_fds: Vec<Option<RawFd>> = self
            .sockets
            .iter()
            .map(|s| s.as_ref().map(|s| s.as_raw_fd()))
            .collect();

        // Put in place read requests for all sockets.

        let mut outstanding_entries = 0;

        for (index, socket_fd) in socket_fds.iter().enumerate() {
            let socket_fd = match socket_fd {
                Some(fd) => fd,
                None => continue,
            };
            //println!("putting in read for index {index}!");
            let (buf_index, buf) = match bufpool.pop() {
                Some(buf_index) => (buf_index, &mut buf_alloc[buf_index]),
                None => {
                    let buf = BytesSlab::new(20);
                    let buf_entry = buf_alloc.vacant_entry();
                    let buf_index = buf_entry.key();
                    (buf_index, buf_entry.insert(buf))
                }
            };

            let read_token = token_alloc.insert(Token::Read {
                fd: *socket_fd,
                buf_index,
                target_index: index,
            });

            let read_e = opcode::Recv::new(
                types::Fd(*socket_fd),
                buf.empty().as_mut_ptr(),
                buf.empty().len() as _,
            )
            .build()
            .user_data(read_token as _);

            // Yikes!
            unsafe {
                match ring.submission().push(&read_e) {
                    Ok(_) => (),
                    Err(e) => {
                        panic!("trying to submit initial reads: {:?}", e);
                    }
                }
            }
            outstanding_entries += 1;
        }

        ring.submit().expect("error submitting");

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
            socket_fds,
            ring,
            outstanding_entries,
            bufpool,
            buf_alloc,
            token_alloc,
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

    // io-uring business
    ring: IoUring,
    outstanding_entries: usize,
    bufpool: Vec<usize>,
    buf_alloc: Slab<BytesSlab>,
    token_alloc: Slab<Token>,
    socket_fds: Vec<Option<RawFd>>,
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

        //println!(
        //    "receive!, outstanding_entries: {}",
        //    self.outstanding_entries
        //);
        //if self.outstanding_entries > 0 {
            //self.ring.submit_and_wait(1).expect("error submitting");
            //self.ring.submit().expect("error submitting");
        //}

        //println!("sync!");
        let mut cq = self.ring.completion();
        //cq.sync();

        let mut new_entries = Vec::new();

        for cqe in &mut cq {
            self.outstanding_entries -= 1;
            //println!("cqe: {:?}", cqe);

            let ret = cqe.result();
            let token_index = cqe.user_data() as usize;

            if ret < 0 {
                eprintln!(
                    "token {:?} error: {:?}",
                    self.token_alloc.get(token_index),
                    std::io::Error::from_raw_os_error(-ret)
                );
                continue;
            }

            let token = &mut self.token_alloc[token_index];

            match token.clone() {
                Token::Read {
                    fd,
                    buf_index,
                    target_index,
                } => {
                    //if ret == 0 {
                    //    self.bufpool.push(buf_index);
                    //    self.token_alloc.remove(token_index);
                    //
                    //    println!("socket shutting down?");
                    //
                    //    unsafe {
                    //        libc::close(fd);
                    //    }
                    //} else {
                        self.token_alloc.remove(token_index);

                        let mut still_active = true;
                        let len = ret as usize;
                        let buf = &mut self.buf_alloc[buf_index];

                        println!("received len: {}", len);

                        buf.make_valid(len);

                        // Consume complete messages from the front of self.buffer.
                        while let Some(header) = MessageHeader::try_read(buf.valid()) {
                            // TODO: Consolidate message sequences sent to the same worker?
                            let peeled_bytes = header.required_bytes();
                            let bytes = buf.extract(peeled_bytes);

                            if header.length > 0 {
                                self.recvs[target_index].extend(vec![bytes]);
                            } else {
                                // Shutting down; confirm absence of subsequent data.
                                still_active = false;
                                if !buf.valid().is_empty() {
                                    panic!("Clean shutdown followed by data.");
                                }
                            }
                        }

                        if !still_active {
                            continue;
                        }

                        //println!("putting in read for index {target_index}!");

                        let read_token = self.token_alloc.insert(Token::Read {
                            fd,
                            buf_index,
                            target_index,
                        });

                        //println!("buf empty len {}", buf.empty().len());
                        buf.ensure_capacity(1024);
                        let read_e = opcode::Recv::new(
                            types::Fd(fd),
                            buf.empty().as_mut_ptr(),
                            buf.empty().len() as _,
                        )
                        .build()
                        .user_data(read_token as _);

                        new_entries.push(read_e);
                    //}
                }
                Token::Write {
                    fd: _,
                    buf_index: _,
                    offset: _,
                    len: _,
                } => unreachable!("not doing uring writes yet"),
            }
        }

        drop(cq);

        for entry in new_entries.iter() {
            // Yikes!
            unsafe {
                match self.ring.submission().push(entry) {
                    Ok(_) => (),
                    Err(e) => {
                        panic!("trying to submit new entry {:?}: {:?}", entry, e);
                    }
                }
            }
            self.outstanding_entries += 1;
        }
        if self.outstanding_entries > 0 {
            self.ring.submit().expect("error submitting");
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
            //println!("release!, target_index: {}", target_index);
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

    fn await_events(&self, _duration: Option<std::time::Duration>) {
        //if self.outstanding_entries > 0 {
        //    self.ring.submit_and_wait(1).expect("error submitting");
        //}
        //if let Some(duration) = duration {
        //    std::thread::park_timeout(duration);
        //}
    }
}

fn tcp_panic(context: &'static str, cause: std::io::Error) -> ! {
    // NOTE: some downstream crates sniff out "timely communication error:" from
    // the panic message. Avoid removing or rewording this message if possible.
    // It'd be nice to instead use `panic_any` here with a structured error
    // type, but the panic message for `panic_any` is no good (Box<dyn Any>).
    panic!("timely communication error: {}: {}", context, cause)
}

#[derive(Clone, Debug)]
enum Token {
    Read {
        fd: RawFd,
        buf_index: usize,
        target_index: usize,
    },
    Write {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
    },
}
