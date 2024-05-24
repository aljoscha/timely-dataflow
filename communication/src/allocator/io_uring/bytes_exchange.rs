//! Types and traits for sharing `Bytes`.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use super::bytes_slab::BytesSlab;
use bytes::arc::Bytes;

/// A target for `Bytes`.
pub trait BytesPush {
    // /// Pushes bytes at the instance.
    // fn push(&mut self, bytes: Bytes);
    /// Pushes many bytes at the instance.
    fn extend<I: IntoIterator<Item = Bytes>>(&mut self, iter: I);
}
/// A source for `Bytes`.
pub trait BytesPull {
    // /// Pulls bytes from the instance.
    // fn pull(&mut self) -> Option<Bytes>;
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>);
}

use std::sync::atomic::{AtomicBool, Ordering};

/// An unbounded queue of bytes intended for point-to-point communication
/// between threads. Cloning returns another handle to the same queue.
///
/// TODO: explain "extend"
#[derive(Clone)]
pub struct MergeQueue {
    queue: Rc<RefCell<VecDeque<Bytes>>>, // queue of bytes.
    panic: Rc<AtomicBool>,
}

impl MergeQueue {
    /// Allocates a new queue with an associated signal.
    pub fn new() -> Self {
        MergeQueue {
            queue: Rc::new(RefCell::new(VecDeque::new())),
            panic: Rc::new(AtomicBool::new(false)),
        }
    }

    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> bool {
        if self.panic.load(Ordering::SeqCst) {
            panic!("MergeQueue poisoned.");
        }

        Rc::strong_count(&self.queue) == 1 && self.queue.borrow().is_empty()
    }
}

impl BytesPush for MergeQueue {
    fn extend<I: IntoIterator<Item = Bytes>>(&mut self, iterator: I) {
        if self.panic.load(Ordering::SeqCst) {
            panic!("MergeQueue poisoned.");
        }

        let mut queue = self.queue.borrow_mut();

        let mut iterator = iterator.into_iter();
        if let Some(bytes) = iterator.next() {
            let mut tail = if let Some(mut tail) = queue.pop_back() {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
                tail
            } else {
                bytes
            };

            for bytes in iterator {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
            }
            queue.push_back(tail);
        }
    }
}

impl BytesPull for MergeQueue {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        if self.panic.load(Ordering::SeqCst) {
            panic!("MergeQueue poisoned.");
        }

        let mut queue = self.queue.borrow_mut();

        vec.extend(queue.drain(..));
    }
}

// We want to ping in the drop because a channel closing can unblock a thread waiting on
// the next bit of data to show up.
impl Drop for MergeQueue {
    fn drop(&mut self) {
        // Propagate panic information, to distinguish between clean and unclean shutdown.
        if ::std::thread::panicking() {
            self.panic.store(true, Ordering::SeqCst);
        } else {
            // TODO: Perhaps this aggressive ordering can relax orderings elsewhere.
            if self.panic.load(Ordering::SeqCst) {
                panic!("MergeQueue poisoned.");
            }
        }
    }
}

/// A `BytesPush` wrapper which stages writes.
pub struct SendEndpoint<P: BytesPush> {
    send: P,
    buffer: BytesSlab,
}

impl<P: BytesPush> SendEndpoint<P> {
    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) {
        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {
            self.send.extend(Some(self.buffer.extract(valid_len)));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
        }
    }
    /// Makes the next `bytes` bytes valid.
    ///
    /// The current implementation also sends the bytes, to ensure early visibility.
    pub fn make_valid(&mut self, bytes: usize) {
        self.buffer.make_valid(bytes);
        self.send_buffer();
    }
    /// Acquires a prefix of `self.empty()` of length at least `capacity`.
    pub fn reserve(&mut self, capacity: usize) -> &mut [u8] {
        if self.buffer.empty().len() < capacity {
            self.send_buffer();
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        self.buffer.empty()
    }
    /// Marks all written data as valid, makes visible.
    pub fn publish(&mut self) {
        self.send_buffer();
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        self.send_buffer();
    }
}
