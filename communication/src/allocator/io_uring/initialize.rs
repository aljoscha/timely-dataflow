//! Network initialization.

use std::io;
use std::io::{Read, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use abomonation::{decode, encode};

use crate::allocator::io_uring::allocator::IoUringBuilder;

// This constant is sent along immediately after establishing a TCP stream, so
// that it is easy to sniff out Timely traffic when it is multiplexed with
// other traffic on the same port.
const HANDSHAKE_MAGIC: u64 = 0xc2f1fb770118add9;

/// Join handles for send and receive threads.
///
/// On drop, the guard joins with each of the threads to ensure that they complete
/// cleanly and send all necessary data.
pub struct CommsGuard {
    send_guards: Vec<::std::thread::JoinHandle<()>>,
    recv_guards: Vec<::std::thread::JoinHandle<()>>,
}

impl Drop for CommsGuard {
    fn drop(&mut self) {
        for handle in self.send_guards.drain(..) {
            handle.join().expect("Send thread panic");
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            handle.join().expect("Recv thread panic");
        }
        // println!("RECV THREADS JOINED");
    }
}

use crate::logging::{CommunicationEvent, CommunicationSetup};
use logging_core::Logger;

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    process_index: usize,
    threads: usize,
    noisy: bool,
    _log_sender: Box<
        dyn Fn(CommunicationSetup) -> Option<Logger<CommunicationEvent, CommunicationSetup>>
            + Send
            + Sync,
    >,
) -> ::std::io::Result<Vec<IoUringBuilder>> {
    println!(
        "addresses: {:?}, my_index: {process_index}, threads: {threads}",
        addresses
    );
    let peers = addresses.len();

    let sockets = create_sockets(addresses, threads, process_index, noisy)?;
    println!("sockets: {:#?}", sockets);

    let builders = sockets
        .into_iter()
        .enumerate()
        .map(|(local_index, sockets)| {
            let index = process_index * threads + local_index;

            IoUringBuilder {
                index,
                peers,
                sockets,
            }
        })
        .collect();

    //println!("builders: {:#?}", builders);

    Ok(builders)
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
pub fn create_sockets(
    addresses: Vec<String>,
    threads: usize,
    process_index: usize,
    noisy: bool,
) -> Result<Vec<Vec<Option<TcpStream>>>> {
    let mut tasks = Vec::new();

    let hosts = Arc::new(addresses);

    for thread in 0..threads {
        let worker_index = thread + threads * process_index;

        let hosts1 = hosts.clone();
        let hosts2 = hosts.clone();

        let start_task = thread::spawn(move || start_connections(hosts1, worker_index, noisy));
        let await_task = thread::spawn(move || await_connections(hosts2, worker_index, noisy));

        tasks.push((start_task, await_task));
    }

    let mut results = Vec::new();
    for (start_task, await_task) in tasks {
        let mut worker_results = start_task.join().unwrap()?;
        worker_results.push(None);
        let to_extend = await_task.join().unwrap()?;
        worker_results.extend(to_extend.into_iter());

        results.push(worker_results);
    }

    if noisy {
        println!("worker {}:\tinitialization complete", process_index)
    }

    Ok(results)
}

/// Result contains connections [0, my_index - 1].
pub fn start_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
    noisy: bool,
) -> Result<Vec<Option<TcpStream>>> {
    let results = addresses
        .iter()
        .take(my_index)
        .enumerate()
        .map(|(index, address)| loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    unsafe { encode(&HANDSHAKE_MAGIC, &mut stream) }
                        .expect("failed to encode/send handshake magic");
                    unsafe { encode(&(my_index as u64), &mut stream) }
                        .expect("failed to encode/send worker index");
                    if noisy {
                        println!("worker {}:\tconnection to worker {}", my_index, index);
                    }
                    break Some(stream);
                }
                Err(error) => {
                    println!(
                        "worker {}:\terror connecting to worker {}: {}; retrying",
                        my_index, index, error
                    );
                    sleep(Duration::from_secs(1));
                }
            }
        })
        .collect();

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
pub fn await_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
    noisy: bool,
) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1))
        .map(|_| None)
        .collect();
    let listener = TcpListener::bind(&addresses[my_index][..])?;

    for _ in (my_index + 1)..addresses.len() {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8; 16];
        stream.read_exact(&mut buffer)?;
        let (magic, mut buffer) =
            unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        if magic != &HANDSHAKE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "received incorrect timely handshake",
            ));
        }
        let identifier = unsafe { decode::<u64>(&mut buffer) }
            .expect("failed to decode worker index")
            .0
            .clone() as usize;
        results[identifier - my_index - 1] = Some(stream);
        if noisy {
            println!(
                "worker {}:\tconnection from worker {}",
                my_index, identifier
            );
        }
    }

    Ok(results)
}
