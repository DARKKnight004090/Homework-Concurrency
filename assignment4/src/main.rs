use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use rand::RngExt; 

const TERMINATION_SIGNAL: i32 = -1;

fn main() {
    const ITEM_COUNT: usize = 20;
    const PRODUCER_COUNT: usize = 2;
    const CONSUMER_COUNT: usize = 3;

    let (tx, rx) = mpsc::channel();
    let shared_rx = Arc::new(Mutex::new(rx));
    let mut handles = vec![];

    for i in 0..PRODUCER_COUNT {
        let thread_tx = tx.clone();
        let handle = thread::spawn(move || {
            producer(i, thread_tx, ITEM_COUNT / PRODUCER_COUNT);
        });
        handles.push(handle);
    }

    for i in 0..CONSUMER_COUNT {
        let thread_rx = Arc::clone(&shared_rx);
        let handle = thread::spawn(move || {
            consumer(i, thread_rx);
        });
        handles.push(handle);
    }

    for handle in handles.drain(0..PRODUCER_COUNT) {
        handle.join().unwrap();
    }

    for _ in 0..CONSUMER_COUNT {
        tx.send(TERMINATION_SIGNAL).unwrap();
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("All items have been produced and consumed!");
}

fn producer(id: usize, tx: mpsc::Sender<i32>, item_count: usize) {
    let mut rng = rand::rng(); 
    
    for _ in 0..item_count {
        let num: i32 = rng.random_range(1..100); 
        
        println!("Producer {} generated: {}", id, num);
        tx.send(num).unwrap();
        thread::sleep(Duration::from_millis(100));
    }
    println!("Producer {} finished.", id);
}

fn consumer(id: usize, rx: Arc<Mutex<mpsc::Receiver<i32>>>) {
    loop {
        let message = rx.lock().unwrap().recv().unwrap();

        if message == TERMINATION_SIGNAL {
            println!("Consumer {} received termination signal. Exiting.", id);
            break;
        }

        println!("Consumer {} processed number: {}", id, message);
        thread::sleep(Duration::from_millis(150));
    }
}