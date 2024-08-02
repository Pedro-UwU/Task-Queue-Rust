use std::{collections::VecDeque, sync::Arc, time::Duration, usize};

use tokio::sync::{Mutex, RwLock, Semaphore};

// Type for tasks
type BoxedTask = Box<dyn FnOnce() + Send>;

#[derive(Clone)]
pub struct TaskQueue {
    queue: Arc<Mutex<VecDeque<BoxedTask>>>,
    semaphore: Arc<Semaphore>,  // Semaphore for waiting to spawn new tasks
    running: Arc<RwLock<bool>>, // Boolean to check if the TaskQueue is running
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>, // JoinHandles of the spawned async tasks
}

impl TaskQueue {
    pub fn new(max_threads: usize) -> Self {
        TaskQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            semaphore: Arc::new(Semaphore::new(max_threads)),
            running: Arc::new(RwLock::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_task(&mut self, task: BoxedTask) {
        let mut queue = self.queue.lock().await;
        queue.push_back(task);
    }

    pub async fn start_processing(&mut self) -> tokio::task::JoinHandle<()> {
        // Starts running
        {
            let mut running = self.running.write().await;
            *running = true;
        }
        let running = self.running.clone();
        let queue = self.queue.clone();
        let semaphore = self.semaphore.clone();
        let handles = self.handles.clone();
        // The main executor is an async task that spawns new task in the async runtime. Each task
        // spawns a new thread blocking task and waits for it, this way, CPU intense tasks would
        // use other CPU threads
        let _main_executor = tokio::spawn(async move {
            while *running.read().await {
                let permit = semaphore.acquire().await.unwrap();
                let maybe_task = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };
                if let Some(task) = maybe_task {
                    let sem_clone = semaphore.clone();
                    let handle = tokio::task::spawn(async move {
                        // Each task takes a permit
                        // If the task is aborted, the permit will be droped, so aborting is safe
                        let task_permit = sem_clone.acquire().await.unwrap();
                        let future = tokio::task::spawn_blocking(task);
                        let _ = future.await;

                        drop(task_permit);
                    });
                    handles.lock().await.push(handle);
                } else {
                    drop(permit);
                    // Yield the runtime
                    let _ = tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }

                {
                    // Clean finished handles
                    handles.lock().await.retain(|h| !h.is_finished());
                }
            }
        });
        return _main_executor;
    }

    pub async fn shutdown(&mut self) {
        {
            let mut running = self.running.write().await;
            *running = false;
        }
        let mut handles = vec![];
        {
            let mut locked_handles = self.handles.lock().await;
            // Drain the handles to take ownership of the remaining tasks
            handles = locked_handles.drain(..).collect();
        };
        for handle in handles {
            let _ = handle.await;
        }
    }

    pub async fn is_finished(&self) -> bool {
        let handles = self.handles.lock().await;

        if handles.len() == 0 {
            let queue = self.queue.lock().await;
            return queue.len() == 0;
        }
        false
    }
}

fn sleep_some_time(i: u64) {
    println!("Inside: Before Sleep {}", i);
    std::thread::sleep(Duration::from_secs(2));
    println!("After: Inside Sleep {}", i);
}

#[tokio::main]
async fn main() {
    let mut task_queue = TaskQueue::new(5);
    for i in 0..=10 {
        task_queue
            .add_task(Box::new(move || {
                sleep_some_time(i);
            }))
            .await;
    }

    task_queue.start_processing().await;
    while !task_queue.is_finished().await {}
    task_queue.shutdown().await;
}
