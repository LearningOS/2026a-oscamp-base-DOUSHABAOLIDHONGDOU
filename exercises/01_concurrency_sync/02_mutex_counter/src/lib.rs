//! # Mutex Shared State
//!
//! In this exercise, you will use `Arc<Mutex<T>>` to safely share and modify data between multiple threads.
//!
//! ## Concepts
//! - `Mutex<T>` mutex protects shared data
//! - `Arc<T>` atomic reference counting enables cross-thread sharing
//! - `lock()` acquires the lock and accesses data


use std::sync::{Arc, Mutex};
use std::thread;


/// Increment a counter concurrently using `n_threads` threads.
/// Each thread increments the counter `count_per_thread` times.
/// Returns the final counter value.
///
/// Hint: Use `Arc<Mutex<usize>>` as the shared counter.
pub fn concurrent_counter(n_threads: usize, count_per_thread: usize) -> usize {
    // TODO: Create Arc<Mutex<usize>> with initial value 0
    // TODO: Spawn n_threads threads
    // TODO: In each thread, lock() and increment count_per_thread times
    // TODO: Join all threads, return final value
    
    // Arc 让多个线程共享所有权，Mutex保护里面的usize
    let counter = Arc::new(Mutex::new(0usize));
    let mut handles = Vec::with_capacity(n_threads);

    for _ in 0..n_threads{
        // 每个线程拿一个引用计数副本；不加这一行，会把counter move走
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..count_per_thread{
                // lock()返回MutexGuard; 语句结束时guard 自动drop->自动解锁
                let mut guard = counter.lock().unwrap();
                *guard += 1;
            }
        }));
    }
    // 先全部join确保所有的写操作已完成
    for handle in handles  {
        handle.join().unwrap();
    }
    
    // 最后在读一次最终值
    let total = *counter.lock().unwrap();
    total
}

/// Add elements to a shared vector concurrently using multiple threads.
/// Each thread pushes its own id (0..n_threads) to the vector.
/// Returns the sorted vector.
///
/// Hint: Use `Arc<Mutex<Vec<usize>>>`.
pub fn concurrent_collect(n_threads: usize) -> Vec<usize> {
    // TODO: Create Arc<Mutex<Vec<usize>>>
    // TODO: Each thread pushes its own id
    // TODO: After joining all threads, sort the result and return
    let shard = Arc::new(Mutex::new(Vec::<usize>::new()));

    let mut handles = Vec::with_capacity(n_threads);

    for id in 0..n_threads{
        let shared = Arc::clone(&shard);

        handles.push(thread::spawn(move || {
            // 一行搞定：临时guard 在本语句结束时就解锁
            shared.lock().unwrap().push(id);
        }));
    }

    for handle in handles{
        handle.join().unwrap();
    }

    // 各个线程的push的先后顺序不确定，所以排个序
    let mut result = shard.lock().unwrap().clone();
    result.sort_unstable();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_single_thread() {
        assert_eq!(concurrent_counter(1, 100), 100);
    }

    #[test]
    fn test_counter_multi_thread() {
        assert_eq!(concurrent_counter(10, 100), 1000);
    }

    #[test]
    fn test_counter_zero() {
        assert_eq!(concurrent_counter(5, 0), 0);
    }

    #[test]
    fn test_collect() {
        let result = concurrent_collect(5);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_collect_single() {
        assert_eq!(concurrent_collect(1), vec![0]);
    }
}
