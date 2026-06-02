use std::sync::Arc;

use tokio::sync::{Barrier, Mutex};

/// Runs a simple two-phase BSP-style computation.
///
/// Each worker computes a local sum in superstep 1,
/// synchronizes, then worker 0 aggregates in superstep 2.
pub async fn bsp_sum(workers_data: Vec<Vec<i32>>) -> i32 {
    if workers_data.is_empty() {
        return 0;
    }

    let workers = workers_data.len();
    let barrier = Arc::new(Barrier::new(workers));
    let partials = Arc::new(Mutex::new(vec![0; workers]));
    let result = Arc::new(Mutex::new(0));

    let mut handles = Vec::with_capacity(workers);

    for (id, data) in workers_data.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let partials = Arc::clone(&partials);
        let result = Arc::clone(&result);

        handles.push(tokio::spawn(async move {
            // Superstep 1: local compute.
            let local_sum = data.into_iter().sum::<i32>();
            {
                let mut partials = partials.lock().await;
                partials[id] = local_sum;
            }

            // Global synchronization barrier.
            barrier.wait().await;

            // Superstep 2: single worker aggregation.
            if id == 0 {
                let total = {
                    let partials = partials.lock().await;
                    partials.iter().sum::<i32>()
                };
                let mut shared_result = result.lock().await;
                *shared_result = total;
            }

            // Ensure all workers finish the superstep before exit.
            barrier.wait().await;
        }));
    }

    for handle in handles {
        handle.await.expect("worker task panicked");
    }

    let final_total = *result.lock().await;
    final_total
}

#[cfg(test)]
mod tests {
    use super::bsp_sum;

    #[tokio::test]
    async fn sums_values_across_workers() {
        let input = vec![vec![1, 2, 3], vec![4, 5], vec![6]];
        let total = bsp_sum(input).await;
        assert_eq!(total, 21);
    }

    #[tokio::test]
    async fn empty_workers_returns_zero() {
        let total = bsp_sum(Vec::<Vec<i32>>::new()).await;
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn handles_negative_values() {
        let input = vec![vec![10, -5], vec![1, -3, 2], vec![-1]];
        let total = bsp_sum(input).await;
        assert_eq!(total, 4);
    }
}
