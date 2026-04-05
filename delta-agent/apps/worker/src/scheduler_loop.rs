use scheduler::queue::ScheduledTask;
use scheduler::worker::SchedulerWorker;

pub async fn run() {
    let mut worker = SchedulerWorker::default();
    worker.enqueue(ScheduledTask {
        id: "task-1".to_owned(),
        branch_id: "main".to_owned(),
        priority_score: 100,
    });

    let _ = worker.run_once().await;
    tracing::info!("worker loop tick complete");
}
