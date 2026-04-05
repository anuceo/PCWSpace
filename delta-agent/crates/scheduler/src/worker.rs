use crate::queue::{ScheduledTask, SchedulerQueue};

#[derive(Debug, Default)]
pub struct SchedulerWorker {
    queue: SchedulerQueue,
}

impl SchedulerWorker {
    pub fn enqueue(&mut self, task: ScheduledTask) {
        self.queue.push(task);
    }

    pub async fn run_once(&mut self) -> Option<ScheduledTask> {
        self.queue.pop()
    }
}
