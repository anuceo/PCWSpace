use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledTask {
    pub id: String,
    pub branch_id: String,
    pub priority_score: i64,
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority_score
            .cmp(&other.priority_score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub struct SchedulerQueue {
    heap: BinaryHeap<ScheduledTask>,
}

impl SchedulerQueue {
    pub fn push(&mut self, task: ScheduledTask) {
        self.heap.push(task);
    }

    pub fn pop(&mut self) -> Option<ScheduledTask> {
        self.heap.pop()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}
