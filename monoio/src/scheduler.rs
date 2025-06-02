use std::{cell::UnsafeCell, collections::VecDeque, marker::PhantomData};

use crate::task::{Schedule, Task};

/// 本地调度器
/// 
/// 任务只会在单个线程内流转，不会跨线程移动，因此叫本地调度器。
pub(crate) struct LocalScheduler;

/// 实现调度器
impl Schedule for LocalScheduler {
    /// 对任务进行调度
    /// 
    /// 调度逻辑：将任务插入到当前线程待执行队列尾部，排队执行。
    fn schedule(&self, task: Task<Self>) {
        crate::runtime::CURRENT.with(|cx| cx.tasks.push(task));
    }

    /// 调度为最高优先级
    /// 
    /// 调度逻辑：将任务插入到当前线程待执行队列头部，作为下一个被执行的任务。
    fn yield_now(&self, task: Task<Self>) {
        crate::runtime::CURRENT.with(|cx| cx.tasks.push_front(task));
    }
}

/// 本地待执行任务队列
/// 
/// - 本地的运行时会在循环自己的队列逐个执行(Task::run())任务。
/// - 提供内部可变性。包装了VecDeque。
/// - Drop逻辑：弹出并销毁所有任务。
pub(crate) struct TaskQueue {
    // Local queue.
    queue: UnsafeCell<VecDeque<Task<LocalScheduler>>>,
    // Make sure the type is `!Send` and `!Sync`.
    _marker: PhantomData<*const ()>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        unsafe {
            let queue = &mut *self.queue.get();
            while let Some(_task) = queue.pop_front() {}
        }
    }
}

impl TaskQueue {
    /// 新建队列
    /// 
    /// 预分配长度4096.
    pub(crate) fn new() -> Self {
        const DEFAULT_TASK_QUEUE_SIZE: usize = 4096;
        Self::new_with_capacity(DEFAULT_TASK_QUEUE_SIZE)
    }

    /// 新建队列
    /// 
    /// 自定义预分配长度。
    pub(crate) fn new_with_capacity(capacity: usize) -> Self {
        Self {
            queue: UnsafeCell::new(VecDeque::with_capacity(capacity)),
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        unsafe { (*self.queue.get()).len() }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn push(&self, runnable: Task<LocalScheduler>) {
        unsafe {
            (*self.queue.get()).push_back(runnable);
        }
    }

    pub(crate) fn push_front(&self, runnable: Task<LocalScheduler>) {
        unsafe {
            (*self.queue.get()).push_front(runnable);
        }
    }

    pub(crate) fn pop(&self) -> Option<Task<LocalScheduler>> {
        unsafe { (*self.queue.get()).pop_front() }
    }
}
