//! Blocking tasks related.
//! 
//! 融合阻塞任务。
//! 
//! spawn_blocking：
//! - 创建：
//!   * Blocking Function --> BockingFuture -->`[new_task]`(Task,JoinHandle) --> BlockingTask
//! - 调度：
//!   * 任务入池：ThreadPool::schedule_task() --> pool.execute(t)
//! - 返回：
//!   * JoinHandle<Result<R,Err>>
//! - 池中任务：
//!  * 执行：BlockingTask::run() --> Task::run() --> BlockingFuture::poll() --> Func()
//!  * 结果：JoinHandle.await
//! 
//! 在spawn_blocking返回时，任务仍在池中执行，通过JoinHandle异步等待结果。
//! 
//! 阻塞任务和异步任务异同：
//! - 都创建Task异步任务
//! - 都通过JoinHandle.wait等待结果。(融合到异步上下文中)
//! - 不同点：一个调度给异步运行时，一个调度给了线程池。
//!

use std::{future::Future, task::Poll};

use threadpool::{Builder as ThreadPoolBuilder, ThreadPool as ThreadPoolImpl};

use crate::{
    task::{new_task, JoinHandle},
    utils::thread_id::DEFAULT_THREAD_ID,
};

/// Users may implement a ThreadPool and attach it to runtime.
/// We also provide an implementation based on threadpool crate, you can use DefaultThreadPool.
/// 
/// 要想作为monoio的阻塞线程池，只需要实现ThreadPool，即实现将任务让渡给线程池执行(schedule_task)。
/// 
/// 本库提供了一个基于`threadpool`项目的实现DefaultThreadPoll。
pub trait ThreadPool {
    /// Monoio runtime will call `schedule_task` on `spawn_blocking`.
    /// ThreadPool impl must execute it now or later.
    fn schedule_task(&self, task: BlockingTask);
}

/// Error on waiting blocking task.
#[derive(Debug, Clone, Copy)]
pub enum JoinError {
    /// Task is canceled.
    Canceled,
}

/// BlockingTask is contrusted by monoio, ThreadPool impl
/// will execute it with `.run()`.
/// 
/// ThreadPool的实现者通过`.run()`方法执行BlockingTask。
pub struct BlockingTask {
    task: Option<crate::task::Task<NoopScheduler>>,
    blocking_vtable: &'static BlockingTaskVtable,
}

/// BlockingTask可以安全的在线程间移动。
unsafe impl Send for BlockingTask {}

/// 任务虚表，只有一个drop方法。用于销毁任务。
struct BlockingTaskVtable {
    pub(crate) drop: unsafe fn(&mut crate::task::Task<NoopScheduler>),
}

/// 新建虚表，返回其静态引用。
fn blocking_vtable<V>() -> &'static BlockingTaskVtable {
    &BlockingTaskVtable {
        drop: blocking_task_drop::<V>,
    }
}

/// 为虚表的drop字段提供的方法值。
/// 
/// 遗弃逻辑：终止任务且将任务执行结果设为已取消。
fn blocking_task_drop<V>(task: &mut crate::task::Task<NoopScheduler>) {
    let mut opt: Option<Result<V, JoinError>> = Some(Err(JoinError::Canceled));
    unsafe { task.finish((&mut opt) as *mut _ as *mut ()) };
}

/// 阻塞任务的遗弃逻辑：调用虚表的遗弃方法执行实际的遗弃操作。
impl Drop for BlockingTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.as_mut() {
            unsafe { (self.blocking_vtable.drop)(task) };
        }
    }
}

impl BlockingTask {
    /// Run task.
    /// 
    /// 执行
    #[inline]
    pub fn run(mut self) {
        let task = self.task.take().unwrap();
        task.run();
        // // if we are within a runtime, just run it.
        // if crate::runtime::CURRENT.is_set() {
        //     task.run();
        //     return;
        // }
        // // if we are on a standalone thread, we will use thread local ctx as Context.
        // crate::runtime::DEFAULT_CTX.with(|ctx| {
        //     crate::runtime::CURRENT.set(ctx, || task.run());
        // });
    }
}

/// BlockingStrategy can be set if there is no ThreadPool attached.
/// It controls how to handle `spawn_blocking` without thread pool.
/// 
/// 如果运行时没有附加线程池，可以设置阻塞的策略：BlockingStrategy。
/// 阻塞策略用于控制在不使用线程池的情况下处理`spawn_blocking`。
#[derive(Clone, Copy, Debug)]
pub enum BlockingStrategy {
    /// Panic when `spawn_blocking`.
    /// 
    /// `spawn_blocking`孵化阻塞任务时直接恐慌。
    Panic,
    /// Execute with current thread when `spawn_blocking`.
    /// 
    /// 在当前线程正常执行。
    ExecuteLocal,
}

/// `spawn_blocking` is used for executing a task(without async) with heavy computation or blocking
/// io.
/// 
/// `spawn_blocking`用于孵化并执行CPU敏感型或带阻塞IO的非异步任务。
///
/// To used it, users may initialize a thread pool and attach it on creating runtime.
/// Users can also set `BlockingStrategy` for a runtime when there is no thread pool.
/// WARNING: DO NOT USE THIS FOR ASYNC TASK! Async tasks will not be executed but only built the
/// future!
/// 
/// 如何使用：
/// - 先将同步操作函数包装为异步操作BlockingFuture，然后创建异步任务。
/// - 如果附加了线程池：将Task任务套壳为BlockingTask调度给线程池负责执行。
/// - 如果没有附加线程池：依据策略要么执行，要么直接抛出Panic。
/// 
/// 切记：
/// - 不要为异步任务使用这个功能，异步任务被调用时只会创建Future，而不是直接执行。
pub fn spawn_blocking<F, R>(func: F) -> JoinHandle<Result<R, JoinError>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let fut = BlockingFuture(Some(func));
    let (task, join) = new_task(DEFAULT_THREAD_ID, fut, NoopScheduler);

    crate::runtime::CURRENT.with(|inner| {
        let handle = &inner.blocking_handle;
        match handle {
            BlockingHandle::Attached(shared) => shared.schedule_task(BlockingTask {
                task: Some(task),
                blocking_vtable: blocking_vtable::<R>(),
            }),
            BlockingHandle::Empty(BlockingStrategy::ExecuteLocal) => task.run(),
            BlockingHandle::Empty(BlockingStrategy::Panic) => {
                // For users: if you see this panic, you have 2 choices:
                // 1. attach a shared thread pool to execute blocking tasks
                // 2. set runtime blocking strategy to `BlockingStrategy::ExecuteLocal`
                // Note: solution 2 will execute blocking task on current thread and may block other
                // tasks This may cause other tasks high latency.
                panic!("execute blocking task without thread pool attached")
            }
        }
    });

    join
}

/// DefaultThreadPool is a simple wrapped `threadpool::ThreadPool` that implement
/// `monoio::blocking::ThreadPool`. You may use this implementation, or you can use your own thread
/// pool implementation.
/// 
/// 引入并包装`threadpoll`项目的线程池作为monoio的阻塞线程池的实现。
/// 
/// 需要为默认线程池实现monoio进行阻塞任务调度并执行的功能，即ThreadPool特质。。
#[derive(Clone)]
pub struct DefaultThreadPool {
    pool: ThreadPoolImpl,
}

impl DefaultThreadPool {
    /// Create a new DefaultThreadPool.
    pub fn new(num_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::default()
            .num_threads(num_threads)
            .build();
        Self { pool }
    }
}

impl ThreadPool for DefaultThreadPool {
    #[inline]
    fn schedule_task(&self, task: BlockingTask) {
        self.pool.execute(move || task.run());
    }
}

/// 阻塞任务的调度器。
/// 
/// 因为阻塞任务不会被异步调度，因此不需要调度器。
/// 但是要创建Task还需要，因此弄个空的做占位符。
pub(crate) struct NoopScheduler;

impl crate::task::Schedule for NoopScheduler {
    fn schedule(&self, _task: crate::task::Task<Self>) {
        unreachable!()
    }

    fn yield_now(&self, _task: crate::task::Task<Self>) {
        unreachable!()
    }
}

/// 阻塞线程池设置
/// 
/// 附加到运行时，或为空(但是指定了为空时的处理策略)。
pub(crate) enum BlockingHandle {
    Attached(Box<dyn crate::blocking::ThreadPool + Send + 'static>),
    Empty(BlockingStrategy),
}

impl From<BlockingStrategy> for BlockingHandle {
    fn from(value: BlockingStrategy) -> Self {
        Self::Empty(value)
    }
}

/// 阻塞操作异步包装
/// 
/// 异步执行逻辑：poll()时直接执行，执行完返回Poll::Ready(r)。
struct BlockingFuture<F>(Option<F>);

impl<T> Unpin for BlockingFuture<T> {}

impl<F, R> Future for BlockingFuture<F>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    type Output = Result<R, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let me = &mut *self;
        let func = me.0.take().expect("blocking task ran twice.");
        Poll::Ready(Ok(func()))
    }
}

#[cfg(test)]
mod tests {
    use super::DefaultThreadPool;

    /// NaiveThreadPool always create a new thread on executing tasks.
    struct NaiveThreadPool;

    impl super::ThreadPool for NaiveThreadPool {
        fn schedule_task(&self, task: super::BlockingTask) {
            std::thread::spawn(move || {
                task.run();
            });
        }
    }

    /// FakeThreadPool always drop tasks.
    struct FakeThreadPool;

    impl super::ThreadPool for FakeThreadPool {
        fn schedule_task(&self, _task: super::BlockingTask) {}
    }

    #[test]
    fn hello_blocking() {
        let shared_pool = Box::new(NaiveThreadPool);
        let mut rt = crate::RuntimeBuilder::<crate::FusionDriver>::new()
            .attach_thread_pool(shared_pool)
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let begin = std::time::Instant::now();
            let join = crate::spawn_blocking(|| {
                // Simulate a heavy computation.
                std::thread::sleep(std::time::Duration::from_millis(400));
                "hello spawn_blocking!".to_string()
            });
            let sleep_async = crate::time::sleep(std::time::Duration::from_millis(400));
            let (result, _) = crate::join!(join, sleep_async);
            let eps = begin.elapsed();
            assert!(eps < std::time::Duration::from_millis(800));
            assert!(eps >= std::time::Duration::from_millis(400));
            assert_eq!(result.unwrap(), "hello spawn_blocking!");
        });
    }

    #[test]
    #[should_panic]
    fn blocking_panic() {
        let mut rt = crate::RuntimeBuilder::<crate::FusionDriver>::new()
            .with_blocking_strategy(crate::blocking::BlockingStrategy::Panic)
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let join = crate::spawn_blocking(|| 1);
            let _ = join.await;
        });
    }

    #[test]
    fn blocking_current() {
        let mut rt = crate::RuntimeBuilder::<crate::FusionDriver>::new()
            .with_blocking_strategy(crate::blocking::BlockingStrategy::ExecuteLocal)
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let begin = std::time::Instant::now();
            let join = crate::spawn_blocking(|| {
                // Simulate a heavy computation.
                std::thread::sleep(std::time::Duration::from_millis(100));
                "hello spawn_blocking!".to_string()
            });
            let sleep_async = crate::time::sleep(std::time::Duration::from_millis(100));
            let (result, _) = crate::join!(join, sleep_async);
            let eps = begin.elapsed();
            assert!(eps > std::time::Duration::from_millis(200));
            assert_eq!(result.unwrap(), "hello spawn_blocking!");
        });
    }

    #[test]
    fn drop_task() {
        let shared_pool = Box::new(FakeThreadPool);
        let mut rt = crate::RuntimeBuilder::<crate::FusionDriver>::new()
            .attach_thread_pool(shared_pool)
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let ret = crate::spawn_blocking(|| 1).await;
            assert!(matches!(ret, Err(super::JoinError::Canceled)));
        });
    }

    #[test]
    fn default_pool() {
        let shared_pool = Box::new(DefaultThreadPool::new(6));
        let mut rt = crate::RuntimeBuilder::<crate::FusionDriver>::new()
            .attach_thread_pool(shared_pool)
            .enable_timer()
            .build()
            .unwrap();
        macro_rules! thread_sleep {
            ($s:expr) => {
                || {
                    // Simulate a heavy computation.
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    $s
                }
            };
        }
        rt.block_on(async {
            let begin = std::time::Instant::now();
            let join1 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking1!"));
            let join2 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking2!"));
            let join3 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking3!"));
            let join4 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking4!"));
            let join5 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking5!"));
            let join6 = crate::spawn_blocking(thread_sleep!("hello spawn_blocking6!"));
            let sleep_async = crate::time::sleep(std::time::Duration::from_millis(500));
            let (result1, result2, result3, result4, result5, result6, _) =
                crate::join!(join1, join2, join3, join4, join5, join6, sleep_async);
            let eps = begin.elapsed();
            assert!(eps < std::time::Duration::from_millis(3000));
            assert!(eps >= std::time::Duration::from_millis(500));
            assert_eq!(result1.unwrap(), "hello spawn_blocking1!");
            assert_eq!(result2.unwrap(), "hello spawn_blocking2!");
            assert_eq!(result3.unwrap(), "hello spawn_blocking3!");
            assert_eq!(result4.unwrap(), "hello spawn_blocking4!");
            assert_eq!(result5.unwrap(), "hello spawn_blocking5!");
            assert_eq!(result6.unwrap(), "hello spawn_blocking6!");
        });
    }
}
