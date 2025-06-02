//! Task impl
// Heavily borrowed from tokio.
// Copyright (c) 2021 Tokio Contributors, licensed under the MIT license.

mod utils;
pub(crate) mod waker_fn;

mod core;
use self::core::{Cell, Header};

mod harness;
use self::harness::Harness;

mod join;
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub use self::join::JoinHandle;

mod raw;
use self::raw::RawTask;

mod state;

mod waker;

use std::{future::Future, marker::PhantomData, ptr::NonNull};

/// An owned handle to the task, tracked by ref count, not sendable
/// 
/// 异步任务主句柄，用于执行任务时使用。具有任务的所有权(依据OBRM即负责任务的销毁)。
/// 
/// 另一个句柄：JoinHandle，用于任务的await，异步等待任务完成。
#[repr(transparent)]
pub(crate) struct Task<S: 'static> {
    raw: RawTask,
    _p: PhantomData<S>,
}

impl<S: 'static> Task<S> {
    unsafe fn from_raw(ptr: NonNull<Header>) -> Task<S> {
        Task {
            raw: RawTask::from_raw(ptr),
            _p: PhantomData,
        }
    }

    /// 任务块的头块地址
    fn header(&self) -> &Header {
        self.raw.header()
    }

    /// 轮询一次任务，透传给任务的poll()方法
    pub(crate) fn run(self) {
        self.raw.poll();
    }

    /// 任务标记为已完成。
    #[cfg(feature = "sync")]
    pub(crate) unsafe fn finish(&mut self, val_slot: *mut ()) {
        self.raw.finish(val_slot);
    }
}

impl<S: 'static> Drop for Task<S> {
    fn drop(&mut self) {
        // Decrement the ref count
        if self.header().state.ref_dec() {
            // Deallocate if this is the final ref count
            self.raw.dealloc();
        }
    }
}

/// 调度器特质，尺寸固定且具有静态存活期。
pub(crate) trait Schedule: Sized + 'static {
    /// Schedule the task
    /// 设计为对任务进行调度。(优先级不确定)
    fn schedule(&self, task: Task<Self>);
    /// Schedule the task to run in the near future, yielding the thread to
    /// other tasks.
    /// ???。
    fn yield_now(&self, task: Task<Self>) {
        self.schedule(task);
    }
}

/// 根据提供的异步操作创建任务。返回两大任务句柄。
/// 
/// 要求：Future值和Future::Output值具有静态存活期
/// - 解释一：不含有非'static引用，或不包含引用
/// - 解释二：不依赖任何非static的值，永远不用担心自身在使用期间会意外失效。
pub(crate) fn new_task<T, S>(
    owner_id: usize,
    task: T,
    scheduler: S,
) -> (Task<S>, JoinHandle<T::Output>)
where
    S: Schedule,
    T: Future + 'static,
    T::Output: 'static,
{
    unsafe { new_task_holding(owner_id, task, scheduler) }
}

/// 根据提供的异步操作创建任务。返回两大任务句柄。
/// 
/// 没有静态存活期的限制，扩大了函数的使用范围。
pub(crate) unsafe fn new_task_holding<T, S>(
    owner_id: usize,
    task: T,
    scheduler: S,
) -> (Task<S>, JoinHandle<T::Output>)
where
    S: Schedule,
    T: Future,
{
    let raw = RawTask::new::<T, S>(owner_id, task, scheduler);
    let task = Task {
        raw,
        _p: PhantomData,
    };
    let join = JoinHandle::new(raw);

    (task, join)
}
