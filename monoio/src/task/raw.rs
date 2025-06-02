use std::{
    future::Future,
    ptr::NonNull,
    task::{Poll, Waker},
};

use crate::task::{Cell, Harness, Header, Schedule};

/// 任务本体，位于堆上。
/// 
/// 任务本体内存布局：
/// - `Cell[Header, Core, Trailer]`
pub(crate) struct RawTask {
    ptr: NonNull<Header>,
}

impl Clone for RawTask {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for RawTask {}

/// 任务虚表
/// 
/// 实际任务方法调用顺序：
/// - RawTask::Method --> Vtable::ProxyMethod --> RealFunc --> Harness::Method
pub(crate) struct Vtable {
    /// Poll the future
    /// 轮询
    pub(crate) poll: unsafe fn(NonNull<Header>),
    /// Deallocate the memory
    /// 释放
    pub(crate) dealloc: unsafe fn(NonNull<Header>),

    /// Read the task output, if complete
    /// 获取输出
    pub(crate) try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker),

    /// The join handle has been dropped
    /// 慢释放等待句柄
    pub(crate) drop_join_handle_slow: unsafe fn(NonNull<Header>),

    /// Set future output
    /// 设置任务输出
    #[cfg(feature = "sync")]
    pub(crate) finish: unsafe fn(NonNull<Header>, *mut ()),
}

/// Get the vtable for the requested `T` and `S` generics.
/// 
/// 任务虚表，负责代理自身方法执行。
/// - poll, dealloc, try_read_output, drop_join_handle_slow, finish。
pub(super) fn vtable<T: Future, S: Schedule>() -> &'static Vtable {
    &Vtable {
        poll: poll::<T, S>,
        dealloc: dealloc::<T, S>,
        try_read_output: try_read_output::<T, S>,
        drop_join_handle_slow: drop_join_handle_slow::<T, S>,
        #[cfg(feature = "sync")]
        finish: finish::<T, S>,
    }
}

impl RawTask {
    /// 新建任务本体
    /// - 输入：线程编号，异步操作Future，调度器。
    /// 
    /// 逻辑：
    /// - 创建任务内存块Cell，块内包含：Header，Core，Trailer。
    /// - 取任务内存块的地址，转为Header指针，作为任务本体的指针。
    /// 
    /// 这一部分结构略显混乱。
    pub(crate) fn new<T, S>(owner_id: usize, task: T, scheduler: S) -> RawTask
    where
        T: Future,
        S: Schedule,
    {
        let ptr = Box::into_raw(Cell::new(owner_id, task, scheduler));
        let ptr = unsafe { NonNull::new_unchecked(ptr as *mut Header) };

        RawTask { ptr }
    }

    pub(crate) unsafe fn from_raw(ptr: NonNull<Header>) -> RawTask {
        RawTask { ptr }
    }

    /// 取任务块头部指针。
    pub(crate) fn header(&self) -> &Header {
        unsafe { self.ptr.as_ref() }
    }

    /// Safety: mutual exclusion is required to call this function.
    /// 
    /// 代理函数：执行任务的轮询。
    /// 
    /// 调用此函数时，需要互斥。
    pub(crate) fn poll(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.poll)(self.ptr) }
    }

    /// 代理函数：回收内存块。
    pub(crate) fn dealloc(self) {
        let vtable = self.header().vtable;
        unsafe {
            (vtable.dealloc)(self.ptr);
        }
    }

    /// Safety: `dst` must be a `*mut Poll<super::Result<T::Output>>` where `T`
    /// is the future stored by the task.
    /// 
    /// 代理函数：尝试读取任务输出。如果没有输出则登记唤醒器。
    pub(crate) unsafe fn try_read_output(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        (vtable.try_read_output)(self.ptr, dst, waker);
    }

    /// 代理函数：慢移除任务等待者。
    pub(crate) fn drop_join_handle_slow(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.drop_join_handle_slow)(self.ptr) }
    }

    /// 代理函数：终结任务，同时设置任务输出内容。
    #[cfg(feature = "sync")]
    pub(crate) unsafe fn finish(self, val_slot: *mut ()) {
        let vtable = self.header().vtable;
        unsafe { (vtable.finish)(self.ptr, val_slot) }
    }
}

/// 代理目标函数：轮询任务
unsafe fn poll<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.poll();
}

/// 代理目标函数：回收任务
unsafe fn dealloc<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.dealloc();
}

/// 代理目标函数：任务终结，并设置任务输出内容。
#[cfg(feature = "sync")]
unsafe fn finish<T: Future, S: Schedule>(ptr: NonNull<Header>, val: *mut ()) {
    let harness = Harness::<T, S>::from_raw(ptr);
    let val = &mut *(val as *mut Option<<T as Future>::Output>);
    harness.finish(val.take().unwrap());
}

/// 代理目标函数：尝试读取任务输出。
unsafe fn try_read_output<T: Future, S: Schedule>(
    ptr: NonNull<Header>,
    dst: *mut (),
    waker: &Waker,
) {
    let out = &mut *(dst as *mut Poll<T::Output>);

    let harness = Harness::<T, S>::from_raw(ptr);
    harness.try_read_output(out, waker);
}

/// 代理目标函数：慢移除任务等待者。
unsafe fn drop_join_handle_slow<T: Future, S: Schedule>(ptr: NonNull<Header>) {
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.drop_join_handle_slow()
}
