use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use super::raw::RawTask;

/// JoinHandle can be used to wait task finished.
/// Note if you drop it directly, task will not be terminated.
/// 
/// 任务等待者，任务的几种句柄之一，丢弃后也不会导致任务终止。
/// 
/// 实现了Future，可以异步等待任务。
pub struct JoinHandle<T> {
    raw: RawTask,
    _p: PhantomData<T>,
}

unsafe impl<T: Send> Send for JoinHandle<T> {}
unsafe impl<T: Send> Sync for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub(super) fn new(raw: RawTask) -> JoinHandle<T> {
        JoinHandle {
            raw,
            _p: PhantomData,
        }
    }

    /// Checks if the task associated with this `JoinHandle` has finished.
    /// 
    /// 校验与此句柄关联的任务是否已经完成。是从状态值来判断的。
    pub fn is_finished(&self) -> bool {
        let state = self.raw.header().state.load();
        state.is_complete()
    }
}

impl<T> Unpin for JoinHandle<T> {}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ret = Poll::Pending;

        // Try to read the task output. If the task is not yet complete, the
        // waker is stored and is notified once the task does complete.
        //
        // The function must go via the vtable, which requires erasing generic
        // types. To do this, the function "return" is placed on the stack
        // **before** calling the function and is passed into the function using
        // `*mut ()`.
        //
        // Safety:
        //
        // The type of `T` must match the task's output type.
        unsafe {
            self.raw
                .try_read_output(&mut ret as *mut _ as *mut (), cx.waker());
        }
        ret
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        // 如果是在初始状态遗弃句柄，则直接移除标记位就行了。
        if self.raw.header().state.drop_join_handle_fast().is_ok() {
            return;
        }

        // 否则，执行慢移除句柄逻辑
        self.raw.drop_join_handle_slow();
    }
}
