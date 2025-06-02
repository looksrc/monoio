use core::task::{RawWaker, RawWakerVTable, Waker};
use std::cell::Cell;

/// Creates a waker that does nothing.
/// 
/// 创建一个根任务结果就绪唤醒器，唤醒后执行器会去尝试读取根任务的执行结果。
///
/// This `Waker` is useful for polling a `Future` to check whether it is
/// `Ready`, without doing any additional work.
/// 
/// 此唤醒器用于poll(ctx)探测任务是否Ready，并依据探测结果修改SHOULD_POLL标记。
/// 
/// wake, wake_by_ref：设置线程标记SHOULD_POLL为true，表示需要根任务需要被轮询。
pub(crate) fn dummy_waker() -> Waker {
    fn raw_waker() -> RawWaker {
        // the pointer is never dereferenced, so null is ok
        RawWaker::new(std::ptr::null::<()>(), vtable())
    }

    fn vtable() -> &'static RawWakerVTable {
        &RawWakerVTable::new(
            |_| raw_waker(),
            |_| {
                set_poll();
            },
            |_| {
                set_poll();
            },
            |_| {},
        )
    }

    unsafe { Waker::from_raw(raw_waker()) }
}

#[cfg(feature = "unstable")]
#[thread_local]
static SHOULD_POLL: Cell<bool> = Cell::new(true);

#[cfg(not(feature = "unstable"))]
thread_local! {
    static SHOULD_POLL: Cell<bool> = const { Cell::new(true) };
}

#[inline]
pub(crate) fn should_poll() -> bool {
    SHOULD_POLL.replace(false)
}

#[inline]
pub(crate) fn set_poll() {
    SHOULD_POLL.set(true);
}
