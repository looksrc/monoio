use std::{
    future::Future,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops,
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable, Waker},
};

use super::{core::Header, harness::Harness, Schedule};

/// 唤醒器引用对象
/// 
/// - 实际具有唤醒器所有权，但禁止了自动遗弃，改为手动。
/// - 可以解引用为内部的唤醒器。
pub(super) struct WakerRef<'a, S: 'static> {
    waker: ManuallyDrop<Waker>,
    _p: PhantomData<(&'a Header, S)>,
}

/// Returns a `WakerRef` which avoids having to pre-emptively increase the
/// refcount if there is no need to do so.
/// 
/// 创建唤醒器，创建唤醒器引用对象。
pub(super) fn waker_ref<T, S>(header: &Header) -> WakerRef<'_, S>
where
    T: Future,
    S: Schedule,
{
    // `Waker::will_wake` uses the VTABLE pointer as part of the check. This
    // means that `will_wake` will always return false when using the current
    // task's waker. (discussion at rust-lang/rust#66281).
    // 
    // will_wake函数会比较数据指针和虚表指针，因此在使用当前任务唤醒器的时候会永远返回false。
    //
    // To fix this, we use a single vtable. Since we pass in a reference at this
    // point and not an *owned* waker, we must ensure that `drop` is never
    // called on this waker instance. This is done by wrapping it with
    // `ManuallyDrop` and then never calling drop.
    //
    // 修复这个问题：
    // 使用一个独立的虚表。由于此时传入的是引用而非唤醒器实例，因此必须确保唤醒器不会被遗弃。利用ManuallyDrop实现。
    let waker = unsafe { ManuallyDrop::new(Waker::from_raw(raw_waker::<T, S>(header))) };

    WakerRef {
        waker,
        _p: PhantomData,
    }
}

impl<S> ops::Deref for WakerRef<'_, S> {
    type Target = Waker;

    fn deref(&self) -> &Waker {
        &self.waker
    }
}

/// 虚表方法：克隆唤醒器
unsafe fn clone_waker<T, S>(ptr: *const ()) -> RawWaker
where
    T: Future,
    S: Schedule,
{
    let header = ptr as *const Header;
    trace!("MONOIO DEBUG[Waker]: clone_waker");
    (*header).state.ref_inc();
    raw_waker::<T, S>(header)
}

/// 虚表方法：遗弃唤醒器
unsafe fn drop_waker<T, S>(ptr: *const ())
where
    T: Future,
    S: Schedule,
{
    let ptr = NonNull::new_unchecked(ptr as *mut Header);
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.drop_reference();
}

/// 虚表方法：按值唤醒
unsafe fn wake_by_val<T, S>(ptr: *const ())
where
    T: Future,
    S: Schedule,
{
    let ptr = NonNull::new_unchecked(ptr as *mut Header);
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.wake_by_val();
}

/// 虚表方法：按引用唤醒
unsafe fn wake_by_ref<T, S>(ptr: *const ())
where
    T: Future,
    S: Schedule,
{
    let ptr = NonNull::new_unchecked(ptr as *mut Header);
    let harness = Harness::<T, S>::from_raw(ptr);
    harness.wake_by_ref();
}


/// 创建一个RawWaker
/// 
/// 数据指向任务的Header。
pub(super) fn raw_waker<T, S>(header: *const Header) -> RawWaker
where
    T: Future,
    S: Schedule,
{
    let ptr = header as *const ();
    let vtable = &RawWakerVTable::new(
        clone_waker::<T, S>,
        wake_by_val::<T, S>,
        wake_by_ref::<T, S>,
        drop_waker::<T, S>,
    );
    RawWaker::new(ptr, vtable)
}
