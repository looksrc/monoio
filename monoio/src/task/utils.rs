use std::cell::UnsafeCell;

pub(crate) trait UnsafeCellExt<T> {
    fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R;
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R;
}

impl<T> UnsafeCellExt<T> for UnsafeCell<T> {
    /// 扩展方法，以自身为入参执行一个操作。
    fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.get())
    }

    /// 扩展方法，以自身为入参执行一个操作。
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.get())
    }
}
