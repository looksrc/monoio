use std::{
    fmt,
    sync::atomic::{
        AtomicUsize,
        Ordering::{AcqRel, Acquire},
    },
};

/// 任务状态位序列。
pub(crate) struct State(AtomicUsize);

/// Current state value
/// 
/// 任务状态位序列快照，用作状态临时值。
#[derive(Copy, Clone)]
pub(crate) struct Snapshot(usize);

type UpdateResult = Result<Snapshot, Snapshot>;

/// The task is currently being run.
/// 任务内在状态：正在执行中。
const RUNNING: usize = 0b0001;

/// The task is complete.
///
/// Once this bit is set, it is never unset
///
/// 任务内在状态：已完成，不可逆。
const COMPLETE: usize = 0b0010;

/// Extracts the task's lifecycle value from the state
///
/// 任务内在状态掩码。
const LIFECYCLE_MASK: usize = 0b11;

/// Flag tracking if the task has been pushed into a run queue.
///
/// 任务外在状态：已插入执行队列。
const NOTIFIED: usize = 0b100;

/// The join handle is still around
///
/// 任务外在状态：有等待者。
#[allow(clippy::unusual_byte_groupings)] // https://github.com/rust-lang/rust-clippy/issues/6556
const JOIN_INTEREST: usize = 0b1_000;

/// A join handle waker has been set
///
/// 任务外在状态：已注册等待句柄的唤醒器。
#[allow(clippy::unusual_byte_groupings)] // https://github.com/rust-lang/rust-clippy/issues/6556
const JOIN_WAKER: usize = 0b10_000;

/// All bits
///
/// 任务全状态掩码。(内在、外在)
const STATE_MASK: usize = LIFECYCLE_MASK | NOTIFIED | JOIN_INTEREST | JOIN_WAKER;

/// Bits used by the ref count portion of the state.
///
/// 引用计数掩码位，任务状态位占用了低5位，高26位为引用计数位，共占用32位(u32)。
const REF_COUNT_MASK: usize = !STATE_MASK;

/// Number of positions to shift the ref count
///
/// 引用计数每增一，state值应该增 2^5，即`1<<5`
const REF_COUNT_SHIFT: usize = REF_COUNT_MASK.count_zeros() as usize;

/// One ref count
///
/// 引用计数每增一，state值应该增 2^5，即`1<<5`
const REF_ONE: usize = 1 << REF_COUNT_SHIFT;

/// State a task is initialized with
///
/// 初始状态值：
/// - 非正在执行中：0
/// - 未完成：0
/// - 未通知：0
/// - 有等待者：1
/// - 未设置等待者唤醒器：0
/// - 引用计数2：Task和JoinHandle。
/// 
/// A task is initialized with two references:
///
///  * A reference for Task.
///  * A reference for the JoinHandle.
///
/// As the task starts with a `JoinHandle`, `JOIN_INTEREST` is set.
/// As the task starts with a `Notified`, `NOTIFIED` is set.
const INITIAL_STATE: usize = (REF_ONE * 2) | JOIN_INTEREST | NOTIFIED;

#[must_use]
pub(super) enum TransitionToIdle {
    Ok,
    OkNotified,
}

#[must_use]
pub(super) enum TransitionToNotified {
    DoNothing,
    Submit,
}

impl State {
    /// 创建初始状态。
    pub(crate) fn new() -> Self {
        State(AtomicUsize::new(INITIAL_STATE))
    }

    /// 当前状态快照。
    pub(crate) fn load(&self) -> Snapshot {
        Snapshot(self.0.load(Acquire))
    }

    /// Attempt to transition the lifecycle to `Running`. This sets the
    /// notified bit to false so notifications during the poll can be detected.
    /// 
    /// 状态更改：
    /// - 更改前：非执行中，已通知，非已完成。
    /// - 更改为：执行中，未通知。
    pub(super) fn transition_to_running(&self) {
        self.fetch_update_action(|mut curr| {
            debug_assert!(curr.is_notified());
            debug_assert!(curr.is_idle());
            curr.set_running();
            curr.unset_notified();
            ((), Some(curr))
        });
    }

    /// Transitions the task from `Running` -> `Idle`.
    /// 
    /// 状态更改：
    /// - 更改前：执行中
    /// - 更改为：非执行中，
    /// - 返回值：如果当前为已通知，则返回OkNotified，否则返回Ok。
    pub(super) fn transition_to_idle(&self) -> TransitionToIdle {
        self.fetch_update_action(|mut curr| {
            debug_assert!(curr.is_running());
            curr.unset_running();
            let action = if curr.is_notified() {
                TransitionToIdle::OkNotified
            } else {
                TransitionToIdle::Ok
            };
            (action, Some(curr))
        })
    }

    /// Transitions the task from `Running` -> `Complete`.
    /// 
    /// 状态更改：
    /// - 更改前：执行中，未完成
    /// - 更改为：未执行，已完成
    pub(super) fn transition_to_complete(&self) -> Snapshot {
        const DELTA: usize = RUNNING | COMPLETE;

        // 异或：相同为0相异为1。(没必要搞这种)
        // 此时RUNNING必为1，COMPLETE必为0，因此最终效果RUNNING被设为了0，COMPLETE被设为了1。
        let prev = Snapshot(self.0.fetch_xor(DELTA, AcqRel));
        debug_assert!(prev.is_running());
        debug_assert!(!prev.is_complete());

        Snapshot(prev.0 ^ DELTA)
    }

    /// Try transitions the state to `NOTIFIED`, but if it cannot do it without submitting, it will
    /// return false. In another word, if it returns true, it means we have marked the task notified
    /// and do not have to do anything.
    /// 
    /// 状态更改：
    /// - 执行中：改为已通知
    /// - 已完成或已通知：不动
    /// - 其它，既非执行中，也非已完成：false。
    pub(crate) fn transition_to_notified_without_submit(&self) -> bool {
        self.fetch_update_action(|mut curr| {
            if curr.is_running() {
                curr.set_notified();
                (true, Some(curr))
            } else if curr.is_complete() || curr.is_notified() {
                (true, Some(curr))
            } else {
                (false, Some(curr))
            }
        })
    }

    /// Transitions the state to `NOTIFIED`.
    /// 
    /// 状态变更为已通知。返回状态指示是否需要执行提交队列操作。
    pub(super) fn transition_to_notified(&self) -> TransitionToNotified {
        self.fetch_update_action(|mut curr| {
            let action = if curr.is_running() {
                curr.set_notified();
                TransitionToNotified::DoNothing
            } else if curr.is_complete() || curr.is_notified() {
                TransitionToNotified::DoNothing
            } else {
                curr.set_notified();
                TransitionToNotified::Submit
            };
            (action, Some(curr))
        })
    }

    /// Optimistically tries to swap the state assuming the join handle is
    /// __immediately__ dropped on spawn
    /// 
    /// 用于在刚孵化出任务时立即遗弃等待者句柄(即不需要句柄)。
    pub(super) fn drop_join_handle_fast(&self) -> Result<(), ()> {
        match self.fetch_update(|curr| {
            if *curr == INITIAL_STATE {
                Some(Snapshot((INITIAL_STATE - REF_ONE) & !JOIN_INTEREST))
            } else {
                None
            }
        }) {
            Ok(_) => {
                trace!("MONOIO DEBUG[State]: drop_join_handle_fast");
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    /// Try to unset the JOIN_INTEREST flag.
    ///
    /// Returns `Ok` if the operation happens before the task transitions to a
    /// completed state, `Err` otherwise.
    /// 
    /// 移除等待者标记。如果任务已完成则移除失败。
    pub(super) fn unset_join_interested(&self) -> UpdateResult {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());

            if curr.is_complete() {
                return None;
            }

            let mut next = curr;
            next.unset_join_interested();

            Some(next)
        })
    }

    /// Set the `JOIN_WAKER` bit.
    ///
    /// Returns `Ok` if the bit is set, `Err` otherwise. This operation fails if
    /// the task has completed.
    /// 
    /// 添加等待者唤醒器标记。如果任务已完成则添加失败。
    pub(super) fn set_join_waker(&self) -> UpdateResult {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());
            assert!(!curr.has_join_waker());

            if curr.is_complete() {
                return None;
            }

            let mut next = curr;
            next.set_join_waker();

            Some(next)
        })
    }

    /// Unsets the `JOIN_WAKER` bit.
    ///
    /// Returns `Ok` has been unset, `Err` otherwise. This operation fails if
    /// the task has completed.
    /// 
    /// 移除`JOIN_WAKER`状态位。
    /// 
    /// 前导状态必须是：有等待者、有等待者唤醒器。
    /// 如果任务为已完成则不修改，否则修改。最终返回修改结论。
    pub(super) fn unset_waker(&self) -> UpdateResult {
        self.fetch_update(|curr| {
            assert!(curr.is_join_interested());
            assert!(curr.has_join_waker());

            if curr.is_complete() {
                return None;
            }

            let mut next = curr;
            next.unset_join_waker();

            Some(next)
        })
    }

    /// 引用计数增一，如果状态值溢出则终止进程。
    pub(crate) fn ref_inc(&self) {
        use std::{process, sync::atomic::Ordering::Relaxed};

        let prev = Snapshot(self.0.fetch_add(REF_ONE, Relaxed));

        trace!(
            "MONOIO DEBUG[State]: ref_inc {}, ptr: {:p}",
            prev.ref_count() + 1,
            self
        );

        // If the reference count overflowed, abort.
        if prev.0 > isize::MAX as usize {
            process::abort();
        }
    }

    /// Returns `true` if the task should be released.
    /// 
    /// 引用计数减一，如果减到了0则返回true，说明任务需要释放掉。
    pub(crate) fn ref_dec(&self) -> bool {
        let prev = Snapshot(self.0.fetch_sub(REF_ONE, AcqRel));
        debug_assert!(prev.ref_count() >= 1);
        trace!(
            "MONOIO DEBUG[State]: ref_dec {}, ptr: {:p}",
            prev.ref_count() - 1,
            self
        );
        prev.ref_count() == 1
    }

    /// 利用当前状态执行一个有可能改变状态的操作，输出“操作”结果
    /// 
    /// 闭包说明：
    /// - 如果想修改状态，则闭包必须返回Some(New)。接下来利用乐观锁修改状态。
    /// - 如果不想修改状态，则闭包必须返回None。
    ///
    /// 乐观锁：
    /// - 当做完一套操作后，回写状态时，需要保证依然处于原状态。
    /// - 如果回写时不是原状态，则需要重做整个过程，直到锁成功。
    ///
    /// 换句话说：
    /// - 你依据原来的值做了一些操作，生成了新值。
    /// - 当你回写新值时，一定要保证被回写的值还是原来那个值。
    /// - 假如不是原来那个值，则需要回写的新值也失去意义。
    fn fetch_update_action<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(Snapshot) -> (T, Option<Snapshot>),
    {
        // 做当前状态快照
        let mut curr = self.load();

        loop {
            // 用快照执行目标操作
            // output: 操作输出;
            // next: 需要回写的新状态值;
            let (output, next) = f(curr);

            // 确认是否需要回写
            // 如果无需回写(None),说明所做操作不影响状态，则直接返回输出就完了。
            let next = match next {
                Some(next) => next,
                None => return output,
            };

            // 乐观执行回写
            let res = self.0.compare_exchange(curr.0, next.0, AcqRel, Acquire);

            // 回写结果：
            // - 成功，返回操作输出。乐观锁成功的条件是，在执行完操作并回写状态时，
            //   原状态未被更改过。
            // - 失败，标明操作期间原状态发生了变化，乐观锁失败了，需要加载最新状态，重新执行操作。
            match res {
                Ok(_) => return output,
                Err(actual) => curr = Snapshot(actual),
            }
        }
    }

    /// 利用当前状态执行一个有可能改变状态的操作，输出“状态修改”结果
    /// - Ok: 状态发生了修改
    /// - Err：状态未发生修改
    fn fetch_update<F>(&self, mut f: F) -> Result<Snapshot, Snapshot>
    where
        F: FnMut(Snapshot) -> Option<Snapshot>,
    {
        let mut curr = self.load();

        loop {
            let next = match f(curr) {
                Some(next) => next,
                None => return Err(curr),
            };

            let res = self.0.compare_exchange(curr.0, next.0, AcqRel, Acquire);

            match res {
                Ok(_) => return Ok(next),
                Err(actual) => curr = Snapshot(actual),
            }
        }
    }
}

impl std::ops::Deref for Snapshot {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Snapshot {
    /// Returns `true` if the task is in an idle state.
    ///
    /// 空闲状态判断：非执行中、非已完成。
    pub(super) fn is_idle(self) -> bool {
        self.0 & (RUNNING | COMPLETE) == 0
    }

    /// Returns `true` if the task has been flagged as notified.
    ///
    /// 是否已通知。
    pub(super) fn is_notified(self) -> bool {
        self.0 & NOTIFIED == NOTIFIED
    }

    /// 移除状态：已通知。
    fn unset_notified(&mut self) {
        self.0 &= !NOTIFIED
    }

    /// 设置状态：已通知。
    fn set_notified(&mut self) {
        self.0 |= NOTIFIED
    }

    /// 是否正在执行。
    pub(super) fn is_running(self) -> bool {
        self.0 & RUNNING == RUNNING
    }

    /// 设置状态：正在执行中。
    fn set_running(&mut self) {
        self.0 |= RUNNING;
    }

    /// 移除状态：正在执行中。
    fn unset_running(&mut self) {
        self.0 &= !RUNNING;
    }

    /// Returns `true` if the task's future has completed execution.
    /// 
    /// 是否已完成。
    pub(super) fn is_complete(self) -> bool {
        self.0 & COMPLETE == COMPLETE
    }

    /// 是否有等待者。
    pub(super) fn is_join_interested(self) -> bool {
        self.0 & JOIN_INTEREST == JOIN_INTEREST
    }

    /// 移除状态：等待者。
    fn unset_join_interested(&mut self) {
        self.0 &= !JOIN_INTEREST
    }

    /// 是否设置了等待者唤醒器。
    pub(super) fn has_join_waker(self) -> bool {
        self.0 & JOIN_WAKER == JOIN_WAKER
    }

    /// 设置状态：有等待者唤醒器。
    fn set_join_waker(&mut self) {
        self.0 |= JOIN_WAKER;
    }

    /// 移除状态：无等待者唤醒器。
    fn unset_join_waker(&mut self) {
        self.0 &= !JOIN_WAKER
    }

    /// 计算引用计数。
    pub(super) fn ref_count(self) -> usize {
        (self.0 & REF_COUNT_MASK) >> REF_COUNT_SHIFT
    }
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self.load();
        snapshot.fmt(fmt)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Snapshot")
            .field("is_running", &self.is_running())
            .field("is_complete", &self.is_complete())
            .field("is_notified", &self.is_notified())
            .field("is_join_interested", &self.is_join_interested())
            .field("has_join_waker", &self.has_join_waker())
            .field("ref_count", &self.ref_count())
            .finish()
    }
}
