use alloc::rc::{Rc, Weak};

pub trait Downgrade {
    type Target: Upgrade;
    fn downgrade(&self) -> Self::Target;
}

impl<'a, T> Downgrade for &'a T
where
    T: Downgrade,
{
    type Target = T::Target;

    fn downgrade(&self) -> Self::Target {
        (**self).downgrade()
    }
}

impl<'a, T> Downgrade for &'a mut T
where
    T: Downgrade,
{
    type Target = T::Target;

    fn downgrade(&self) -> Self::Target {
        (**self).downgrade()
    }
}

impl<T> Downgrade for Option<T>
where
    T: Downgrade,
{
    type Target = Option<T::Target>;

    fn downgrade(&self) -> Self::Target {
        self.as_ref().map(|value| value.downgrade())
    }
}

impl<T> Upgrade for Option<T>
where
    T: Upgrade,
{
    type Target = Option<T::Target>;

    fn upgrade(&self) -> Option<Self::Target> {
        match self {
            // Nothing to upgrade: the upgrade itself succeeds with no value.
            None => Some(None),
            // A present value must still be alive, otherwise the upgrade fails.
            Some(value) => value.upgrade().map(Some),
        }
    }
}

pub trait Upgrade {
    type Target;
    fn upgrade(&self) -> Option<Self::Target>;
}

impl<'a, T> Upgrade for &'a T
where
    T: Upgrade,
{
    type Target = T::Target;

    fn upgrade(&self) -> Option<Self::Target> {
        (**self).upgrade()
    }
}

impl<'a, T> Upgrade for &'a mut T
where
    T: Upgrade,
{
    type Target = T::Target;

    fn upgrade(&self) -> Option<Self::Target> {
        (**self).upgrade()
    }
}

impl<T> Downgrade for Rc<T> {
    type Target = Weak<T>;

    fn downgrade(&self) -> Self::Target {
        Rc::downgrade(self)
    }
}

impl<T> Upgrade for Weak<T> {
    type Target = Rc<T>;

    fn upgrade(&self) -> Option<Self::Target> {
        Weak::upgrade(self)
    }
}

impl<T> Downgrade for Weak<T> {
    type Target = Weak<T>;

    fn downgrade(&self) -> Self::Target {
        self.clone()
    }
}

impl<T> Downgrade for alloc::sync::Arc<T> {
    type Target = alloc::sync::Weak<T>;

    fn downgrade(&self) -> Self::Target {
        alloc::sync::Arc::downgrade(self)
    }
}

impl<T> Upgrade for alloc::sync::Weak<T> {
    type Target = alloc::sync::Arc<T>;

    fn upgrade(&self) -> Option<Self::Target> {
        alloc::sync::Weak::upgrade(self)
    }
}

impl<T> Downgrade for alloc::sync::Weak<T> {
    type Target = alloc::sync::Weak<T>;

    fn downgrade(&self) -> Self::Target {
        self.clone()
    }
}

macro_rules! primitives {
    ($($t:ty),+) => {
        $(
            impl Downgrade for $t {
                type Target = $t;

                fn downgrade(&self) -> Self::Target {
                    *self
                }
            }

            impl Upgrade for $t {
                type Target = $t;

                fn upgrade(&self) -> Option<Self::Target> {
                    Some(*self)
                }
            }
        )+
    };
}

primitives!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, char
);

macro_rules! tuples {
    ($first: ident) => {
        impl<$first> Downgrade for ($first,)
        where
            $first: Downgrade,
        {
            type Target = ($first::Target,);

            fn downgrade(&self) -> Self::Target {
                (self.0.downgrade(),)
            }
        }

        impl<$first> Upgrade for ($first,)
        where
            $first: Upgrade,
        {
            type Target = ($first::Target,);

            fn upgrade(&self) -> Option<Self::Target> {
                Some((self.0.upgrade()?,))
            }
        }
    };
    ($first: ident, $($rest: ident),+) => {
        tuples!($($rest),+);

        impl<$first, $($rest),+> Downgrade for ($first, $($rest),+)
        where
            $first: Downgrade,
            $($rest: Downgrade),+
        {
            type Target = ($first::Target, $($rest::Target),+);

            #[allow(non_snake_case)]
            fn downgrade(&self) -> Self::Target {
                let ($first, $($rest),+) = self;
                ($first.downgrade(), $($rest.downgrade()),+)
            }
        }

        impl<$first, $($rest),+> Upgrade for ($first, $($rest),+)
        where
            $first: Upgrade,
            $($rest: Upgrade),+
        {
            type Target = ($first::Target, $($rest::Target),+);

            #[allow(non_snake_case)]
            fn upgrade(&self) -> Option<Self::Target> {
                let ($first, $($rest),+) = self;
                Some(($first.upgrade()?, $($rest.upgrade()?),+))
            }
        }

    };
}

tuples!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16
);

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{rc::Rc, string::String, sync::Arc};

    fn downgrade_generic<D: Downgrade>(value: &D) -> D::Target {
        value.downgrade()
    }

    #[test]
    fn rc_downgrade_then_upgrade_returns_same_allocation() {
        let rc = Rc::new(5);
        let weak = rc.downgrade();

        let upgraded = weak.upgrade().expect("rc is alive");
        assert!(Rc::ptr_eq(&rc, &upgraded));
    }

    #[test]
    fn rc_weak_fails_to_upgrade_after_drop() {
        let rc = Rc::new(5);
        let weak = rc.downgrade();
        drop(rc);

        assert!(Upgrade::upgrade(&weak).is_none());
    }

    #[test]
    fn rc_weak_downgrade_is_a_clone() {
        let rc = Rc::new("x");
        let weak = Rc::downgrade(&rc);
        let weak2 = Downgrade::downgrade(&weak);

        assert!(Upgrade::upgrade(&weak2).is_some());
        drop(rc);
        assert!(Upgrade::upgrade(&weak2).is_none());
        assert!(Upgrade::upgrade(&weak).is_none());
    }

    #[test]
    fn arc_downgrade_then_upgrade_returns_same_allocation() {
        let arc = Arc::new(String::from("shared"));
        let weak = arc.downgrade();

        let upgraded = Upgrade::upgrade(&weak).expect("arc is alive");
        assert!(Arc::ptr_eq(&arc, &upgraded));
        drop(upgraded);
        drop(arc);
        assert!(Upgrade::upgrade(&weak).is_none());
    }

    #[test]
    fn arc_weak_downgrade_is_a_clone() {
        let arc = Arc::new(1u8);
        let weak = Arc::downgrade(&arc);
        let weak2 = Downgrade::downgrade(&weak);

        assert!(Upgrade::upgrade(&weak2).is_some());
        drop(arc);
        assert!(Upgrade::upgrade(&weak2).is_none());
    }

    #[test]
    fn references_delegate_to_the_underlying_value() {
        let rc = Rc::new(1);
        let mut rc_mut = Rc::clone(&rc);

        let weak_ref = downgrade_generic(&&rc);
        let weak_mut = downgrade_generic(&&mut rc_mut);

        assert!(Rc::ptr_eq(&weak_ref.upgrade().unwrap(), &rc));
        assert!(Rc::ptr_eq(&weak_mut.upgrade().unwrap(), &rc));

        let weak = Rc::downgrade(&rc);
        assert!(Upgrade::upgrade(&&weak).is_some());
        let mut weak_mut_binding = weak.clone();
        assert!(Upgrade::upgrade(&&mut weak_mut_binding).is_some());

        drop(rc_mut);
        drop(weak_ref);
    }

    #[test]
    fn primitives_round_trip_unchanged() {
        assert_eq!(7u8.downgrade().upgrade(), Some(7u8));
        assert_eq!((-3i64).downgrade().upgrade(), Some(-3i64));
        assert_eq!(1.5f32.downgrade().upgrade(), Some(1.5f32));
        assert_eq!(true.downgrade().upgrade(), Some(true));
        assert_eq!('x'.downgrade().upgrade(), Some('x'));
        assert_eq!(usize::MAX.downgrade().upgrade(), Some(usize::MAX));
        assert_eq!(u128::MAX.downgrade().upgrade(), Some(u128::MAX));
    }

    #[test]
    fn option_some_downgrades_inner_value() {
        let rc = Rc::new(9);
        let weak = Some(Rc::clone(&rc)).downgrade();

        assert!(weak.is_some());
        let upgraded = weak.upgrade().expect("inner value is alive");
        assert!(Rc::ptr_eq(&upgraded.unwrap(), &rc));
    }

    #[test]
    fn option_some_with_dead_inner_fails_to_upgrade() {
        let rc = Rc::new(9);
        let weak = Some(Rc::clone(&rc)).downgrade();
        drop(rc);

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn option_none_downgrades_to_none() {
        let none: Option<Rc<i32>> = None;
        assert!(none.downgrade().is_none());
    }

    #[test]
    fn option_none_upgrades_to_none_value() {
        let none: Option<Weak<i32>> = None;
        assert!(matches!(Upgrade::upgrade(&none), Some(None)));
    }

    #[test]
    fn single_element_tuple_round_trips() {
        let a = Rc::new(1);
        let weak = (Rc::clone(&a),).downgrade();

        let (upgraded,) = weak.upgrade().expect("all members alive");
        assert!(Rc::ptr_eq(&upgraded, &a));
    }

    #[test]
    fn mixed_tuple_round_trips() {
        let a = Rc::new(1);
        let b = Arc::new(2);
        let weak = (Rc::clone(&a), Arc::clone(&b), 3u32, 'c').downgrade();

        let (ua, ub, n, c) = weak.upgrade().expect("all members alive");
        assert!(Rc::ptr_eq(&ua, &a));
        assert!(Arc::ptr_eq(&ub, &b));
        assert_eq!(n, 3);
        assert_eq!(c, 'c');
    }

    #[test]
    fn tuple_upgrade_fails_if_any_member_is_dead() {
        let a = Rc::new(1);
        let b = Rc::new(2);
        let c = Rc::new(3);
        let weak = (Rc::clone(&a), Rc::clone(&b), Rc::clone(&c)).downgrade();

        drop(b);
        assert!(weak.upgrade().is_none());

        // The remaining members are still reachable individually.
        assert!(weak.0.upgrade().is_some());
        assert!(weak.2.upgrade().is_some());
    }

    #[test]
    fn tuple_upgrade_fails_when_first_or_last_member_is_dead() {
        let a = Rc::new(1);
        let b = Rc::new(2);
        let weak = (Rc::clone(&a), Rc::clone(&b)).downgrade();

        drop(a);
        assert!(weak.upgrade().is_none());

        let a = Rc::new(1);
        let weak = (Rc::clone(&a), Rc::clone(&b)).downgrade();
        drop(b);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn largest_supported_tuple_round_trips() {
        let rcs: [Rc<u8>; 16] = core::array::from_fn(|i| Rc::new(i as u8));
        let [
            r0,
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
            r13,
            r14,
            r15,
        ] = rcs.clone();
        let weak = (
            r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15,
        )
            .downgrade();

        let upgraded = weak.upgrade().expect("all members alive");
        assert_eq!(*upgraded.0, 0);
        assert_eq!(*upgraded.15, 15);

        drop(upgraded);
        drop(rcs);
        assert!(weak.upgrade().is_none());
    }
}
