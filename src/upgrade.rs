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
        self.as_ref().map(|value| value.upgrade())
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
