pub use goerdet::LocalSpawner;

use crate::{Downgrade, Upgrade};

pub trait SpawnerExt<'a>: LocalSpawner<'a> {
    fn spawn_task<'b: 'a, F, U>(&self, task: F) -> Self::Task
    where
        F: FnOnce(Self) -> U,
        U: Future<Output = ()> + 'b,
        Self: Clone + Sized,
    {
        self.spawn(task(self.clone()))
    }

    fn spawn_with<'b: 'a, F, W, U>(&self, value: W, map: F) -> Self::Task
    where
        W: Downgrade,
        W::Target: Upgrade + 'b,
        F: FnOnce(Self, <W::Target as Upgrade>::Target) -> U + 'b,
        U: Future<Output = ()> + 'b,
        Self: Clone + Sized + 'a,
    {
        let downgraded_value = value.downgrade();

        self.spawn_task(move |this| async move {
            let Some(strong_self) = downgraded_value.upgrade() else {
                return;
            };

            map(this, strong_self).await
        })
    }
}

impl<'a, T> SpawnerExt<'a> for T where T: LocalSpawner<'a> {}

pub trait DriverableSpawner<'a>: LocalSpawner<'a> {
    fn tick(&self) -> bool;
}

#[cfg(feature = "compio")]
impl DriverableSpawner<'static> for compio::runtime::Runtime {
    fn tick(&self) -> bool {
        let remaining_tasks = self.run();
        if remaining_tasks {
            self.poll_with(Some(std::time::Duration::ZERO));
        } else {
            self.poll();
        }

        remaining_tasks
    }
}
