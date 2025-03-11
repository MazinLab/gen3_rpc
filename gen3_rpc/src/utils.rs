pub mod little_fixed {
    use std::ops::Deref;
    macro_rules! little_fixedi {
        ($name:ident, $dname:ident, $inner_type:ty) => {
            pub struct $dname {
                inner: $inner_type,
                bits: usize,
            }

            impl $dname {
                pub fn min(&self) -> $inner_type {
                    -(1 << (self.bits - 1))
                }

                pub fn max(&self) -> $inner_type {
                    (1 << (self.bits - 1)) - 1
                }

                pub fn mask(&self) -> $inner_type {
                    self.inner & ((1 << self.bits) - 1)
                }

                pub fn try_new(value: $inner_type, bits: usize) -> Option<Self> {
                    let n = $dname { inner: value, bits };
                    if value >= n.min() && value <= n.max() {
                        Some(n)
                    } else {
                        None
                    }
                }
            }

            impl Deref for $dname {
                type Target = $inner_type;

                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            pub struct $name<const BITS: usize> {
                inner: $inner_type,
            }

            impl<const BITS: usize> $name<BITS> {
                const TS_MIN: $inner_type = -(1 << (BITS - 1));
                const TS_MAX: $inner_type = (1 << (BITS - 1)) - 1;

                pub fn mask(&self) -> $inner_type {
                    self.inner & ((1 << BITS) - 1)
                }
            }

            impl<const BITS: usize> Deref for $name<BITS> {
                type Target = $inner_type;

                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl<const BITS: usize> TryFrom<$inner_type> for $name<BITS> {
                type Error = ();

                fn try_from(value: $inner_type) -> Result<Self, Self::Error> {
                    if value >= Self::TS_MIN && value <= Self::TS_MAX {
                        Ok($name { inner: value })
                    } else {
                        Err(())
                    }
                }
            }
        };
    }
    macro_rules! little_fixedu {
        ($name:ident, $dname:ident, $inner_type:ty) => {
            pub struct $dname {
                inner: $inner_type,
                bits: usize,
            }

            impl $dname {
                pub fn min(&self) -> $inner_type {
                    0
                }

                pub fn max(&self) -> $inner_type {
                    (1 << (self.bits)) - 1
                }

                pub fn mask(&self) -> $inner_type {
                    self.inner & ((1 << self.bits) - 1)
                }

                pub fn try_new(value: $inner_type, bits: usize) -> Option<Self> {
                    let n = $dname { inner: value, bits };
                    if value >= n.min() && value <= n.max() {
                        Some(n)
                    } else {
                        None
                    }
                }
            }

            impl Deref for $dname {
                type Target = $inner_type;

                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            pub struct $name<const BITS: usize> {
                inner: $inner_type,
            }

            impl<const BITS: usize> $name<BITS> {
                const TS_MIN: $inner_type = 0;
                const TS_MAX: $inner_type = (1 << (BITS)) - 1;

                pub fn mask(&self) -> $inner_type {
                    self.inner & ((1 << BITS) - 1)
                }
            }

            impl<const BITS: usize> Deref for $name<BITS> {
                type Target = $inner_type;

                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl<const BITS: usize> TryFrom<$inner_type> for $name<BITS> {
                type Error = ();

                fn try_from(value: $inner_type) -> Result<Self, Self::Error> {
                    if value >= Self::TS_MIN && value <= Self::TS_MAX {
                        Ok($name { inner: value })
                    } else {
                        Err(())
                    }
                }
            }
        };
    }

    little_fixedi!(LittleFixedI8, LittleFixedDynI8, i8);
    little_fixedu!(LittleFixedU8, LittleFixedDynU8, u8);
    little_fixedi!(LittleFixedI16, LittleFixedDynI16, i16);
    little_fixedu!(LittleFixedU16, LittleFixedDynU16, u16);
    little_fixedi!(LittleFixedI32, LittleFixedDynI32, i32);
    little_fixedu!(LittleFixedU32, LittleFixedDynU32, u32);
    little_fixedi!(LittleFixedI64, LittleFixedDynI64, i64);
    little_fixedu!(LittleFixedU64, LittleFixedDynU64, u64);
    little_fixedi!(LittleFixedI128, LittleFixedDynI128, i128);
    little_fixedu!(LittleFixedU128, LittleFixedDynU128, u128);
}

pub mod client {}

pub mod server {
    use crate::{
        gen3rpc_capnp,
        server::{DDCChannel, DRState, DroppableReferenceImpl, DDC},
        ChannelAllocationError, DDCCapabilities, DDCChannelConfig,
    };

    use std::{
        collections::{hash_map::Entry, HashMap},
        marker::PhantomData,
        sync::{Arc, Mutex, RwLock},
    };

    #[derive(Clone)]
    pub struct ChannelAllocator<T: DDCChannel + Send + Sync + 'static, const C: u32> {
        capabilites: DDCCapabilities,
        channels:
            Arc<Mutex<HashMap<u32, DroppableReferenceImpl<T, gen3rpc_capnp::ddc_channel::Client>>>>,
        shared: T::Shared,
    }

    impl<T: DDCChannel + Send + Sync + 'static, const C: u32> ChannelAllocator<T, C> {
        pub fn new(caps: DDCCapabilities, shared: T::Shared) -> Self {
            ChannelAllocator {
                capabilites: caps,
                channels: Arc::new(Mutex::new(HashMap::new())),
                shared,
            }
        }

        pub fn allocate_channel_inner(
            &self,
            config: DDCChannelConfig,
            exclusive: bool,
            retrieve: bool,
        ) -> Result<
            DroppableReferenceImpl<T, gen3rpc_capnp::ddc_channel::Client>,
            ChannelAllocationError,
        > {
            let mut l = self.channels.lock().unwrap();
            match config.actualize() {
                Ok(actualized) => {
                    let entry = l.entry(actualized.dest_bin);
                    match entry {
                        Entry::Occupied(o) => {
                            let mut state = o.get().state.write().unwrap();
                            match *state {
                                DRState::Unshared => {
                                    let mut iloc = o.get().inner.write().unwrap();
                                    if retrieve && iloc.get() != actualized {
                                        return Err(ChannelAllocationError::DestinationInUse);
                                    }
                                    iloc.set(actualized.erase())?;
                                    if exclusive {
                                        *state = DRState::Exclusive;
                                    } else {
                                        *state = DRState::Shared(1);
                                    }
                                    return Ok(DroppableReferenceImpl {
                                        state: o.get().state.clone(),
                                        inner: o.get().inner.clone(),
                                        stale: false,
                                        phantom: PhantomData,
                                    });
                                }
                                DRState::Exclusive => {
                                    return Err(ChannelAllocationError::DestinationInUse);
                                }
                                DRState::Shared(i) => {
                                    if exclusive {
                                        return Err(ChannelAllocationError::DestinationInUse);
                                    }
                                    let iloc = o.get().inner.write().unwrap();
                                    if iloc.get() == actualized {
                                        *state = DRState::Shared(i + 1);
                                        return Ok(DroppableReferenceImpl {
                                            state: o.get().state.clone(),
                                            inner: o.get().inner.clone(),
                                            stale: false,
                                            phantom: PhantomData,
                                        });
                                    } else {
                                        return Err(ChannelAllocationError::DestinationInUse);
                                    }
                                }
                            }
                        }
                        Entry::Vacant(v) => {
                            if retrieve {
                                return Err(ChannelAllocationError::DestinationInUse);
                            }
                            let dr = DroppableReferenceImpl {
                                state: Arc::new(RwLock::new(DRState::Shared(1))),
                                inner: Arc::new(RwLock::new(T::from_actualized(
                                    actualized,
                                    self.capabilites,
                                    self.shared.clone(),
                                )?)),
                                stale: false,
                                phantom: PhantomData,
                            };
                            v.insert(dr.clone_weak());
                            return Ok(dr);
                        }
                    }
                }
                Err(erased) => {
                    for i in 0..2048 {
                        if let Some(ent) = l.get(&i) {
                            let mut slock = ent.state.write().unwrap();
                            match *slock {
                                DRState::Shared(i) => {
                                    if exclusive {
                                        continue;
                                    }
                                    let iloc = ent.inner.read().unwrap();
                                    if iloc.get() == erased.with_dest(i as u32) {
                                        *slock = DRState::Shared(i + 1);
                                        return Ok(ent.clone_weak());
                                    }
                                }
                                DRState::Unshared => {
                                    let mut iloc = ent.inner.write().unwrap();
                                    if retrieve && iloc.get() != erased.with_dest(i) {
                                        continue;
                                    }
                                    if exclusive {
                                        *slock = DRState::Exclusive;
                                    } else {
                                        *slock = DRState::Shared(1);
                                    }
                                    iloc.set(erased)?;
                                    return Ok(ent.clone_weak());
                                }
                                DRState::Exclusive => {}
                            }
                        }
                    }
                    for i in 0..2048 {
                        if let Entry::Vacant(v) = l.entry(i) {
                            let dr = DroppableReferenceImpl {
                                state: Arc::new(RwLock::new(DRState::Shared(1))),
                                inner: Arc::new(RwLock::new(T::from_actualized(
                                    erased.with_dest(i),
                                    self.capabilites,
                                    self.shared.clone(),
                                )?)),
                                stale: false,
                                phantom: PhantomData,
                            };
                            v.insert(dr.clone_weak());
                            return Ok(dr);
                        }
                    }
                }
            }
            Err(ChannelAllocationError::OutOfChannels)
        }
    }

    impl<T: DDCChannel + Send + Sync + 'static + Clone, const C: u32> DDC<T>
        for ChannelAllocator<T, C>
    {
        fn capabilities(&self) -> DDCCapabilities {
            self.capabilites
        }

        fn allocate_channel(
            &self,
            config: DDCChannelConfig,
        ) -> Result<
            DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>,
            ChannelAllocationError,
        > {
            self.allocate_channel_inner(config, false, false)
        }

        fn allocate_channel_mut(
            &self,
            config: DDCChannelConfig,
        ) -> Result<
            DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>,
            ChannelAllocationError,
        > {
            self.allocate_channel_inner(config, true, false)
        }

        fn retrieve_channel(
            &self,
            config: DDCChannelConfig,
        ) -> Option<DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>> {
            self.allocate_channel_inner(config, false, true).ok()
        }
    }
}
