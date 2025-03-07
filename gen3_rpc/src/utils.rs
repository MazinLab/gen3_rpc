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
    }

    impl<T: DDCChannel + Send + Sync + 'static, const C: u32> ChannelAllocator<T, C> {
        pub fn new(caps: DDCCapabilities) -> Self {
            ChannelAllocator {
                capabilites: caps,
                channels: Arc::new(Mutex::new(HashMap::new())),
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
                                inner: Arc::new(RwLock::new(T::from_actualized(actualized)?)),
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
