use gen3_rpc::{
    gen3rpc_capnp::{
        self,
        capture::capture_tap::Which::{DdcIq, Phase, RawIq},
    },
    ActualizedDDCChannelConfig, AttenError, Attens, CaptureError, ChannelAllocationError,
    ChannelConfigError, DDCChannelConfig, ErasedDDCChannelConfig, FrequencyError, Hertz, SnapAvg,
};
use num::Complex;
use num_complex::Complex64;

use std::{
    collections::HashMap,
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex, RwLock, RwLockWriteGuard, TryLockError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use capnp::{
    capability::{FromClientHook, Promise},
    traits::FromPointerBuilder,
};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{future::try_join_all, AsyncReadExt, FutureExt, TryFutureExt};

struct DSPScaleMock {
    fft: u16,
}

pub trait DSPScale {
    fn set(&mut self, v: u16) -> Result<u16, u16>;
    fn get(&self) -> u16;
}

impl DSPScale for DSPScaleMock {
    fn set(&mut self, v: u16) -> Result<u16, u16> {
        let vp = v & 0xfff;
        self.fft = vp;
        if vp == v {
            Ok(vp)
        } else {
            Err(vp)
        }
    }

    fn get(&self) -> u16 {
        self.fft
    }
}

struct IFBoardMock {
    lo: Hertz,
    attens: Attens,
}

pub trait IFBoard {
    fn set_lo(&mut self, v: Hertz) -> Result<Hertz, FrequencyError>;
    fn get_lo(&self) -> Hertz;
    fn set_attens(&mut self, a: Attens) -> Result<Attens, AttenError>;
    fn get_attens(&self) -> Attens;
}

pub trait Snap {
    fn get(&self) -> gen3_rpc::Snap;
    fn average(&self) -> gen3_rpc::SnapAvg {
        let b = self.get();
        match b {
            gen3_rpc::Snap::Raw(v) => SnapAvg::Raw(
                Complex64::new(
                    v.iter().map(|p| p.re as f64).sum(),
                    v.iter().map(|p| p.im as f64).sum(),
                ) / v.len() as f64,
            ),
            gen3_rpc::Snap::DdcIQ(vs) => SnapAvg::DdcIQ(
                vs.into_iter()
                    .map(|v| {
                        Complex64::new(
                            v.iter().map(|p| p.re as f64).sum(),
                            v.iter().map(|p| p.im as f64).sum(),
                        ) / v.len() as f64
                    })
                    .collect(),
            ),
            gen3_rpc::Snap::Phase(vs) => SnapAvg::Phase(
                vs.into_iter()
                    .map(|v| v.iter().map(|p| *p as f64).sum::<f64>() / v.len() as f64)
                    .collect(),
            ),
        }
    }
    fn rms(&self) -> gen3_rpc::SnapAvg {
        let avgs = self.average();
        let b = self.get();
        match (avgs, b) {
            (gen3_rpc::SnapAvg::Raw(a), gen3_rpc::Snap::Raw(v)) => SnapAvg::Raw(Complex64::new(
                (v.iter()
                    .map(|p| ((p.re as f64) - a.re).powi(2))
                    .sum::<f64>()
                    / v.len() as f64)
                    .sqrt(),
                (v.iter()
                    .map(|p| ((p.im as f64) - a.im).powi(2))
                    .sum::<f64>()
                    / v.len() as f64)
                    .sqrt(),
            )),
            (gen3_rpc::SnapAvg::DdcIQ(avgs), gen3_rpc::Snap::DdcIQ(vs)) => SnapAvg::DdcIQ(
                vs.into_iter()
                    .zip(avgs)
                    .map(|(v, a)| {
                        Complex64::new(
                            (v.iter()
                                .map(|p| ((p.re as f64) - a.re).powi(2))
                                .sum::<f64>()
                                / v.len() as f64)
                                .sqrt(),
                            (v.iter()
                                .map(|p| ((p.im as f64) - a.im).powi(2))
                                .sum::<f64>()
                                / v.len() as f64)
                                .sqrt(),
                        )
                    })
                    .collect(),
            ),
            (gen3_rpc::SnapAvg::Phase(avgs), gen3_rpc::Snap::Phase(vs)) => SnapAvg::Phase(
                vs.into_iter()
                    .zip(avgs)
                    .map(|(v, a)| {
                        (v.iter().map(|p| ((*p as f64) - a).powi(2)).sum::<f64>() / v.len() as f64)
                            .sqrt()
                    })
                    .collect(),
            ),
            _ => unreachable!("Shouldn't get here"),
        }
    }
}

impl Snap for gen3_rpc::Snap {
    fn get(&self) -> gen3_rpc::Snap {
        self.clone()
    }
}

impl IFBoard for IFBoardMock {
    fn set_lo(&mut self, v: Hertz) -> Result<Hertz, FrequencyError> {
        self.lo = v;
        Ok(v)
    }
    fn get_lo(&self) -> Hertz {
        self.lo
    }
    fn set_attens(&mut self, a: Attens) -> Result<Attens, AttenError> {
        self.attens = a;
        Ok(a)
    }
    fn get_attens(&self) -> Attens {
        self.attens
    }
}

struct DACTableMock {
    values: Box<[Complex<i16>; 524288]>,
}

trait DACTable {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>);
    fn get(&self) -> Box<[Complex<i16>; 524288]>;
}

impl DACTable for DACTableMock {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>) {
        self.values = v;
    }
    fn get(&self) -> Box<[Complex<i16>; 524288]> {
        self.values.clone()
    }
}

trait DDCChannel {
    fn get(&self) -> ActualizedDDCChannelConfig;
    fn set(&mut self, replacement: ErasedDDCChannelConfig) -> Result<(), ChannelConfigError>;

    fn set_source(&mut self, source_bin: u32) -> Result<(), ChannelConfigError>;
    fn set_ddc_freq(&mut self, ddc_freq: i32) -> Result<(), ChannelConfigError>;
    fn set_rotation(&mut self, rotation: i32) -> Result<(), ChannelConfigError>;
    fn set_center(&mut self, center: Complex<i32>) -> Result<(), ChannelConfigError>;

    fn get_baseband_freq(&self) -> Hertz;
    fn get_dest(&self) -> u32 {
        self.get().dest_bin
    }
}

impl DDCChannel for ActualizedDDCChannelConfig {
    fn get(&self) -> ActualizedDDCChannelConfig {
        self.clone()
    }
    fn set(&mut self, replacement: ErasedDDCChannelConfig) -> Result<(), ChannelConfigError> {
        self.source_bin = replacement.source_bin;
        self.ddc_freq = replacement.ddc_freq;
        self.rotation = replacement.rotation;
        self.center = replacement.center;
        Ok(())
    }
    fn set_source(&mut self, source_bin: u32) -> Result<(), ChannelConfigError> {
        self.source_bin = source_bin;
        Ok(())
    }
    fn set_ddc_freq(&mut self, ddc_freq: i32) -> Result<(), ChannelConfigError> {
        self.ddc_freq = ddc_freq;
        Ok(())
    }
    fn set_rotation(&mut self, rotation: i32) -> Result<(), ChannelConfigError> {
        self.rotation = rotation;
        Ok(())
    }
    fn set_center(&mut self, center: Complex<i32>) -> Result<(), ChannelConfigError> {
        self.center = center;
        Ok(())
    }
    fn get_baseband_freq(&self) -> Hertz {
        Hertz::new(self.ddc_freq as i64, i32::MAX as i64)
    }
}

struct Gen3BoardImpl {
    ddc: DDCImpl,
    dac_table: DACTableImpl,
    capture: CaptureImpl,
    dsp_scale: DSPScaleImpl,
    if_board: IFBoardImpl,
}

struct ResultImpl<T, E> {
    inner: Result<T, E>,
}

#[derive(PartialEq, Eq)]
enum DRState {
    Unshared,
    Exclusive,
    Shared(usize),
}

struct DroppableReferenceImpl<T: 'static + Send + Sync, C> {
    state: Arc<RwLock<DRState>>,
    inner: Arc<RwLock<T>>,
    stale: bool,
    phantom: PhantomData<fn() -> C>, //TODO: Should this be Send+Sync
}

impl<T: Send + Sync, C> Drop for DroppableReferenceImpl<T, C> {
    fn drop(&mut self) {
        if !self.stale {
            let mut i = self.state.write().unwrap();
            match *i {
                DRState::Unshared => {}
                DRState::Exclusive => *i = DRState::Unshared,
                DRState::Shared(1) => *i = DRState::Unshared,
                DRState::Shared(rc) => *i = DRState::Shared(rc - 1),
            }
        }
    }
}

struct HiddenClient<T: Clone + FromClientHook> {
    client: T,
}

// Probably need to send a PR so this can be derived from the implementation that follows
impl<T: Clone + FromClientHook>
    gen3rpc_capnp::result::Server<capnp::any_pointer::Owned, capnp::any_pointer::Owned>
    for ResultImpl<HiddenClient<T>, HiddenClient<T>>
{
    fn get(
        &mut self,
        _: gen3rpc_capnp::result::GetParams<capnp::any_pointer::Owned, capnp::any_pointer::Owned>,
        mut response: gen3rpc_capnp::result::GetResults<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let r = response.get().init_result();
        match &self.inner {
            Ok(t) => {
                r.init_ok()
                    .set_as_capability(t.client.clone().into_client_hook());
            }
            Err(e) => {
                r.init_error()
                    .set_as_capability(e.client.clone().into_client_hook());
            }
        }
        Promise::ok(())
    }
    fn is_ok(
        &mut self,
        _: gen3rpc_capnp::result::IsOkParams<capnp::any_pointer::Owned, capnp::any_pointer::Owned>,
        mut response: gen3rpc_capnp::result::IsOkResults<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response.get().set_some(self.inner.is_ok());
        Promise::ok(())
    }
    fn unwrap(
        &mut self,
        _: gen3rpc_capnp::result::UnwrapParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: gen3rpc_capnp::result::UnwrapResults<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        match &self.inner {
            Ok(t) => {
                response
                    .get()
                    .init_some()
                    .set_as_capability(t.client.clone().into_client_hook());
                Promise::ok(())
            }
            Err(_) => Promise::err(capnp::Error {
                kind: capnp::ErrorKind::Failed,
                extra: "Tried to unwrap an error".into(),
            }),
        }
    }
    fn unwrap_or(
        &mut self,
        params: gen3rpc_capnp::result::UnwrapOrParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: gen3rpc_capnp::result::UnwrapOrResults<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response
            .get()
            .init_result()
            .set_as_capability(match &self.inner {
                Ok(t) => t.client.clone().into_client_hook(),
                Err(_) => {
                    let p = pry!(pry!(params.get()).get_or());
                    let k: T = pry!(p.clone().get_as_capability());
                    k.into_client_hook()
                }
            });
        Promise::ok(())
    }
}

impl<T, E, C, D> gen3rpc_capnp::result::Server<C, D> for ResultImpl<T, E>
where
    T: capnp::traits::SetterInput<C> + Clone,
    E: capnp::traits::SetterInput<D> + Clone,
    C: capnp::traits::Owned,
    D: capnp::traits::Owned,
{
    fn get(
        &mut self,
        _: gen3rpc_capnp::result::GetParams<C, D>,
        mut response: gen3rpc_capnp::result::GetResults<C, D>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut r = response.get().init_result();
        match &self.inner {
            Ok(t) => pry!(r.set_ok(t.clone())),
            Err(e) => pry!(r.set_error(e.clone())),
        }
        Promise::ok(())
    }

    fn is_ok(
        &mut self,
        _: gen3rpc_capnp::result::IsOkParams<C, D>,
        mut response: gen3rpc_capnp::result::IsOkResults<C, D>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response.get().set_some(self.inner.is_ok());
        Promise::ok(())
    }

    fn unwrap(
        &mut self,
        _: gen3rpc_capnp::result::UnwrapParams<C, D>,
        mut response: gen3rpc_capnp::result::UnwrapResults<C, D>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        match &self.inner {
            Ok(t) => {
                pry!(response.get().set_some(t.clone()));
                Promise::ok(())
            }
            Err(_) => Promise::err(capnp::Error {
                kind: capnp::ErrorKind::Failed,
                extra: "Tried to unwrap an error".into(),
            }),
        }
    }

    fn unwrap_or(
        &mut self,
        params: gen3rpc_capnp::result::UnwrapOrParams<C, D>,
        mut response: gen3rpc_capnp::result::UnwrapOrResults<C, D>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        match &self.inner {
            Ok(t) => {
                pry!(response.get().set_result(t.clone()));
            }
            Err(_) => {
                let or = pry!(pry!(params.get()).get_or());
                pry!(response.get().set_result(or));
            }
        }
        Promise::ok(())
    }
}

impl<T: Send + Sync, C> DroppableReferenceImpl<T, C> {
    fn lock_mut(&self) -> Result<RwLockWriteGuard<'_, T>, capnp::Error> {
        let ls = self.state.read().unwrap();
        match *ls {
            DRState::Exclusive => Ok(self.inner.write().unwrap()),
            _ => Err(capnp::Error::failed("Client does not have an exclusive reference to resource but called an exclusive method".into()))
        }
    }

    fn try_clone(&self) -> Result<Self, TryLockError<RwLockWriteGuard<DRState>>> {
        if self.stale {
            unreachable!("Client tried to clone a stale reference, which means it innapropriately leaked it past a drop, dropMut, or tryIntoMut call")
        }
        let mut i = self.state.write().unwrap();
        match *i {
            DRState::Exclusive => Err(TryLockError::WouldBlock),
            DRState::Unshared => {
                *i = DRState::Shared(1usize);
                Ok(Self {
                    inner: self.inner.clone(),
                    state: self.state.clone(),
                    stale: false,
                    phantom: PhantomData,
                })
            }
            DRState::Shared(u) => {
                *i = DRState::Shared(u + 1);
                Ok(Self {
                    inner: self.inner.clone(),
                    state: self.state.clone(),
                    stale: false,
                    phantom: PhantomData,
                })
            }
        }
    }

    fn clone_weak(&self) -> Self {
        DroppableReferenceImpl {
            inner: self.inner.clone(),
            state: self.state.clone(),
            stale: self.stale,
            phantom: PhantomData,
        }
    }

    fn check_shared(&self) -> Result<(), capnp::Error> {
        if self.stale {
            Err(capnp::Error {
                kind: capnp::ErrorKind::Failed,
                extra: "Attempted to call a method on a stale reference".into(),
            })
        } else {
            Ok(())
        }
    }

    fn check_exc(&self) -> Result<(), capnp::Error> {
        self.check_shared()?;
        if *self.state.read().unwrap() != DRState::Exclusive {
            return Err(capnp::Error {
                kind: capnp::ErrorKind::Failed,
                extra: "Attempted to call a mutable method on a shared reference".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
struct DDCImpl {
    inner: Arc<Mutex<HashMap<u16, DDCChannelImpl>>>,
}

#[derive(Clone)]
struct CaptureImpl {
    inner: Arc<Mutex<()>>,
}

macro_rules! droppable_reference {
    ($inner_trait:path, $client_type:path) => {
        impl<T: $inner_trait + Send + Sync> gen3rpc_capnp::droppable_reference::Server
            for DroppableReferenceImpl<T, $client_type>
        {
            fn drop(
                &mut self,
                _: gen3rpc_capnp::droppable_reference::DropParams,
                _: gen3rpc_capnp::droppable_reference::DropResults,
            ) -> capnp::capability::Promise<(), capnp::Error> {
                let mut i = self.state.write().unwrap();
                match *i {
                    DRState::Unshared => unreachable!(),
                    DRState::Exclusive => {
                        *i = DRState::Unshared;
                    }
                    DRState::Shared(1) => {
                        *i = DRState::Unshared;
                    }
                    DRState::Shared(rc) => {
                        *i = DRState::Shared(rc - 1);
                    }
                }
                Promise::ok(())
            }

            fn is_mut(
                &mut self,
                _: gen3rpc_capnp::droppable_reference::IsMutParams,
                mut response: gen3rpc_capnp::droppable_reference::IsMutResults,
            ) -> capnp::capability::Promise<(), capnp::Error> {
                let i = self.state.read().unwrap();
                match *i {
                    DRState::Unshared => unreachable!(),
                    DRState::Exclusive => {
                        response.get().set_mutable(true);
                    }
                    DRState::Shared(_) => {
                        response.get().set_mutable(false);
                    }
                }
                Promise::ok(())
            }

            fn drop_mut(
                &mut self,
                _: gen3rpc_capnp::droppable_reference::DropMutParams,
                mut response: gen3rpc_capnp::droppable_reference::DropMutResults,
            ) -> capnp::capability::Promise<(), capnp::Error> {
                let mut i = self.state.write().unwrap();
                *i = DRState::Shared(1);
                self.stale = true;
                let c: $client_type = capnp_rpc::new_client(DroppableReferenceImpl {
                    state: self.state.clone(),
                    inner: self.inner.clone(),
                    stale: false,
                    phantom: PhantomData,
                });
                response
                    .get()
                    .init_nonmut()
                    .set_as_capability(c.into_client_hook());
                Promise::ok(())
            }

            fn try_into_mut(
                &mut self,
                _: gen3rpc_capnp::droppable_reference::TryIntoMutParams,
                mut response: gen3rpc_capnp::droppable_reference::TryIntoMutResults,
            ) -> capnp::capability::Promise<(), capnp::Error> {
                println!("Attpmption to turn something into a mutable ref");
                let mut i = self.state.write().unwrap();
                let resimp = match *i {
                    DRState::Unshared => unreachable!(),
                    DRState::Shared(1) | DRState::Exclusive => {
                        self.stale = true;
                        *i = DRState::Exclusive;

                        let d: $client_type = capnp_rpc::new_client(DroppableReferenceImpl {
                            state: self.state.clone(),
                            inner: self.inner.clone(),
                            stale: false,
                            phantom: PhantomData,
                        });
                        let c = HiddenClient { client: d };
                        ResultImpl { inner: Ok(c) }
                    }
                    DRState::Shared(_) => {
                        self.stale = true;

                        let d: $client_type = capnp_rpc::new_client(DroppableReferenceImpl {
                            state: self.state.clone(),
                            inner: self.inner.clone(),
                            stale: false,
                            phantom: PhantomData,
                        });
                        let c = HiddenClient { client: d };
                        ResultImpl { inner: Err(c) }
                    }
                };
                let c = capnp_rpc::new_client(resimp);
                response.get().set_maybe_mut(c);
                Promise::ok(())
            }
        }
    };
}

type DSPScaleImpl = DroppableReferenceImpl<DSPScaleMock, gen3rpc_capnp::dsp_scale::Client>;
type IFBoardImpl = DroppableReferenceImpl<IFBoardMock, gen3rpc_capnp::if_board::Client>;
type DACTableImpl = DroppableReferenceImpl<DACTableMock, gen3rpc_capnp::dac_table::Client>;
type DDCChannelImpl =
    DroppableReferenceImpl<ActualizedDDCChannelConfig, gen3rpc_capnp::ddc_channel::Client>;

droppable_reference!(DSPScale, gen3rpc_capnp::dsp_scale::Client);
droppable_reference!(IFBoard, gen3rpc_capnp::if_board::Client);
droppable_reference!(DACTable, gen3rpc_capnp::dac_table::Client);
droppable_reference!(Snap, gen3rpc_capnp::snap::Client);
droppable_reference!(DDCChannel, gen3rpc_capnp::ddc_channel::Client);

impl DDCImpl {
    fn allocate_channel_inner(
        &mut self,
        ccfg: gen3rpc_capnp::ddc_channel::channel_config::Reader,
        mode: DRState,
    ) -> Result<ResultImpl<gen3rpc_capnp::ddc_channel::Client, ChannelAllocationError>, capnp::Error>
    {
        let db = ccfg.get_destination_bin().which()?;
        let c = ccfg.get_center()?;
        let ccfg = DDCChannelConfig {
            source_bin: ccfg.get_source_bin(),
            ddc_freq: ccfg.get_ddc_freq(),
            rotation: ccfg.get_rotation(),
            dest_bin: match db {
                gen3rpc_capnp::ddc_channel::channel_config::destination_bin::Which::None(()) => {
                    None
                }
                gen3rpc_capnp::ddc_channel::channel_config::destination_bin::Which::Some(i) => {
                    Some(i)
                }
            },
            center: Complex::new(c.get_real(), c.get_imag()),
        };
        println!("Allocating channel with spec: {:#?}", ccfg);
        let mut hm = self.inner.lock().unwrap();
        match ccfg.actualize() {
            Ok(a) => {
                if let std::collections::hash_map::Entry::Vacant(e) = hm.entry(a.dest_bin as u16) {
                    println!("Allocated with dest {}", a.dest_bin);
                    let ddcimpl = DDCChannelImpl {
                        inner: Arc::new(RwLock::new(a)),
                        stale: false,
                        state: Arc::new(RwLock::new(mode)),
                        phantom: PhantomData,
                    };
                    e.insert(ddcimpl.clone_weak());
                    let res: ResultImpl<
                        gen3rpc_capnp::ddc_channel::Client,
                        ChannelAllocationError,
                    > = ResultImpl {
                        inner: Ok(capnp_rpc::new_client(ddcimpl)),
                    };
                    Ok(res)
                } else {
                    println!("Allocated failed");
                    let res: ResultImpl<
                        gen3rpc_capnp::ddc_channel::Client,
                        ChannelAllocationError,
                    > = ResultImpl {
                        inner: Err(ChannelAllocationError::DestinationInUse),
                    };
                    Ok(res)
                }
            }
            Err(mut c) => {
                for i in 0..2048 {
                    if !hm.contains_key(&i) {
                        c.dest_bin = Some(i as u32);
                        break;
                    }
                }
                match c.actualize() {
                    Ok(d) => {
                        println!("Allocated with dest {}", d.dest_bin);
                        let db = d.dest_bin;
                        let ddcimpl = DDCChannelImpl {
                            inner: Arc::new(RwLock::new(d)),
                            stale: false,
                            state: Arc::new(RwLock::new(mode)),
                            phantom: PhantomData,
                        };
                        hm.insert(db as u16, ddcimpl.clone_weak());
                        let res: ResultImpl<
                            gen3rpc_capnp::ddc_channel::Client,
                            ChannelAllocationError,
                        > = ResultImpl {
                            inner: Ok(capnp_rpc::new_client(ddcimpl)),
                        };
                        Ok(res)
                    }
                    Err(_) => {
                        println!("Allocated failed");
                        let res: ResultImpl<
                            gen3rpc_capnp::ddc_channel::Client,
                            ChannelAllocationError,
                        > = ResultImpl {
                            inner: Err(ChannelAllocationError::OutOfChannels),
                        };
                        Ok(res)
                    }
                }
            }
        }
    }
}

impl gen3rpc_capnp::ddc::Server for DDCImpl {
    fn capabilities(
        &mut self,
        _: gen3rpc_capnp::ddc::CapabilitiesParams,
        mut results: gen3rpc_capnp::ddc::CapabilitiesResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut fr = results.get().init_freq_resolution();
        fr.set_numerator(15624);
        fr.set_denominator(512);
        results.get().init_bin_control().set_full_swizzle(());

        results.get().set_freq_bits(16);
        results.get().set_rotation_bits(16);
        results.get().set_center_bits(16);

        Promise::ok(())
    }

    fn allocate_channel(
        &mut self,
        params: gen3rpc_capnp::ddc::AllocateChannelParams,
        mut response: gen3rpc_capnp::ddc::AllocateChannelResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let ccfg = pry!(pry!(params.get()).get_config());
        let res = pry!(self.allocate_channel_inner(ccfg, DRState::Shared(1)));
        response.get().set_result(capnp_rpc::new_client(res));
        Promise::ok(())
    }

    fn allocate_channel_mut(
        &mut self,
        params: gen3rpc_capnp::ddc::AllocateChannelMutParams,
        mut response: gen3rpc_capnp::ddc::AllocateChannelMutResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let ccfg = pry!(pry!(params.get()).get_config());
        let res = pry!(self.allocate_channel_inner(ccfg, DRState::Exclusive));
        response.get().set_result(capnp_rpc::new_client(res));
        Promise::ok(())
    }

    fn retrieve_channel(
        &mut self,
        _: gen3rpc_capnp::ddc::RetrieveChannelParams,
        _: gen3rpc_capnp::ddc::RetrieveChannelResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
}

impl<T: DDCChannel + Send + Sync> gen3rpc_capnp::ddc_channel::Server
    for DroppableReferenceImpl<T, gen3rpc_capnp::ddc_channel::Client>
{
    fn get(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetParams,
        mut response: gen3rpc_capnp::ddc_channel::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut r = response.get();
        let l = self.inner.read().unwrap().get();
        r.set_source_bin(l.source_bin);
        r.set_ddc_freq(l.ddc_freq);
        r.set_rotation(l.rotation);
        r.set_dest_bin(l.dest_bin);
        let mut c = r.init_center();
        c.set_real(l.center.re);
        c.set_imag(l.center.im);

        Promise::ok(())
    }

    fn set(
        &mut self,
        params: gen3rpc_capnp::ddc_channel::SetParams,
        mut response: gen3rpc_capnp::ddc_channel::SetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let mut il = pry!(self.lock_mut());
        let ccfg = pry!(pry!(params.get()).get_replace());
        let c = pry!(ccfg.get_center());
        let ccfg = ErasedDDCChannelConfig {
            source_bin: ccfg.get_source_bin(),
            ddc_freq: ccfg.get_ddc_freq(),
            rotation: ccfg.get_rotation(),
            center: Complex::new(c.get_real(), c.get_imag()),
        };
        response
            .get()
            .set_result(capnp_rpc::new_client(ResultImpl::<_, ChannelConfigError> {
                inner: il.set(ccfg),
            }));
        Promise::ok(())
    }

    fn set_source(
        &mut self,
        params: gen3rpc_capnp::ddc_channel::SetSourceParams,
        mut response: gen3rpc_capnp::ddc_channel::SetSourceResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let source_bin = pry!(params.get()).get_source_bin();
        let mut il = pry!(self.lock_mut());

        response
            .get()
            .set_result(capnp_rpc::new_client(ResultImpl::<_, ChannelConfigError> {
                inner: il.set_source(source_bin),
            }));
        Promise::ok(())
    }

    fn set_ddc_freq(
        &mut self,
        params: gen3rpc_capnp::ddc_channel::SetDdcFreqParams,
        mut response: gen3rpc_capnp::ddc_channel::SetDdcFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let ddc_freq = pry!(params.get()).get_ddc_freq();
        let mut il = pry!(self.lock_mut());

        response
            .get()
            .set_result(capnp_rpc::new_client(ResultImpl::<_, ChannelConfigError> {
                inner: il.set_ddc_freq(ddc_freq),
            }));
        Promise::ok(())
    }

    fn set_rotation(
        &mut self,
        params: gen3rpc_capnp::ddc_channel::SetRotationParams,
        mut response: gen3rpc_capnp::ddc_channel::SetRotationResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let rotation = pry!(params.get()).get_rotation();
        let mut il = pry!(self.lock_mut());

        response
            .get()
            .set_result(capnp_rpc::new_client(ResultImpl::<_, ChannelConfigError> {
                inner: il.set_rotation(rotation),
            }));
        Promise::ok(())
    }

    fn set_center(
        &mut self,
        params: gen3rpc_capnp::ddc_channel::SetCenterParams,
        mut response: gen3rpc_capnp::ddc_channel::SetCenterResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let center = pry!(pry!(params.get()).get_center());
        let mut il = pry!(self.lock_mut());

        response
            .get()
            .set_result(capnp_rpc::new_client(ResultImpl::<_, ChannelConfigError> {
                inner: il.set_center(Complex::new(center.get_real(), center.get_imag())),
            }));
        Promise::ok(())
    }

    fn get_baseband_frequency(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetBasebandFrequencyParams,
        mut response: gen3rpc_capnp::ddc_channel::GetBasebandFrequencyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut f = response.get().init_frequency().init_frequency();
        let il = self.inner.read().unwrap();
        let fr = il.get_baseband_freq();
        f.set_numerator(*fr.numer());
        f.set_denominator(*fr.denom());
        Promise::ok(())
    }

    fn get_dest(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetDestParams,
        mut response: gen3rpc_capnp::ddc_channel::GetDestResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        response
            .get()
            .set_dest_bin(self.inner.read().unwrap().get_dest());
        Promise::ok(())
    }
}

impl gen3rpc_capnp::capture::Server for CaptureImpl {
    fn capture(
        &mut self,
        params: gen3rpc_capnp::capture::CaptureParams,
        mut response: gen3rpc_capnp::capture::CaptureResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let pr = params.get();
        let p = pry!(pr);
        let tap = pry!(p.get_tap());
        let length = p.get_length();

        let _lock = self.inner.lock().unwrap();

        match pry!(tap.which()) {
            RawIq(()) => {
                println!("Capturing Raw IQ");
                let t = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0));
                let _table_offset = (t.subsec_nanos() % (128_000)) * 4;
                let v: Vec<_> = (0..length)
                    .map(|i| Complex::new((i & 0x7FFF) as i16, 0))
                    .collect();
                let mut r = response.get();
                r.set_result(capnp_rpc::new_client(ResultImpl::<
                    gen3_rpc::gen3rpc_capnp::snap::Client,
                    CaptureError,
                > {
                    inner: Ok(capnp_rpc::new_client(DroppableReferenceImpl {
                        state: Arc::new(RwLock::new(DRState::Exclusive)),
                        inner: Arc::new(RwLock::new(gen3_rpc::Snap::Raw(v))),
                        stale: false,
                        phantom: PhantomData,
                    })),
                }));
                Promise::ok(())
            }
            DdcIq(d) => {
                let d = pry!(d);
                let l = d.len();
                let mut ids = vec![];
                for c in d.into_iter() {
                    ids.push(pry!(c).get_dest_request());
                }
                Promise::from_future(
                    try_join_all(ids.into_iter().map(|a| {
                        a.send().promise.map(|resp| {
                            let r = resp?;
                            Ok::<_, capnp::Error>(r.get()?.get_dest_bin())
                        })
                    }))
                    .map_ok(move |ids| {
                        println!("Capturing ddciq from channels {:?}", ids);
                        let mut result = Vec::with_capacity(l as usize);
                        for i in ids.into_iter() {
                            let mut iqv = Vec::with_capacity(length as usize);
                            for j in 0..length {
                                iqv.push(Complex::new((i & 0x7FFF) as i16, (j & 0x7FFF) as i16));
                            }
                            result.push(iqv);
                        }
                        let mut p = response.get();
                        p.set_result(capnp_rpc::new_client(ResultImpl::<
                            gen3_rpc::gen3rpc_capnp::snap::Client,
                            CaptureError,
                        > {
                            inner: Ok(capnp_rpc::new_client(DroppableReferenceImpl {
                                state: Arc::new(RwLock::new(DRState::Exclusive)),
                                inner: Arc::new(RwLock::new(gen3_rpc::Snap::DdcIQ(result))),
                                stale: false,
                                phantom: PhantomData,
                            })),
                        }));
                    }),
                )
            }
            Phase(p) => {
                let p = pry!(p);
                let l = p.len();
                let mut ids = vec![];
                for c in p.into_iter() {
                    ids.push(pry!(c).get_dest_request());
                }
                Promise::from_future(
                    try_join_all(ids.into_iter().map(|a| {
                        a.send().promise.map(|resp| {
                            let r = resp?;
                            Ok::<_, capnp::Error>(r.get()?.get_dest_bin())
                        })
                    }))
                    .map_ok(move |ids| {
                        println!("Capturing phase from channels {:?}", ids);
                        let mut result = Vec::with_capacity(l as usize);
                        for i in ids.into_iter() {
                            let mut iqp = Vec::with_capacity(length as usize);
                            for j in 0..length {
                                iqp.push((i & 0x7FFF) as i16 + (j & 0x7FFF) as i16);
                            }
                            result.push(iqp);
                        }
                        let mut p = response.get();
                        p.set_result(capnp_rpc::new_client(ResultImpl::<
                            gen3_rpc::gen3rpc_capnp::snap::Client,
                            CaptureError,
                        > {
                            inner: Ok(capnp_rpc::new_client(DroppableReferenceImpl {
                                state: Arc::new(RwLock::new(DRState::Exclusive)),
                                inner: Arc::new(RwLock::new(gen3_rpc::Snap::Phase(result))),
                                stale: false,
                                phantom: PhantomData,
                            })),
                        }));
                    }),
                )
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Scale16 {
    scale: u16,
}

impl capnp::traits::SetterInput<gen3rpc_capnp::dsp_scale::scale16::Owned> for Scale16 {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::dsp_scale::scale16::Builder::init_pointer(builder, 1);
        builder.set_scale(input.scale);
        Ok(())
    }
}

impl<T: DSPScale + Send + Sync> gen3rpc_capnp::dsp_scale::Server
    for DroppableReferenceImpl<T, gen3rpc_capnp::dsp_scale::Client>
{
    fn get_fft_scale(
        &mut self,
        _: gen3rpc_capnp::dsp_scale::GetFftScaleParams,
        mut response: gen3rpc_capnp::dsp_scale::GetFftScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut s16 = response.get().init_scale();
        s16.set_scale(self.inner.read().unwrap().get());
        Promise::ok(())
    }

    fn set_fft_scale(
        &mut self,
        params: gen3rpc_capnp::dsp_scale::SetFftScaleParams,
        mut response: gen3rpc_capnp::dsp_scale::SetFftScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let s16 = pry!(pry!(params.get()).get_scale()).get_scale();
        let resimp = ResultImpl {
            inner: self
                .inner
                .write()
                .unwrap()
                .set(s16)
                .map(|scale| Scale16 { scale })
                .map_err(|scale| Scale16 { scale }),
        };
        let client = capnp_rpc::new_client(resimp);
        response.get().set_scale(client);
        Promise::ok(())
    }
}

impl<T: IFBoard + Send + Sync> gen3rpc_capnp::if_board::Server
    for DroppableReferenceImpl<T, gen3rpc_capnp::if_board::Client>
{
    fn get_freq(
        &mut self,
        _: gen3rpc_capnp::if_board::GetFreqParams,
        mut response: gen3rpc_capnp::if_board::GetFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut f = response.get().init_freq().init_frequency();
        let l = self.inner.read().unwrap();
        f.set_numerator(*l.get_lo().numer());
        f.set_denominator(*l.get_lo().denom());
        Promise::ok(())
    }

    fn set_freq(
        &mut self,
        params: gen3rpc_capnp::if_board::SetFreqParams,
        mut response: gen3rpc_capnp::if_board::SetFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let pf = pry!(pry!(pry!(params.get()).get_freq()).get_frequency());
        let mut l = self.inner.write().unwrap();
        response
            .get()
            .set_freq(capnp_rpc::new_client(ResultImpl::<_, FrequencyError> {
                inner: l.set_lo(Hertz::new(pf.get_numerator(), pf.get_denominator())),
            }));
        Promise::ok(())
    }

    fn get_attens(
        &mut self,
        _: gen3rpc_capnp::if_board::GetAttensParams,
        mut response: gen3rpc_capnp::if_board::GetAttensResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut a = response.get().init_attens();
        let l = self.inner.read().unwrap();
        let attens = l.get_attens();
        a.set_input(attens.input);
        a.set_output(attens.output);
        Promise::ok(())
    }

    fn set_attens(
        &mut self,
        params: gen3rpc_capnp::if_board::SetAttensParams,
        mut response: gen3rpc_capnp::if_board::SetAttensResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let pa = pry!(pry!(params.get()).get_attens());
        let attens = Attens {
            input: pa.get_input(),
            output: pa.get_output(),
        };
        let mut l = self.inner.write().unwrap();
        response
            .get()
            .set_attens(capnp_rpc::new_client(ResultImpl::<_, AttenError> {
                inner: l.set_attens(attens),
            }));

        Promise::ok(())
    }
}

impl<T: DACTable + Send + Sync> gen3rpc_capnp::dac_table::Server
    for DroppableReferenceImpl<T, gen3rpc_capnp::dac_table::Client>
{
    fn get(
        &mut self,
        _: gen3rpc_capnp::dac_table::GetParams,
        mut response: gen3rpc_capnp::dac_table::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let inner = self.inner.read().unwrap();
        let values = inner.get();
        response.get().init_data(values.len() as u32);
        for (i, val) in values.iter().enumerate() {
            let mut v = pry!(response.get().get_data()).get(i as u32);
            v.set_real(val.re);
            v.set_imag(val.im);
        }
        Promise::ok(())
    }

    fn set(
        &mut self,
        params: gen3rpc_capnp::dac_table::SetParams,
        _: gen3rpc_capnp::dac_table::SetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let replace = pry!(pry!(pry!(params.get()).get_replace()).get_data());
        println!("Setting DacTable");

        let mut values = Box::new([Complex::new(0, 0); 524288]);
        assert_eq!(values.len(), replace.len() as usize);

        for k in 0..replace.len() {
            values[k as usize] = Complex::new(replace.get(k).get_real(), replace.get(k).get_imag())
        }
        let mut i = self.inner.write().unwrap();
        i.set(values);

        Promise::ok(())
    }
}

impl gen3rpc_capnp::gen3_board::Server for Gen3BoardImpl {
    fn get_ddc(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetDdcParams,
        mut results: gen3rpc_capnp::gen3_board::GetDdcResults,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .set_ddc(capnp_rpc::new_client(self.ddc.clone()));
        Promise::ok(())
    }
    fn get_dac_table(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetDacTableParams,
        mut results: gen3rpc_capnp::gen3_board::GetDacTableResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_dac_table(capnp_rpc::new_client(self.dac_table.try_clone().unwrap()));
        Promise::ok(())
    }
    fn get_capture(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetCaptureParams,
        mut results: gen3rpc_capnp::gen3_board::GetCaptureResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_capture(capnp_rpc::new_client(self.capture.clone()));
        Promise::ok(())
    }
    fn get_dsp_scale(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetDspScaleParams,
        mut results: gen3rpc_capnp::gen3_board::GetDspScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_dsp_scale(capnp_rpc::new_client(self.dsp_scale.try_clone().unwrap()));
        Promise::ok(())
    }
    fn get_if_board(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetIfBoardParams,
        mut results: gen3rpc_capnp::gen3_board::GetIfBoardResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_if_board(capnp_rpc::new_client(self.if_board.try_clone().unwrap()));
        Promise::ok(())
    }
}

impl<T: Snap + Send + Sync> gen3rpc_capnp::snap::Server
    for DroppableReferenceImpl<T, gen3rpc_capnp::snap::Client>
{
    fn get(
        &mut self,
        _: gen3rpc_capnp::snap::GetParams,
        mut results: gen3rpc_capnp::snap::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let b = self.inner.read().unwrap().get();
        match b {
            gen3_rpc::Snap::Raw(v) => {
                let mut bv = results.get().init_raw_iq(v.len() as u32);
                for (i, c) in v.iter().enumerate() {
                    let mut bc = bv.reborrow().get(i as u32);
                    bc.set_real(c.re);
                    bc.set_imag(c.im);
                }
            }
            gen3_rpc::Snap::DdcIQ(v) => {
                let mut bv = results.get().init_ddc_iq(v.len() as u32);
                for (i, cv) in v.iter().enumerate() {
                    let mut bcv = bv.reborrow().init(i as u32, cv.len() as u32);
                    for (j, c) in cv.iter().enumerate() {
                        let mut bc = bcv.reborrow().get(j as u32);
                        bc.set_real(c.re);
                        bc.set_imag(c.im);
                    }
                }
            }
            gen3_rpc::Snap::Phase(v) => {
                let mut bv = results.get().init_phase(v.len() as u32);
                for (i, pv) in v.iter().enumerate() {
                    let mut bpv = bv.reborrow().init(i as u32, pv.len() as u32);
                    for (j, p) in pv.iter().enumerate() {
                        bpv.set(j as u32, *p);
                    }
                }
            }
        }
        Promise::ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                54321,
            ))
            .await?;
            let client: gen3rpc_capnp::gen3_board::Client = capnp_rpc::new_client(Gen3BoardImpl {
                ddc: DDCImpl {
                    inner: Arc::new(Mutex::new(HashMap::new())),
                },
                dac_table: DACTableImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(DACTableMock {
                        values: Box::new([Complex::i(); 524288]),
                    })),
                    stale: false,
                    phantom: PhantomData,
                },
                capture: CaptureImpl {
                    inner: Arc::new(Mutex::new(())),
                },
                dsp_scale: DSPScaleImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(DSPScaleMock { fft: 0 })),
                    stale: false,
                    phantom: PhantomData,
                },
                if_board: IFBoardImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(IFBoardMock {
                        lo: Hertz::new(6_000_000_000, 1),
                        attens: Attens {
                            input: 10.,
                            output: 20.,
                        },
                    })),
                    stale: false,
                    phantom: PhantomData,
                },
            });

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) =
                    tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network = twoparty::VatNetwork::new(
                    futures::io::BufReader::new(reader),
                    futures::io::BufWriter::new(writer),
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );
                let rpc_system = RpcSystem::new(Box::new(network), Some(client.clone().client));

                tokio::task::spawn_local(rpc_system);
            }
        })
        .await
}
