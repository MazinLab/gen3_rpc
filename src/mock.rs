use gen3_rpc::{
    gen3rpc_capnp::{
        self,
        capture::capture_tap::Which::{DdcIq, Phase, RawIq},
    },
    CaptureError, ChannelAllocationError, DDCChannelConfig, Snap,
};
use num::Complex;

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    ops::Deref,
    sync::{Arc, Mutex, RwLock, RwLockWriteGuard, TryLockError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use capnp::{capability::Promise, traits::FromPointerBuilder};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{future::try_join_all, AsyncReadExt, FutureExt, TryFutureExt};

struct DSPScale {
    fft: u16,
}
struct IFBoard;
struct DACTable {
    values: Box<[Complex<i16>; 524288]>,
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

enum DRState {
    Unshared,
    Exclusive,
    Shared(usize),
}

struct DroppableReferenceImpl<T: 'static> {
    state: Arc<RwLock<DRState>>,
    inner: Arc<RwLock<T>>,
    stale: bool,
}

impl<T> Drop for DroppableReferenceImpl<T> {
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

impl<T> DroppableReferenceImpl<T> {
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
                })
            }
            DRState::Shared(u) => {
                *i = DRState::Shared(u + 1);
                Ok(Self {
                    inner: self.inner.clone(),
                    state: self.state.clone(),
                    stale: false,
                })
            }
        }
    }
}

#[derive(Clone)]
struct DDCImpl {
    inner: Arc<Mutex<HashMap<u16, DDCChannelConfig>>>,
}

#[derive(Clone)]
struct CaptureImpl {
    inner: Arc<Mutex<()>>,
}

type DSPScaleImpl = DroppableReferenceImpl<DSPScale>;
type IFBoardImpl = DroppableReferenceImpl<IFBoard>;
type DACTableImpl = DroppableReferenceImpl<DACTable>;
type SnapImpl = DroppableReferenceImpl<Snap>;
type DDCChannelImpl = DroppableReferenceImpl<DDCChannelConfig>;

impl<T> gen3rpc_capnp::droppable_reference::Server for DroppableReferenceImpl<T> {
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
        response
            .get()
            .set_nonmut(capnp_rpc::new_client(DroppableReferenceImpl {
                state: self.state.clone(),
                inner: Arc::clone(&self.inner),
                stale: false,
            }));
        Promise::ok(())
    }

    fn try_into_mut(
        &mut self,
        _: gen3rpc_capnp::droppable_reference::TryIntoMutParams,
        mut response: gen3rpc_capnp::droppable_reference::TryIntoMutResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut i = self.state.write().unwrap();
        match *i {
            DRState::Unshared => unreachable!(),
            DRState::Shared(1) | DRState::Exclusive => {
                self.stale = true;
                *i = DRState::Exclusive;

                let resimp: ResultImpl<
                    gen3rpc_capnp::droppable_reference::Client,
                    gen3rpc_capnp::droppable_reference::Client,
                > = ResultImpl {
                    inner: Ok(capnp_rpc::new_client(DroppableReferenceImpl {
                        state: self.state.clone(),
                        inner: Arc::clone(&self.inner),
                        stale: false,
                    })),
                };
                response.get().set_maybe_mut(capnp_rpc::new_client(resimp));
            }
            DRState::Shared(_) => {
                self.stale = true;
                *i = DRState::Exclusive;

                let resimp: ResultImpl<
                    gen3rpc_capnp::droppable_reference::Client,
                    gen3rpc_capnp::droppable_reference::Client,
                > = ResultImpl {
                    inner: Err(capnp_rpc::new_client(DroppableReferenceImpl {
                        state: self.state.clone(),
                        inner: Arc::clone(&self.inner),
                        stale: false,
                    })),
                };
                response.get().set_maybe_mut(capnp_rpc::new_client(resimp));
            }
        }
        Promise::ok(())
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
        let db = pry!(ccfg.get_destination_bin().which());
        let c = pry!(ccfg.get_center());
        let mut ccfg = DDCChannelConfig {
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
        if let Some(d) = ccfg.dest_bin {
            if let std::collections::hash_map::Entry::Vacant(e) = hm.entry(d as u16) {
                println!("Allocated with dest {}", d);
                e.insert(ccfg.clone());
                let res: ResultImpl<gen3rpc_capnp::ddc_channel::Client, ChannelAllocationError> =
                    ResultImpl {
                        inner: Ok(capnp_rpc::new_client(DDCChannelImpl {
                            inner: Arc::new(RwLock::new(ccfg)),
                            stale: false,
                            state: Arc::new(RwLock::new(DRState::Exclusive)),
                        })),
                    };
                response.get().set_result(capnp_rpc::new_client(res));
            } else {
                println!("Allocated failed");
                let res: ResultImpl<gen3rpc_capnp::ddc_channel::Client, ChannelAllocationError> =
                    ResultImpl {
                        inner: Err(ChannelAllocationError::DestinationInUse),
                    };
                response.get().set_result(capnp_rpc::new_client(res));
            }
        } else {
            for i in 0..2048 {
                if !hm.contains_key(&i) {
                    ccfg.dest_bin = Some(i as u32);
                    break;
                }
            }
            match ccfg.dest_bin {
                Some(d) => {
                    println!("Allocated with dest {}", d);
                    hm.insert(d as u16, ccfg.clone());
                    let res: ResultImpl<
                        gen3rpc_capnp::ddc_channel::Client,
                        ChannelAllocationError,
                    > = ResultImpl {
                        inner: Ok(capnp_rpc::new_client(DDCChannelImpl {
                            inner: Arc::new(RwLock::new(ccfg)),
                            stale: false,
                            state: Arc::new(RwLock::new(DRState::Exclusive)),
                        })),
                    };
                    response.get().set_result(capnp_rpc::new_client(res));
                }
                None => {
                    println!("Allocated failed");
                    let res: ResultImpl<
                        gen3rpc_capnp::ddc_channel::Client,
                        ChannelAllocationError,
                    > = ResultImpl {
                        inner: Err(ChannelAllocationError::OutOfChannels),
                    };
                    response.get().set_result(capnp_rpc::new_client(res));
                }
            }
        }
        Promise::ok(())
    }

    fn allocate_channel_mut(
        &mut self,
        _: gen3rpc_capnp::ddc::AllocateChannelMutParams,
        _: gen3rpc_capnp::ddc::AllocateChannelMutResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }

    fn retrieve_channel(
        &mut self,
        _: gen3rpc_capnp::ddc::RetrieveChannelParams,
        _: gen3rpc_capnp::ddc::RetrieveChannelResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
}

impl gen3rpc_capnp::ddc_channel::Server for DDCChannelImpl {
    fn get(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetParams,
        mut response: gen3rpc_capnp::ddc_channel::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut r = response.get();
        let l = self.inner.read().unwrap();
        r.set_source_bin(l.source_bin);
        r.set_ddc_freq(l.ddc_freq);
        r.set_rotation(l.rotation);
        r.reborrow()
            .init_destination_bin()
            .set_some(l.dest_bin.unwrap());
        let mut c = r.init_center();
        c.set_real(l.center.re);
        c.set_imag(l.center.im);

        Promise::ok(())
    }
    fn set(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::SetParams,
        _: gen3rpc_capnp::ddc_channel::SetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn set_source(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::SetSourceParams,
        _: gen3rpc_capnp::ddc_channel::SetSourceResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn set_ddc_freq(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::SetDdcFreqParams,
        _: gen3rpc_capnp::ddc_channel::SetDdcFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn set_rotation(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::SetRotationParams,
        _: gen3rpc_capnp::ddc_channel::SetRotationResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn set_center(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::SetCenterParams,
        _: gen3rpc_capnp::ddc_channel::SetCenterResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn get_baseband_frequency(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetBasebandFrequencyParams,
        _: gen3rpc_capnp::ddc_channel::GetBasebandFrequencyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
    fn get_dest(
        &mut self,
        _: gen3rpc_capnp::ddc_channel::GetDestParams,
        mut response: gen3rpc_capnp::ddc_channel::GetDestResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response
            .get()
            .set_dest_bin(self.inner.read().unwrap().dest_bin.unwrap());
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
                        inner: Arc::new(RwLock::new(Snap::Raw(v))),
                        stale: false,
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
                                inner: Arc::new(RwLock::new(Snap::DdcIQ(result))),
                                stale: false,
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
                                inner: Arc::new(RwLock::new(Snap::Phase(result))),
                                stale: false,
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
impl gen3rpc_capnp::dsp_scale::Server for DSPScaleImpl {
    fn get_fft_scale(
        &mut self,
        _: gen3rpc_capnp::dsp_scale::GetFftScaleParams,
        mut response: gen3rpc_capnp::dsp_scale::GetFftScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut s16 = response.get().init_scale();
        s16.set_scale(self.inner.read().unwrap().fft);
        Promise::ok(())
    }

    fn set_fft_scale(
        &mut self,
        params: gen3rpc_capnp::dsp_scale::SetFftScaleParams,
        mut response: gen3rpc_capnp::dsp_scale::SetFftScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let s16 = pry!(pry!(params.get()).get_scale()).get_scale();
        {
            let mut i = self.inner.write().unwrap();
            i.fft = s16 & 0xfff;
            println!("Set FFT Scale to {:03x}", i.fft);
        }
        let resimp = ResultImpl {
            inner: if s16 == s16 & 0xfff {
                Ok(Scale16 { scale: s16 & 0xfff })
            } else {
                Err(Scale16 { scale: s16 & 0xfff })
            },
        };
        let client = capnp_rpc::new_client(resimp);
        response.get().set_scale(client);
        Promise::ok(())
    }
}

impl gen3rpc_capnp::if_board::Server for IFBoardImpl {
    fn get_freq(
        &mut self,
        _: gen3rpc_capnp::if_board::GetFreqParams,
        _: gen3rpc_capnp::if_board::GetFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }

    fn set_freq(
        &mut self,
        _: gen3rpc_capnp::if_board::SetFreqParams,
        _: gen3rpc_capnp::if_board::SetFreqResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }

    fn get_attens(
        &mut self,
        _: gen3rpc_capnp::if_board::GetAttensParams,
        _: gen3rpc_capnp::if_board::GetAttensResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }

    fn set_attens(
        &mut self,
        _: gen3rpc_capnp::if_board::SetAttensParams,
        _: gen3rpc_capnp::if_board::SetAttensResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        todo!()
    }
}

impl gen3rpc_capnp::dac_table::Server for DACTableImpl {
    fn get(
        &mut self,
        _: gen3rpc_capnp::dac_table::GetParams,
        mut response: gen3rpc_capnp::dac_table::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let inner = self.inner.read().unwrap();
        response.get().init_data(inner.values.len() as u32);
        for (i, val) in inner.values.iter().enumerate() {
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
        let replace = pry!(pry!(pry!(params.get()).get_replace()).get_data());

        let mut i = self.inner.write().unwrap();
        for k in 0..replace.len() {
            i.values[k as usize] =
                Complex::new(replace.get(k).get_real(), replace.get(k).get_imag())
        }

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

impl gen3rpc_capnp::snap::Server for SnapImpl {
    fn get(
        &mut self,
        _: gen3rpc_capnp::snap::GetParams,
        mut results: gen3rpc_capnp::snap::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let b = self.inner.read().unwrap();
        match b.deref() {
            Snap::Raw(v) => {
                let mut bv = results.get().init_raw_iq(v.len() as u32);
                for (i, c) in v.iter().enumerate() {
                    let mut bc = bv.reborrow().get(i as u32);
                    bc.set_real(c.re);
                    bc.set_imag(c.im);
                }
            }
            Snap::DdcIQ(v) => {
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
            Snap::Phase(v) => {
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
                    inner: Arc::new(RwLock::new(DACTable {
                        values: Box::new([Complex::i(); 524288]),
                    })),
                    stale: false,
                },
                capture: CaptureImpl {
                    inner: Arc::new(Mutex::new(())),
                },
                dsp_scale: DSPScaleImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(DSPScale { fft: 0 })),
                    stale: false,
                },
                if_board: IFBoardImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(IFBoard)),
                    stale: false,
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
