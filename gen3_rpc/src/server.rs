use std::{
    marker::PhantomData,
    sync::{Arc, RwLock, RwLockWriteGuard, TryLockError},
};

use capnp::capability::{FromClientHook, Promise};

use capnp_rpc::pry;

use futures::{future::try_join_all, FutureExt, TryFutureExt};
use num_complex::{Complex, Complex64};

use crate::{
    utils::{client::DACCapabilities, little_fixed::LittleFixedDynI32},
    ActualizedDDCChannelConfig, AttenError, Attens, CaptureError, ChannelAllocationError,
    ChannelConfigError, DDCCapabilities, DDCChannelConfig, ErasedDDCChannelConfig, FrequencyError,
    Hertz, Scale16, SnapAvg,
};

pub struct Gen3Board<
    TDAC: DACTable,
    TIFB: IFBoard,
    TDSP: DSPScale,
    TDDC: DDC<TCHAN>,
    TCHAN: DDCChannel,
    TCAP: Capture<TSNAP>,
    TSNAP: Snap,
> {
    dac_table: DroppableReferenceImpl<TDAC, crate::gen3rpc_capnp::dac_table::Client>,
    if_board: DroppableReferenceImpl<TIFB, crate::gen3rpc_capnp::if_board::Client>,
    dsp_scale: DroppableReferenceImpl<TDSP, crate::gen3rpc_capnp::dsp_scale::Client>,
    ddc: TDDC,
    capture: TCAP,
    phantom: PhantomData<(TCHAN, TSNAP)>,
}

impl<
        TDAC: DACTable,
        TIFB: IFBoard,
        TDSP: DSPScale,
        TDDC: DDC<TCHAN>,
        TCHAN: DDCChannel,
        TCAP: Capture<TSNAP>,
        TSNAP: Snap,
    > Gen3Board<TDAC, TIFB, TDSP, TDDC, TCHAN, TCAP, TSNAP>
{
    pub fn new(dac_table: TDAC, if_board: TIFB, dsp_scale: TDSP, ddc: TDDC, capture: TCAP) -> Self {
        Gen3Board {
            dac_table: DroppableReferenceImpl::new(dac_table),
            if_board: DroppableReferenceImpl::new(if_board),
            dsp_scale: DroppableReferenceImpl::new(dsp_scale),
            ddc,
            capture,
            phantom: PhantomData,
        }
    }
}
impl<
        TDAC: DACTable,
        TIFB: IFBoard,
        TDSP: DSPScale,
        TDDC: DDC<TCHAN>,
        TCHAN: DDCChannel,
        TCAP: Capture<TSNAP>,
        TSNAP: Snap,
    > crate::gen3rpc_capnp::gen3_board::Server
    for Gen3Board<TDAC, TIFB, TDSP, TDDC, TCHAN, TCAP, TSNAP>
{
    fn get_ddc(
        &mut self,
        _: crate::gen3rpc_capnp::gen3_board::GetDdcParams,
        mut results: crate::gen3rpc_capnp::gen3_board::GetDdcResults,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .set_ddc(capnp_rpc::new_client(PhantomServer::new(self.ddc.clone())));
        Promise::ok(())
    }
    fn get_dac_table(
        &mut self,
        _: crate::gen3rpc_capnp::gen3_board::GetDacTableParams,
        mut results: crate::gen3rpc_capnp::gen3_board::GetDacTableResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_dac_table(capnp_rpc::new_client(self.dac_table.try_clone().unwrap()));
        Promise::ok(())
    }
    fn get_capture(
        &mut self,
        _: crate::gen3rpc_capnp::gen3_board::GetCaptureParams,
        mut results: crate::gen3rpc_capnp::gen3_board::GetCaptureResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_capture(capnp_rpc::new_client(PhantomServer::new(
                self.capture.clone(),
            )));
        Promise::ok(())
    }
    fn get_dsp_scale(
        &mut self,
        _: crate::gen3rpc_capnp::gen3_board::GetDspScaleParams,
        mut results: crate::gen3rpc_capnp::gen3_board::GetDspScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_dsp_scale(capnp_rpc::new_client(self.dsp_scale.try_clone().unwrap()));
        Promise::ok(())
    }
    fn get_if_board(
        &mut self,
        _: crate::gen3rpc_capnp::gen3_board::GetIfBoardParams,
        mut results: crate::gen3rpc_capnp::gen3_board::GetIfBoardResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        results
            .get()
            .set_if_board(capnp_rpc::new_client(self.if_board.try_clone().unwrap()));
        Promise::ok(())
    }
}

/// Phantom server is used to implement the DDC Server and the Capture Server
pub struct PhantomServer<T, D> {
    inner: T,
    phantom: PhantomData<fn() -> D>,
}

impl<T, D> PhantomServer<T, D> {
    pub fn new(inner: T) -> Self {
        PhantomServer {
            inner,
            phantom: PhantomData,
        }
    }
}

pub enum CaptureTap {
    RawIQ,
    DdcIQ(Vec<crate::gen3rpc_capnp::ddc_channel::Client>),
    Phase(Vec<crate::gen3rpc_capnp::ddc_channel::Client>),
}

impl TryFrom<crate::gen3rpc_capnp::capture::capture_tap::WhichReader<'_>> for CaptureTap {
    type Error = capnp::Error;
    fn try_from(
        value: crate::gen3rpc_capnp::capture::capture_tap::WhichReader,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            crate::gen3rpc_capnp::capture::capture_tap::WhichReader::RawIq(()) => Self::RawIQ,
            crate::gen3rpc_capnp::capture::capture_tap::WhichReader::DdcIq(cs) => {
                let cs = cs?;
                let cs: Result<Vec<_>, capnp::Error> = cs.iter().collect();
                Self::DdcIQ(cs?)
            }
            crate::gen3rpc_capnp::capture::capture_tap::WhichReader::Phase(cs) => {
                let cs = cs?;
                let cs: Result<Vec<_>, capnp::Error> = cs.iter().collect();
                Self::Phase(cs?)
            }
        })
    }
}

impl CaptureTap {
    pub fn dest_bins(self) -> Promise<CaptureTapDestBins, capnp::Error> {
        match self {
            Self::RawIQ => Promise::ok(CaptureTapDestBins::RawIQ),
            Self::DdcIQ(c) => {
                let reqs = c.into_iter().map(|c| {
                    c.get_dest_request().send().promise.map(|resp| {
                        let r = resp?;
                        Ok::<_, capnp::Error>(r.get()?.get_dest_bin())
                    })
                });
                Promise::from_future(try_join_all(reqs).map_ok(CaptureTapDestBins::DdcIQ))
            }
            Self::Phase(c) => {
                let reqs = c.into_iter().map(|c| {
                    c.get_dest_request().send().promise.map(|resp| {
                        let r = resp?;
                        Ok::<_, capnp::Error>(r.get()?.get_dest_bin())
                    })
                });
                Promise::from_future(try_join_all(reqs).map_ok(CaptureTapDestBins::Phase))
            }
        }
    }
}

pub enum CaptureTapDestBins {
    RawIQ,
    DdcIQ(Vec<u32>),
    Phase(Vec<u32>),
}

pub trait Capture<T: 'static + Send + Snap + Sync>: Send + Sync + Clone + 'static {
    fn capture(
        &self,
        tap: CaptureTap,
        length: u64,
    ) -> Promise<
        Result<DroppableReferenceImpl<T, crate::gen3rpc_capnp::snap::Client>, CaptureError>,
        capnp::Error,
    >;
    fn average(
        &self,
        tap: CaptureTap,
        length: u64,
    ) -> Promise<Result<SnapAvg, CaptureError>, capnp::Error>;
}

impl<T, D> crate::gen3rpc_capnp::capture::Server for PhantomServer<T, D>
where
    T: Capture<D>,
    D: 'static + Send + Snap + Sync,
{
    fn capture(
        &mut self,
        params: crate::gen3rpc_capnp::capture::CaptureParams,
        mut response: crate::gen3rpc_capnp::capture::CaptureResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let taps: CaptureTap = pry!(pry!(pry!(pry!(params.get()).get_tap()).which()).try_into());
        let length = pry!(params.get()).get_length();
        Promise::from_future(self.inner.capture(taps, length).map_ok(move |inner| {
            response.get().set_result(capnp_rpc::new_client(ResultImpl {
                inner: inner.map(|dr| {
                    capnp_rpc::new_client::<
                        crate::gen3rpc_capnp::snap::Client,
                        DroppableReferenceImpl<D, crate::gen3rpc_capnp::snap::Client>,
                    >(dr)
                }),
            }));
        }))
    }
    fn average(
        &mut self,
        params: crate::gen3rpc_capnp::capture::AverageParams,
        mut response: crate::gen3rpc_capnp::capture::AverageResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let taps: CaptureTap = pry!(pry!(pry!(pry!(params.get()).get_tap()).which()).try_into());
        let length = pry!(params.get()).get_length();
        Promise::from_future(self.inner.average(taps, length).map_ok(move |inner| {
            response
                .get()
                .set_result(capnp_rpc::new_client(ResultImpl { inner }));
        }))
    }
}

pub trait DDC<T: 'static + Send + DDCChannel + Sync>: Send + Sync + Clone + 'static {
    fn capabilities(&self) -> DDCCapabilities;

    fn allocate_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>,
        ChannelAllocationError,
    >;

    fn allocate_channel_mut(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>,
        ChannelAllocationError,
    >;

    fn retrieve_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Option<DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>>;
}

impl<T, D> crate::gen3rpc_capnp::ddc::Server for PhantomServer<T, D>
where
    T: DDC<D>,
    D: 'static + Send + DDCChannel + Sync,
{
    fn capabilities(
        &mut self,
        _: crate::gen3rpc_capnp::ddc::CapabilitiesParams,
        mut results: crate::gen3rpc_capnp::ddc::CapabilitiesResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let caps = self.inner.capabilities();

        let mut sr = results.get().init_opfb_sample_rate();
        sr.set_numerator(*caps.opfb_samplerate.numer());
        sr.set_denominator(*caps.opfb_samplerate.denom());

        results.get().set_opfb_channels(caps.opfb_channels);

        let mut fr = results.get().init_freq_resolution();
        fr.set_numerator(*caps.freq_resolution.numer());
        fr.set_denominator(*caps.freq_resolution.denom());
        match caps.bin_control {
            crate::BinControl::FullSwizzle => results.get().init_bin_control().set_full_swizzle(()),
            crate::BinControl::None => results.get().init_bin_control().set_none(()),
        }

        results.get().set_freq_bits(caps.freq_bits);
        results.get().set_rotation_bits(caps.rotation_bits);
        results.get().set_center_bits(caps.center_bits);

        Promise::ok(())
    }

    fn allocate_channel(
        &mut self,
        params: crate::gen3rpc_capnp::ddc::AllocateChannelParams,
        mut response: crate::gen3rpc_capnp::ddc::AllocateChannelResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let ccfg = pry!(pry!(params.get()).get_config());
        let ccfg = pry!(ccfg.try_into());
        let res: ResultImpl<_, _> = self
            .inner
            .allocate_channel(ccfg)
            .map(capnp_rpc::new_client::<crate::gen3rpc_capnp::ddc_channel::Client, _>)
            .into();
        response.get().set_result(capnp_rpc::new_client(res));
        Promise::ok(())
    }

    fn allocate_channel_mut(
        &mut self,
        params: crate::gen3rpc_capnp::ddc::AllocateChannelMutParams,
        mut response: crate::gen3rpc_capnp::ddc::AllocateChannelMutResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let ccfg = pry!(pry!(params.get()).get_config());
        let ccfg = pry!(ccfg.try_into());
        let res: ResultImpl<_, _> = self
            .inner
            .allocate_channel_mut(ccfg)
            .map(capnp_rpc::new_client::<crate::gen3rpc_capnp::ddc_channel::Client, _>)
            .into();
        response.get().set_result(capnp_rpc::new_client(res));
        Promise::ok(())
    }

    fn retrieve_channel(
        &mut self,
        params: crate::gen3rpc_capnp::ddc::RetrieveChannelParams,
        mut response: crate::gen3rpc_capnp::ddc::RetrieveChannelResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let ccfg = pry!(pry!(params.get()).get_config());
        let ccfg = pry!(ccfg.try_into());
        let res: OptionImpl<_> = self
            .inner
            .retrieve_channel(ccfg)
            .map(capnp_rpc::new_client::<crate::gen3rpc_capnp::ddc_channel::Client, _>)
            .into();
        response.get().set_channel(capnp_rpc::new_client(res));
        todo!()
    }
}

pub trait DSPScale: Send + Sync + 'static {
    fn set(&mut self, v: u16) -> Result<u16, u16>;
    fn get(&self) -> u16;
}

pub trait IFBoard: Send + Sync + 'static {
    fn set_lo(&mut self, v: Hertz) -> Result<Hertz, FrequencyError>;
    fn get_lo(&self) -> Hertz;
    fn set_attens(&mut self, a: Attens) -> Result<Attens, AttenError>;
    fn get_attens(&self) -> Attens;
}

pub trait Snap: Send + Sync + 'static {
    fn get(&self) -> crate::Snap;
    fn average(&self) -> crate::SnapAvg {
        let b = self.get();
        match b {
            crate::Snap::Raw(v) => SnapAvg::Raw(
                Complex64::new(
                    v.iter().map(|p| p.re as f64).sum(),
                    v.iter().map(|p| p.im as f64).sum(),
                ) / v.len() as f64,
            ),
            crate::Snap::DdcIQ(vs) => SnapAvg::DdcIQ(
                vs.into_iter()
                    .map(|v| {
                        Complex64::new(
                            v.iter().map(|p| p.re as f64).sum(),
                            v.iter().map(|p| p.im as f64).sum(),
                        ) / v.len() as f64
                    })
                    .collect(),
            ),
            crate::Snap::Phase(vs) => SnapAvg::Phase(
                vs.into_iter()
                    .map(|v| v.iter().map(|p| *p as f64).sum::<f64>() / v.len() as f64)
                    .collect(),
            ),
        }
    }
    fn rms(&self) -> crate::SnapAvg {
        let avgs = self.average();
        let b = self.get();
        match (avgs, b) {
            (crate::SnapAvg::Raw(a), crate::Snap::Raw(v)) => SnapAvg::Raw(Complex64::new(
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
            (crate::SnapAvg::DdcIQ(avgs), crate::Snap::DdcIQ(vs)) => SnapAvg::DdcIQ(
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
            (crate::SnapAvg::Phase(avgs), crate::Snap::Phase(vs)) => SnapAvg::Phase(
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

impl Snap for crate::Snap {
    fn get(&self) -> crate::Snap {
        self.clone()
    }
}

pub trait DACTable: Send + Sync + 'static {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>);
    fn get(&self) -> Box<[Complex<i16>; 524288]>;
    fn get_capabilities(&self) -> DACCapabilities;
}

pub trait DDCChannel: Sized + Send + Sync + 'static {
    type Shared: Clone + Send + Sync + Sized + 'static;

    fn from_actualized(
        setup: ActualizedDDCChannelConfig,
        caps: DDCCapabilities,
        shared: Self::Shared,
    ) -> Result<Self, ChannelConfigError>;
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

impl DDCChannel for (ActualizedDDCChannelConfig, DDCCapabilities) {
    type Shared = ();

    fn from_actualized(
        setup: ActualizedDDCChannelConfig,
        caps: DDCCapabilities,
        _shared: Self::Shared,
    ) -> Result<Self, ChannelConfigError> {
        Ok((setup, caps))
    }
    fn get(&self) -> ActualizedDDCChannelConfig {
        self.0.clone()
    }
    fn set(&mut self, replacement: ErasedDDCChannelConfig) -> Result<(), ChannelConfigError> {
        self.0.source_bin = replacement.source_bin;
        let ddc_freq = LittleFixedDynI32::try_new(replacement.ddc_freq, self.1.freq_bits as usize)
            .ok_or(ChannelConfigError::UsedTooManyBits)?;
        let rotation =
            LittleFixedDynI32::try_new(replacement.rotation, self.1.rotation_bits as usize)
                .ok_or(ChannelConfigError::UsedTooManyBits)?;
        let center_re =
            LittleFixedDynI32::try_new(replacement.center.re, self.1.center_bits as usize)
                .ok_or(ChannelConfigError::UsedTooManyBits)?;
        let center_im =
            LittleFixedDynI32::try_new(replacement.center.im, self.1.center_bits as usize)
                .ok_or(ChannelConfigError::UsedTooManyBits)?;
        self.0.source_bin = replacement.source_bin;
        self.0.ddc_freq = *ddc_freq;
        self.0.rotation = *rotation;
        self.0.center = Complex::new(*center_re, *center_im);
        Ok(())
    }
    fn set_source(&mut self, source_bin: u32) -> Result<(), ChannelConfigError> {
        self.0.source_bin = source_bin;
        Ok(())
    }
    fn set_ddc_freq(&mut self, ddc_freq: i32) -> Result<(), ChannelConfigError> {
        let ddc_freq = LittleFixedDynI32::try_new(ddc_freq, self.1.freq_bits as usize)
            .ok_or(ChannelConfigError::UsedTooManyBits)?;
        self.0.ddc_freq = *ddc_freq;
        Ok(())
    }
    fn set_rotation(&mut self, rotation: i32) -> Result<(), ChannelConfigError> {
        let rotation = LittleFixedDynI32::try_new(rotation, self.1.rotation_bits as usize)
            .ok_or(ChannelConfigError::UsedTooManyBits)?;
        self.0.rotation = *rotation;
        Ok(())
    }
    fn set_center(&mut self, center: Complex<i32>) -> Result<(), ChannelConfigError> {
        let center_re = LittleFixedDynI32::try_new(center.re, self.1.center_bits as usize)
            .ok_or(ChannelConfigError::UsedTooManyBits)?;
        let center_im = LittleFixedDynI32::try_new(center.im, self.1.center_bits as usize)
            .ok_or(ChannelConfigError::UsedTooManyBits)?;
        self.0.center = Complex::new(*center_re, *center_im);
        Ok(())
    }
    fn get_baseband_freq(&self) -> Hertz {
        Hertz::new(self.0.ddc_freq as i64, i32::MAX as i64)
    }
}

pub struct OptionImpl<T> {
    pub inner: Option<T>,
}

impl<T, C> crate::gen3rpc_capnp::option::Server<C> for OptionImpl<T>
where
    T: capnp::traits::SetterInput<C> + Clone,
    C: capnp::traits::Owned,
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::option::GetParams<C>,
        mut response: crate::gen3rpc_capnp::option::GetResults<C>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut r = response.get().init_option();
        match &self.inner {
            Some(t) => pry!(r.set_some(t.clone())),
            None => r.set_none(()),
        }
        Promise::ok(())
    }

    fn is_some(
        &mut self,
        _: crate::gen3rpc_capnp::option::IsSomeParams<C>,
        mut response: crate::gen3rpc_capnp::option::IsSomeResults<C>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response.get().set_some(self.inner.is_some());
        Promise::ok(())
    }

    fn unwrap(
        &mut self,
        _: crate::gen3rpc_capnp::option::UnwrapParams<C>,
        mut response: crate::gen3rpc_capnp::option::UnwrapResults<C>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        match &self.inner {
            Some(t) => {
                pry!(response.get().set_some(t.clone()));
                Promise::ok(())
            }
            None => Promise::err(capnp::Error {
                kind: capnp::ErrorKind::Failed,
                extra: "Tried to unwrap a none".into(),
            }),
        }
    }

    fn unwrap_or(
        &mut self,
        params: crate::gen3rpc_capnp::option::UnwrapOrParams<C>,
        mut response: crate::gen3rpc_capnp::option::UnwrapOrResults<C>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        match &self.inner {
            Some(t) => {
                pry!(response.get().set_result(t.clone()));
            }
            None => {
                let or = pry!(pry!(params.get()).get_or());
                pry!(response.get().set_result(or));
            }
        }
        Promise::ok(())
    }
}

impl<T> From<Option<T>> for OptionImpl<T> {
    fn from(value: Option<T>) -> Self {
        OptionImpl { inner: value }
    }
}

pub struct ResultImpl<T, E> {
    pub inner: Result<T, E>,
}

impl<T, E> From<Result<T, E>> for ResultImpl<T, E> {
    fn from(value: Result<T, E>) -> Self {
        ResultImpl { inner: value }
    }
}

// Probably need to send a PR so this can be derived from the implementation that follows
impl<T: Clone + FromClientHook>
    crate::gen3rpc_capnp::result::Server<capnp::any_pointer::Owned, capnp::any_pointer::Owned>
    for ResultImpl<HiddenClient<T>, HiddenClient<T>>
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::result::GetParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: crate::gen3rpc_capnp::result::GetResults<
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
        _: crate::gen3rpc_capnp::result::IsOkParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: crate::gen3rpc_capnp::result::IsOkResults<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response.get().set_some(self.inner.is_ok());
        Promise::ok(())
    }
    fn unwrap(
        &mut self,
        _: crate::gen3rpc_capnp::result::UnwrapParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: crate::gen3rpc_capnp::result::UnwrapResults<
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
        params: crate::gen3rpc_capnp::result::UnwrapOrParams<
            capnp::any_pointer::Owned,
            capnp::any_pointer::Owned,
        >,
        mut response: crate::gen3rpc_capnp::result::UnwrapOrResults<
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

impl<T, E, C, D> crate::gen3rpc_capnp::result::Server<C, D> for ResultImpl<T, E>
where
    T: capnp::traits::SetterInput<C> + Clone,
    E: capnp::traits::SetterInput<D> + Clone,
    C: capnp::traits::Owned,
    D: capnp::traits::Owned,
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::result::GetParams<C, D>,
        mut response: crate::gen3rpc_capnp::result::GetResults<C, D>,
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
        _: crate::gen3rpc_capnp::result::IsOkParams<C, D>,
        mut response: crate::gen3rpc_capnp::result::IsOkResults<C, D>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        response.get().set_some(self.inner.is_ok());
        Promise::ok(())
    }

    fn unwrap(
        &mut self,
        _: crate::gen3rpc_capnp::result::UnwrapParams<C, D>,
        mut response: crate::gen3rpc_capnp::result::UnwrapResults<C, D>,
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
        params: crate::gen3rpc_capnp::result::UnwrapOrParams<C, D>,
        mut response: crate::gen3rpc_capnp::result::UnwrapOrResults<C, D>,
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

#[derive(PartialEq, Eq)]
pub enum DRState {
    Unshared,
    Exclusive,
    Shared(usize),
}

struct HiddenClient<T: Clone + FromClientHook> {
    client: T,
}

pub struct DroppableReferenceImpl<T: 'static + Send + Sync, C> {
    pub state: Arc<RwLock<DRState>>,
    pub inner: Arc<RwLock<T>>,
    pub stale: bool,
    pub phantom: PhantomData<fn() -> C>, //TODO: Should this be Send+Sync
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

impl<T: Send + Sync, C> DroppableReferenceImpl<T, C> {
    fn new(val: T) -> Self {
        DroppableReferenceImpl {
            state: Arc::new(RwLock::new(DRState::Unshared)),
            inner: Arc::new(RwLock::new(val)),
            stale: false,
            phantom: PhantomData,
        }
    }

    fn lock_mut(&self) -> Result<RwLockWriteGuard<'_, T>, capnp::Error> {
        let ls = self.state.read().unwrap();
        match *ls {
            DRState::Exclusive => Ok(self.inner.write().unwrap()),
            _ => Err(capnp::Error::failed("Client does not have an exclusive reference to resource but called an exclusive method".into()))
        }
    }

    pub fn try_clone(&self) -> Result<Self, TryLockError<RwLockWriteGuard<DRState>>> {
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

    pub fn clone_weak(&self) -> Self {
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

macro_rules! droppable_reference {
    ($inner_trait:path, $client_type:path) => {
        impl<T: $inner_trait + Send + Sync> crate::gen3rpc_capnp::droppable_reference::Server
            for DroppableReferenceImpl<T, $client_type>
        {
            fn drop(
                &mut self,
                _: crate::gen3rpc_capnp::droppable_reference::DropParams,
                _: crate::gen3rpc_capnp::droppable_reference::DropResults,
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
                _: crate::gen3rpc_capnp::droppable_reference::IsMutParams,
                mut response: crate::gen3rpc_capnp::droppable_reference::IsMutResults,
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
                _: crate::gen3rpc_capnp::droppable_reference::DropMutParams,
                mut response: crate::gen3rpc_capnp::droppable_reference::DropMutResults,
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
                _: crate::gen3rpc_capnp::droppable_reference::TryIntoMutParams,
                mut response: crate::gen3rpc_capnp::droppable_reference::TryIntoMutResults,
            ) -> capnp::capability::Promise<(), capnp::Error> {
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

droppable_reference!(DDCChannel, crate::gen3rpc_capnp::ddc_channel::Client);
impl<T: DDCChannel + Send + Sync> crate::gen3rpc_capnp::ddc_channel::Server
    for DroppableReferenceImpl<T, crate::gen3rpc_capnp::ddc_channel::Client>
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::ddc_channel::GetParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::GetResults,
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
        params: crate::gen3rpc_capnp::ddc_channel::SetParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::SetResults,
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
        params: crate::gen3rpc_capnp::ddc_channel::SetSourceParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::SetSourceResults,
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
        params: crate::gen3rpc_capnp::ddc_channel::SetDdcFreqParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::SetDdcFreqResults,
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
        params: crate::gen3rpc_capnp::ddc_channel::SetRotationParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::SetRotationResults,
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
        params: crate::gen3rpc_capnp::ddc_channel::SetCenterParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::SetCenterResults,
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
        _: crate::gen3rpc_capnp::ddc_channel::GetBasebandFrequencyParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::GetBasebandFrequencyResults,
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
        _: crate::gen3rpc_capnp::ddc_channel::GetDestParams,
        mut response: crate::gen3rpc_capnp::ddc_channel::GetDestResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        response
            .get()
            .set_dest_bin(self.inner.read().unwrap().get_dest());
        Promise::ok(())
    }
}

droppable_reference!(DSPScale, crate::gen3rpc_capnp::dsp_scale::Client);
impl<T: DSPScale + Send + Sync> crate::gen3rpc_capnp::dsp_scale::Server
    for DroppableReferenceImpl<T, crate::gen3rpc_capnp::dsp_scale::Client>
{
    fn get_fft_scale(
        &mut self,
        _: crate::gen3rpc_capnp::dsp_scale::GetFftScaleParams,
        mut response: crate::gen3rpc_capnp::dsp_scale::GetFftScaleResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let mut s16 = response.get().init_scale();
        s16.set_scale(self.inner.read().unwrap().get());
        Promise::ok(())
    }

    fn set_fft_scale(
        &mut self,
        params: crate::gen3rpc_capnp::dsp_scale::SetFftScaleParams,
        mut response: crate::gen3rpc_capnp::dsp_scale::SetFftScaleResults,
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

droppable_reference!(IFBoard, crate::gen3rpc_capnp::if_board::Client);
impl<T: IFBoard + Send + Sync> crate::gen3rpc_capnp::if_board::Server
    for DroppableReferenceImpl<T, crate::gen3rpc_capnp::if_board::Client>
{
    fn get_freq(
        &mut self,
        _: crate::gen3rpc_capnp::if_board::GetFreqParams,
        mut response: crate::gen3rpc_capnp::if_board::GetFreqResults,
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
        params: crate::gen3rpc_capnp::if_board::SetFreqParams,
        mut response: crate::gen3rpc_capnp::if_board::SetFreqResults,
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
        _: crate::gen3rpc_capnp::if_board::GetAttensParams,
        mut response: crate::gen3rpc_capnp::if_board::GetAttensResults,
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
        params: crate::gen3rpc_capnp::if_board::SetAttensParams,
        mut response: crate::gen3rpc_capnp::if_board::SetAttensResults,
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

droppable_reference!(DACTable, crate::gen3rpc_capnp::dac_table::Client);
impl<T: DACTable + Send + Sync> crate::gen3rpc_capnp::dac_table::Server
    for DroppableReferenceImpl<T, crate::gen3rpc_capnp::dac_table::Client>
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::dac_table::GetParams,
        mut response: crate::gen3rpc_capnp::dac_table::GetResults,
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
        params: crate::gen3rpc_capnp::dac_table::SetParams,
        _: crate::gen3rpc_capnp::dac_table::SetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_exc());
        let replace = pry!(pry!(pry!(params.get()).get_replace()).get_data());

        let mut values = Box::new([Complex::new(0, 0); 524288]);
        assert_eq!(values.len(), replace.len() as usize);

        for k in 0..replace.len() {
            values[k as usize] = Complex::new(replace.get(k).get_real(), replace.get(k).get_imag())
        }
        let mut i = self.inner.write().unwrap();
        i.set(values);

        Promise::ok(())
    }
    fn get_capabilities(
        &mut self,
        _: crate::gen3rpc_capnp::dac_table::GetCapabilitiesParams,
        mut response: crate::gen3rpc_capnp::dac_table::GetCapabilitiesResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let inner = self.inner.read().unwrap();
        let caps = inner.get_capabilities();
        response.get().set_length(caps.length as u64);
        let mut sr = response.get().init_sample_rate();
        sr.set_numerator(*caps.bw.numer());
        sr.set_denominator(*caps.bw.denom());

        Promise::ok(())
    }
}

droppable_reference!(Snap, crate::gen3rpc_capnp::snap::Client);
impl<T: Snap + Send + Sync> crate::gen3rpc_capnp::snap::Server
    for DroppableReferenceImpl<T, crate::gen3rpc_capnp::snap::Client>
{
    fn get(
        &mut self,
        _: crate::gen3rpc_capnp::snap::GetParams,
        mut results: crate::gen3rpc_capnp::snap::GetResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        pry!(self.check_shared());
        let b = self.inner.read().unwrap().get();
        match b {
            crate::Snap::Raw(v) => {
                let mut bv = results.get().init_raw_iq(v.len() as u32);
                for (i, c) in v.iter().enumerate() {
                    let mut bc = bv.reborrow().get(i as u32);
                    bc.set_real(c.re);
                    bc.set_imag(c.im);
                }
            }
            crate::Snap::DdcIQ(v) => {
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
            crate::Snap::Phase(v) => {
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
