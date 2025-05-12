use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use crate::*;

use super::gen3rpc_capnp;
use super::gen3rpc_capnp::capture::capture_error::Which as CEWhich;
use super::gen3rpc_capnp::complex_float64::Reader as CF64Reader;
use super::gen3rpc_capnp::complex_int16::Reader as C16Reader;
use super::gen3rpc_capnp::complex_int32::Reader as C32Reader;
use super::gen3rpc_capnp::ddc::capabilities::bin_control::Which as BCWhich;
use super::gen3rpc_capnp::ddc::channel_allocation_error::Which as CAEWhich;
use super::gen3rpc_capnp::if_board::atten_error::Which as AEWhich;
use super::gen3rpc_capnp::if_board::freq_error::Which as FEWhich;
use super::gen3rpc_capnp::option::option::Which as OWhich;
use super::gen3rpc_capnp::rational::Reader as RatReader;
use super::gen3rpc_capnp::result::result::Which as RWhich;
use super::gen3rpc_capnp::snap::snap::Which as SWhich;
use super::gen3rpc_capnp::snap::snap_avg::Which as SAWhich;

use capnp::{
    capability::FromClientHook,
    traits::{FromPointerReader, HasTypeId},
};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures;
use futures_io::{AsyncRead, AsyncWrite};
use num::Rational64;
use num_complex::Complex;
use utils::client::DACCapabilities;

pub struct CaptureTap<'a> {
    pub rfchain: &'a RFChain<'a>,
    pub tap: Tap<'a>,
}

impl<'a> CaptureTap<'a> {
    pub fn new(rfchain: &'a RFChain<'a>, tap: Tap<'a>) -> CaptureTap<'a> {
        CaptureTap { rfchain, tap }
    }
}

pub struct RFChain<'a> {
    pub dac_table: &'a DACTable,
    pub if_board: &'a IFBoard,
    pub dsp_scale: &'a DSPScale,
}

#[derive(Clone)]
pub enum Tap<'a> {
    RawIQ,
    DDCIQ(&'a [&'a SharedDroppableReference<gen3rpc_capnp::ddc_channel::Client, ()>]),
    Phase(&'a [&'a SharedDroppableReference<gen3rpc_capnp::ddc_channel::Client, ()>]),
}

impl<T, E> From<RWhich<T, E>> for Result<T, E> {
    fn from(value: RWhich<T, E>) -> Self {
        match value {
            RWhich::Ok(a) => Ok(a),
            RWhich::Error(a) => Err(a),
        }
    }
}

impl<T> From<OWhich<T>> for Option<T> {
    fn from(value: OWhich<T>) -> Self {
        match value {
            OWhich::Some(t) => Some(t),
            OWhich::None(_) => None,
        }
    }
}

impl From<CEWhich> for CaptureError {
    fn from(value: CEWhich) -> Self {
        match value {
            CEWhich::UnsupportedTap(_) => Self::UnsupportedTap,
            CEWhich::MemoryUnavilable(_) => Self::MemoryUnavailable,
        }
    }
}

impl From<CAEWhich> for ChannelAllocationError {
    fn from(value: CAEWhich) -> Self {
        match value {
            CAEWhich::OutOfChannels(_) => Self::OutOfChannels,
            CAEWhich::DestinationInUse(_) => Self::DestinationInUse,
            CAEWhich::SourceDestIncompatible(_) => {
                Self::ConfigError(ChannelConfigError::SourceDestIncompatability)
            }
            CAEWhich::UsedTooManyBits(_) => Self::ConfigError(ChannelConfigError::UsedTooManyBits),
        }
    }
}

impl From<FEWhich> for FrequencyError {
    fn from(value: FEWhich) -> Self {
        match value {
            FEWhich::CouldntLock(_) => Self::CouldntLock,
            FEWhich::Unachievable(_) => Self::Unachievable,
        }
    }
}

impl From<AEWhich> for AttenError {
    fn from(value: AEWhich) -> Self {
        match value {
            AEWhich::Unachievable(_) => Self::Unachievable,
            AEWhich::Unsafe(_) => Self::Unsafe,
        }
    }
}

impl From<BCWhich> for BinControl {
    fn from(value: BCWhich) -> Self {
        match value {
            BCWhich::FullSwizzle(_) => Self::FullSwizzle,
            BCWhich::None(_) => Self::None,
        }
    }
}

impl<'a> From<RatReader<'a>> for Rational64 {
    fn from(value: RatReader<'a>) -> Self {
        Rational64::new(value.get_numerator(), value.get_denominator())
    }
}

impl<'a> From<CF64Reader<'a>> for Complex<f64> {
    fn from(value: CF64Reader<'a>) -> Self {
        Self::new(value.get_real(), value.get_imag())
    }
}

impl<'a> From<C16Reader<'a>> for Complex<i16> {
    fn from(value: C16Reader<'a>) -> Self {
        Self::new(value.get_real(), value.get_imag())
    }
}

impl<'a> From<C32Reader<'a>> for Complex<i32> {
    fn from(value: C32Reader<'a>) -> Self {
        Self::new(value.get_real(), value.get_imag())
    }
}

pub struct ClientState<T: FromClientHook + HasTypeId, S> {
    pub(crate) client: T,
    state: S,
}

impl<T: FromClientHook + HasTypeId, S: Debug> Debug for ClientState<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientState")
            .field("state", &self.state)
            .field("client", &"<omitted>")
            .finish()
    }
}

#[derive(Debug)]
pub struct SharedDroppableReference<T: FromClientHook + HasTypeId, S> {
    client: ClientState<T, S>,
}

#[derive(Debug)]
pub struct ExclusiveDroppableReference<T: FromClientHook + HasTypeId, S> {
    client: ClientState<T, S>,
}

impl<T: FromClientHook + HasTypeId, S> Deref for SharedDroppableReference<T, S> {
    type Target = ClientState<T, S>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<T: FromClientHook + HasTypeId, S> Deref for ExclusiveDroppableReference<T, S> {
    type Target = ClientState<T, S>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<T: FromClientHook + HasTypeId, S> DerefMut for ExclusiveDroppableReference<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl<'a, T: FromClientHook + HasTypeId + FromPointerReader<'a>, S> SharedDroppableReference<T, S> {
    pub async fn try_into_mut(
        self,
    ) -> Result<
        Result<ExclusiveDroppableReference<T, S>, SharedDroppableReference<T, S>>,
        capnp::Error,
    > {
        let SharedDroppableReference { client } = self;
        let ClientState { client, state } = client;
        let client: gen3rpc_capnp::droppable_reference::Client = client.cast_to();
        let response = client.try_into_mut_request().send().promise.await?;
        let maybemut = response.get()?.get_maybe_mut()?;
        let mm = maybemut.get_request().send().promise.await?;
        let mmr = mm.get()?;
        let res: Result<_, _> = mmr.get_result()?.which()?.into();
        Ok(match res {
            Ok(t) => {
                let b = t?.get_as_capability()?;
                Ok(ExclusiveDroppableReference {
                    client: ClientState { client: b, state },
                })
            }
            Err(t) => Err(SharedDroppableReference {
                client: ClientState {
                    client: t?.get_as_capability()?,
                    state,
                },
            }),
        })
    }
}

impl<'a, T: FromClientHook + HasTypeId + Unpin + FromPointerReader<'a>, S>
    ExclusiveDroppableReference<T, S>
{
    pub async fn drop_mut(self) -> Result<SharedDroppableReference<T, S>, capnp::Error> {
        let ExclusiveDroppableReference { client } = self;
        let ClientState { client, state } = client;
        let client: gen3rpc_capnp::droppable_reference::Client = client.cast_to();
        let response = client.drop_mut_request().send().promise.await?;
        let r = response.get()?.get_nonmut();
        Ok(SharedDroppableReference {
            client: ClientState {
                client: r.get_as_capability()?,
                state,
            },
        })
    }
}

pub struct Gen3Board {
    pub client: gen3rpc_capnp::gen3_board::Client,
}

pub struct DDC {
    pub capabilities: DDCCapabilities,
    client: gen3rpc_capnp::ddc::Client,
}

pub type DACTable = ClientState<gen3rpc_capnp::dac_table::Client, ()>;

pub struct Capture {
    client: gen3rpc_capnp::capture::Client,
}

pub type IFBoard = ClientState<gen3rpc_capnp::if_board::Client, ()>;
pub type DSPScale = ClientState<gen3rpc_capnp::dsp_scale::Client, ()>;
pub type DDCChannel = ClientState<gen3rpc_capnp::ddc_channel::Client, ()>;

impl Gen3Board {
    pub fn new<T, U>(read: T, write: U) -> Self
    where
        T: AsyncRead + Unpin + 'static,
        U: AsyncWrite + Unpin + 'static,
    {
        let net = Box::new(twoparty::VatNetwork::new(
            futures::io::BufReader::new(read),
            futures::io::BufWriter::new(write),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut system = RpcSystem::new(net, None);
        Gen3Board {
            client: system.bootstrap(rpc_twoparty_capnp::Side::Server),
        }
    }

    pub async fn get_ddc(&self) -> Result<DDC, capnp::Error> {
        let ddc = self.client.get_ddc_request().send().promise.await;
        let client = ddc?.get()?.get_ddc()?;
        let response = client.capabilities_request().send().promise.await?;
        let capabilities = DDCCapabilities {
            opfb_channels: response.get()?.get_opfb_channels(),
            opfb_samplerate: response.get()?.get_opfb_sample_rate()?.into(),
            freq_resolution: response.get()?.get_freq_resolution()?.into(),
            freq_bits: response.get()?.get_freq_bits(),
            rotation_bits: response.get()?.get_rotation_bits(),
            center_bits: response.get()?.get_center_bits(),
            bin_control: response.get()?.get_bin_control().which()?.into(),
        };
        Ok(DDC {
            client,
            capabilities,
        })
    }

    pub async fn get_dac_table(
        &self,
    ) -> Result<SharedDroppableReference<gen3rpc_capnp::dac_table::Client, ()>, capnp::Error> {
        let dac_table = self.client.get_dac_table_request().send().promise.await;
        Ok(SharedDroppableReference {
            client: DACTable {
                client: dac_table?.get()?.get_dac_table()?,
                state: (),
            },
        })
    }

    pub async fn get_capture(&self) -> Result<Capture, capnp::Error> {
        let capture = self.client.get_capture_request().send().promise.await;
        Ok(Capture {
            client: capture?.get()?.get_capture()?,
        })
    }

    pub async fn get_dsp_scale(
        &self,
    ) -> Result<SharedDroppableReference<gen3rpc_capnp::dsp_scale::Client, ()>, capnp::Error> {
        let dsp_scale = self.client.get_dsp_scale_request().send().promise.await;
        Ok(SharedDroppableReference {
            client: DSPScale {
                client: dsp_scale?.get()?.get_dsp_scale()?,
                state: (),
            },
        })
    }

    pub async fn get_if_board(
        &self,
    ) -> Result<SharedDroppableReference<gen3rpc_capnp::if_board::Client, ()>, capnp::Error> {
        let ifboard = self.client.get_if_board_request().send().promise.await;
        Ok(SharedDroppableReference {
            client: IFBoard {
                client: ifboard?.get()?.get_if_board()?,
                state: (),
            },
        })
    }
}

impl DDCChannel {
    pub async fn get(&self) -> Result<ActualizedDDCChannelConfig, capnp::Error> {
        let response = self.client.get_request().send().promise.await?;
        let ack = response.get()?;
        Ok(ActualizedDDCChannelConfig {
            ddc_freq: ack.get_ddc_freq(),
            source_bin: ack.get_source_bin(),
            dest_bin: ack.get_dest_bin(),
            rotation: ack.get_rotation(),
            center: Complex::new(ack.get_center()?.get_real(), ack.get_center()?.get_imag()),
        })
    }

    pub async fn set(&mut self, config: ErasedDDCChannelConfig) -> Result<(), ChannelConfigError> {
        let mut request = self.client.set_request();
        let mut chan = request.get().init_replace();
        chan.set_source_bin(config.source_bin);
        chan.set_ddc_freq(config.ddc_freq);
        chan.set_rotation(config.rotation);
        let mut center = chan.reborrow().init_center();
        center.set_real(config.center.re);
        center.set_imag(config.center.im);
        request.send().promise.await?;
        Ok(())
    }
}

impl DDC {
    pub async fn allocate_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        SharedDroppableReference<gen3rpc_capnp::ddc_channel::Client, ()>,
        ChannelAllocationError,
    > {
        let mut request = self.client.allocate_channel_request();
        let mut cp_config = request.get().init_config();
        cp_config.set_source_bin(config.source_bin);
        cp_config.set_ddc_freq(config.ddc_freq);
        cp_config.set_rotation(config.rotation);
        let mut center = cp_config.reborrow().init_center();
        center.set_real(config.center.re);
        center.set_imag(config.center.im);
        match config.dest_bin {
            Some(i) => cp_config.init_destination_bin().set_some(i),
            None => cp_config.init_destination_bin().set_none(()),
        }
        let response = request.send().promise.await?;
        let result = response
            .get()?
            .get_result()?
            .get_request()
            .send()
            .promise
            .await?;

        let client: Result<_, _> = result.get()?.get_result()?.which()?.into();
        match client {
            Ok(p) => Ok(SharedDroppableReference {
                client: DDCChannel {
                    client: p?,
                    state: (),
                },
            }),
            Err(e) => {
                let e = e?;
                Err(e.which()?.into())
            }
        }
    }

    pub async fn retrieve_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        Option<SharedDroppableReference<gen3rpc_capnp::ddc_channel::Client, ()>>,
        capnp::Error,
    > {
        let mut request = self.client.retrieve_channel_request();
        let mut cp_config = request.get().init_config();
        cp_config.set_source_bin(config.source_bin);
        cp_config.set_ddc_freq(config.ddc_freq);
        cp_config.set_rotation(config.rotation);
        let mut center = cp_config.reborrow().init_center();
        center.set_real(config.center.re);
        center.set_imag(config.center.im);
        match config.dest_bin {
            Some(i) => cp_config.init_destination_bin().set_some(i),
            None => cp_config.init_destination_bin().set_none(()),
        }
        let response = request.send().promise.await?;
        let option = response
            .get()?
            .get_channel()?
            .get_request()
            .send()
            .promise
            .await?;
        let client: Option<Result<_, _>> = option.get()?.get_option()?.which()?.into();
        client.map_or(Ok(None), |v| {
            v.map(|client| {
                Some(SharedDroppableReference {
                    client: DDCChannel { client, state: () },
                })
            })
        })
    }
}

impl Capture {
    pub async fn average(&self, tap: CaptureTap<'_>, length: u64) -> Result<SnapAvg, CaptureError> {
        let mut request = self.client.average_request();
        let mut rtap = request.get().init_tap();
        let mut rfchain = rtap.reborrow().init_rf_chain();
        rfchain.set_dac_table(tap.rfchain.dac_table.client.clone());
        rfchain.set_if_board(tap.rfchain.if_board.client.clone());
        rfchain.set_dsp_scale(tap.rfchain.dsp_scale.client.clone());

        match tap.tap {
            Tap::RawIQ => {
                rtap.set_raw_iq(());
            }
            Tap::DDCIQ(ddcs) => {
                let mut taps = rtap.init_ddc_iq(ddcs.len() as u32);
                for (i, ddc) in ddcs.iter().enumerate() {
                    taps.set(i as u32, ddc.client.client.clone().into_client_hook())
                }
            }
            Tap::Phase(ddcs) => {
                let mut taps = rtap.init_phase(ddcs.len() as u32);
                for (i, ddc) in ddcs.iter().enumerate() {
                    taps.set(i as u32, ddc.client.client.clone().into_client_hook())
                }
            }
        }
        request.get().set_length(length);
        let response = request.send().promise.await?;
        let avg = response
            .get()?
            .get_result()?
            .get_request()
            .send()
            .promise
            .await?;
        let res: Result<_, _> = avg.get()?.get_result()?.which()?.into();

        match res {
            Ok(s) => match s?.which()? {
                SAWhich::RawIq(r) => {
                    let r = r?;
                    Ok(SnapAvg::Raw(r.into()))
                }
                SAWhich::DdcIq(d) => {
                    let d = d?;
                    Ok(SnapAvg::DdcIQ(d.iter().map(|c| c.into()).collect()))
                }
                SAWhich::Phase(p) => {
                    let p = p?;
                    Ok(SnapAvg::Phase(p.iter().collect()))
                }
            },
            Err(e) => {
                let a: Result<_, _> = e?.which();
                Err(a?.into())
            }
        }
    }

    pub async fn capture(&self, tap: CaptureTap<'_>, length: u64) -> Result<Snap, CaptureError> {
        let mut request = self.client.capture_request();
        let mut rtap = request.get().init_tap();
        let mut rfchain = rtap.reborrow().init_rf_chain();
        rfchain.set_dac_table(tap.rfchain.dac_table.client.clone());
        rfchain.set_if_board(tap.rfchain.if_board.client.clone());
        rfchain.set_dsp_scale(tap.rfchain.dsp_scale.client.clone());

        match tap.tap {
            Tap::RawIQ => {
                rtap.set_raw_iq(());
            }
            Tap::DDCIQ(ddcs) => {
                let mut taps = rtap.init_ddc_iq(ddcs.len() as u32);
                for (i, ddc) in ddcs.iter().enumerate() {
                    taps.set(i as u32, ddc.client.client.clone().into_client_hook())
                }
            }
            Tap::Phase(ddcs) => {
                let mut taps = rtap.init_phase(ddcs.len() as u32);
                for (i, ddc) in ddcs.iter().enumerate() {
                    taps.set(i as u32, ddc.client.client.clone().into_client_hook())
                }
            }
        }
        request.get().set_length(length);
        let response = request.send().promise.await?;
        let result = response
            .get()?
            .get_result()?
            .get_request()
            .send()
            .promise
            .await?;
        let snap = result.get()?.get_result()?.which()?.into();

        match snap {
            Ok(s) => {
                let request = s?.get_request();
                let response = request.send().promise.await?;
                match response.get()?.which()? {
                    SWhich::RawIq(r) => {
                        let r = r?;
                        Ok(Snap::Raw(r.iter().map(|r| r.into()).collect()))
                    }
                    SWhich::DdcIq(d) => {
                        let d = d?;
                        Ok(Snap::DdcIQ(
                            d.iter()
                                .map(|c| {
                                    c.map(|c| {
                                        c.iter().map(|iq| iq.into()).collect::<Vec<Complex<i16>>>()
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        ))
                    }
                    SWhich::Phase(p) => {
                        let p = p?;
                        Ok(Snap::Phase(
                            p.iter()
                                .map(|c| c.map(|c| c.iter().collect::<Vec<i16>>()))
                                .collect::<Result<Vec<_>, _>>()?,
                        ))
                    }
                }
            }
            Err(e) => {
                let a: Result<_, _> = e?.which();
                Err(a?.into())
            }
        }
    }
}

impl DACTable {
    pub async fn capabilties(&mut self) -> Result<DACCapabilities, capnp::Error> {
        let request = self.client.get_capabilities_request();
        let response = request.send().promise.await?;
        let caps = response.get()?;
        Ok(DACCapabilities {
            bw: Hertz::new(
                caps.get_sample_rate()?.get_numerator(),
                caps.get_sample_rate()?.get_denominator(),
            ),
            length: caps.get_length() as usize,
        })
    }

    pub async fn set_dac_table(&mut self, data: Vec<Complex<i16>>) -> Result<(), capnp::Error> {
        let mut request = self.client.set_request();
        request.get().init_replace().init_data(data.len() as u32);
        for (i, c) in data.iter().enumerate() {
            let mut cp = request.get().get_replace()?.get_data()?.get(i as u32);
            cp.set_real(c.re);
            cp.set_imag(c.im);
        }
        request.send().promise.await?;
        Ok(())
    }

    pub async fn get_dac_table(&self) -> Result<Vec<Complex<i16>>, capnp::Error> {
        let request = self.client.get_request();
        let response = request.send().promise.await?;

        let mut buf = vec![Complex::i(); 524288];

        for i in 0..response.get()?.get_data()?.len() {
            let v = response.get()?.get_data()?.get(i);
            buf[i as usize] = Complex::new(v.get_real(), v.get_imag())
        }

        Ok(buf)
    }
}

impl DSPScale {
    pub async fn get_fft_scale(&self) -> Result<u16, capnp::Error> {
        let request = self.client.get_fft_scale_request().send().promise.await?;
        let response = request.get()?.get_scale()?.get_scale();
        Ok(response)
    }

    pub async fn set_fft_scale(&mut self, scale: u16) -> Result<u16, DSPScaleError> {
        let mut request = self.client.set_fft_scale_request();
        request.get().init_scale().set_scale(scale);
        let response = request.send().promise.await?;
        let scale = response
            .get()?
            .get_scale()?
            .get_request()
            .send()
            .promise
            .await?;
        let scale: Result<_, _> = scale.get()?.get_result()?.which()?.into();
        match scale {
            Ok(a) => Ok(a?.get_scale()),
            Err(a) => Err(DSPScaleError::Clamped(a?.get_scale())),
        }
    }
}

impl IFBoard {
    pub async fn get_freq(&self) -> Result<Hertz, capnp::Error> {
        let request = self.client.get_freq_request().send().promise.await?;
        let response = request.get()?.get_freq()?.get_frequency()?;
        Ok(Hertz::new(
            response.get_numerator(),
            response.get_denominator(),
        ))
    }

    pub async fn set_freq(&mut self, freq: Hertz) -> Result<Hertz, FrequencyError> {
        let mut request = self.client.set_freq_request();
        let mut nd = request.get().init_freq().init_frequency();
        nd.set_numerator(*freq.numer());
        nd.set_denominator(*freq.denom());
        let response = request.send().promise.await?;
        let freq = response
            .get()?
            .get_freq()?
            .get_request()
            .send()
            .promise
            .await?;
        let hertz: Result<_, _> = freq.get()?.get_result()?.which()?.into();
        match hertz {
            Ok(f) => {
                let f = f?.get_frequency()?;
                Ok(Hertz::new(f.get_numerator(), f.get_denominator()))
            }
            Err(e) => {
                let e = e?.which()?;
                Err(e.into())
            }
        }
    }

    pub async fn get_attens(&self) -> Result<Attens, capnp::Error> {
        let request = self.client.get_attens_request().send().promise.await?;
        let response = request.get()?.get_attens()?;
        Ok(Attens {
            input: response.get_input(),
            output: response.get_output(),
        })
    }

    pub async fn set_attens(&self, attens: Attens) -> Result<Attens, AttenError> {
        let mut request = self.client.set_attens_request();
        let mut at = request.get().init_attens();
        at.set_input(attens.input);
        at.set_output(attens.output);
        let response = request.send().promise.await?;
        let at = response
            .get()?
            .get_attens()?
            .get_request()
            .send()
            .promise
            .await?;
        let attens: Result<_, _> = at.get()?.get_result()?.which()?.into();
        match attens {
            Ok(a) => {
                let a = a?;
                Ok(Attens {
                    input: a.get_input(),
                    output: a.get_output(),
                })
            }
            Err(e) => {
                let e = e?.which()?;
                Err(e.into())
            }
        }
    }
}
