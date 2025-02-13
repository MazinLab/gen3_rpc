pub mod gen3rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/gen3rpc_capnp.rs"));
}

use num::Rational64;
use num_complex::Complex;

// pub mod gen3rpc_capnp;

pub type Hertz = Rational64;

#[derive(Debug)]
pub enum Gen3RpcError {
    CapnProto(capnp::Error),
    Capture(CaptureError),
    ChannelAllocation(ChannelAllocationError),
    Frequency(FrequencyError),
    Atten(AttenError),
    DSPScale(DSPScaleError),
}

#[derive(Debug)]
pub enum CaptureError {
    CapnProto(capnp::Error),
    UnsupportedTap,
    MemoryUnavailable,
}

#[derive(Debug)]
pub enum ChannelAllocationError {
    CapnProto(capnp::Error),
    OutOfChannels,
    DestinationInUse,
    SourceDestIncompatability,
}

#[derive(Debug)]
pub enum FrequencyError {
    CapnProto(capnp::Error),
    CouldntLock,
    Unachievable,
}

#[derive(Debug)]
pub enum AttenError {
    CapnProto(capnp::Error),
    Unachievable,
    Unsafe,
}

#[derive(Debug)]
pub enum DSPScaleError {
    CapnProto(capnp::Error),
    Clamped(u16),
}

impl<T: Into<capnp::Error>> From<T> for CaptureError {
    fn from(value: T) -> Self {
        CaptureError::CapnProto(value.into())
    }
}

impl<T: Into<capnp::Error>> From<T> for ChannelAllocationError {
    fn from(value: T) -> Self {
        ChannelAllocationError::CapnProto(value.into())
    }
}

impl<T: Into<capnp::Error>> From<T> for FrequencyError {
    fn from(value: T) -> Self {
        FrequencyError::CapnProto(value.into())
    }
}

impl<T: Into<capnp::Error>> From<T> for AttenError {
    fn from(value: T) -> Self {
        AttenError::CapnProto(value.into())
    }
}

impl<T: Into<capnp::Error>> From<T> for DSPScaleError {
    fn from(value: T) -> Self {
        DSPScaleError::CapnProto(value.into())
    }
}

impl From<CaptureError> for Gen3RpcError {
    fn from(value: CaptureError) -> Self {
        Gen3RpcError::Capture(value)
    }
}

impl From<ChannelAllocationError> for Gen3RpcError {
    fn from(value: ChannelAllocationError) -> Self {
        Gen3RpcError::ChannelAllocation(value)
    }
}

impl From<FrequencyError> for Gen3RpcError {
    fn from(value: FrequencyError) -> Self {
        Gen3RpcError::Frequency(value)
    }
}

impl From<AttenError> for Gen3RpcError {
    fn from(value: AttenError) -> Self {
        Gen3RpcError::Atten(value)
    }
}

impl From<DSPScaleError> for Gen3RpcError {
    fn from(value: DSPScaleError) -> Self {
        Gen3RpcError::DSPScale(value)
    }
}

pub enum Snap {
    Raw(Vec<Complex<i16>>),
    DdcIQ(Vec<Vec<Complex<i16>>>),
    Phase(Vec<Vec<i16>>),
}

pub enum BinControl {
    FullSwizzle,
    None,
}

pub struct DDCChannelConfig {
    pub source_bin: u32,
    pub ddc_freq: i32,
    pub dest_bin: Option<u32>,
    pub rotation: i32,
    pub center: Complex<i32>,
}

pub struct DDCCapabilities {
    pub freq_resolution: Rational64,
    pub freq_bits: u16,
    pub rotation_bits: u16,
    pub center_bits: u16,
    pub bin_control: BinControl,
}

pub struct Attens {
    pub input: f32,
    pub output: f32,
}

pub mod client {

    use crate::*;

    use super::gen3rpc_capnp;
    use super::gen3rpc_capnp::capture::capture_error::Which as CEWhich;
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

    use capnp::capability::FromClientHook;
    use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
    use futures;
    use futures_io::{AsyncRead, AsyncWrite};
    use num::Rational64;
    use num_complex::Complex;

    pub enum CaptureTap<'a> {
        RawIQ,
        DDCIQ(Vec<&'a DDCChannel>),
        Phase(Vec<&'a DDCChannel>),
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
                CAEWhich::SourceDestIncompatible(_) => Self::SourceDestIncompatability,
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

    pub struct Gen3Board {
        pub client: gen3rpc_capnp::gen3_board::Client,
    }

    pub struct DDC {
        pub capabilities: DDCCapabilities,
        client: gen3rpc_capnp::ddc::Client,
    }

    pub struct DACTable {
        client: gen3rpc_capnp::dac_table::Client,
    }

    pub struct Capture {
        client: gen3rpc_capnp::capture::Client,
    }

    pub struct IFBoard {
        client: gen3rpc_capnp::if_board::Client,
    }

    pub struct DSPScale {
        client: gen3rpc_capnp::dsp_scale::Client,
    }

    pub struct DDCChannel {
        client: gen3rpc_capnp::ddc_channel::Client,
    }

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

        pub async fn get_dac_table(&self) -> Result<DACTable, capnp::Error> {
            let dac_table = self.client.get_dac_table_request().send().promise.await;
            Ok(DACTable {
                client: dac_table?.get()?.get_dac_table()?,
            })
        }

        pub async fn get_capture(&self) -> Result<Capture, capnp::Error> {
            let capture = self.client.get_capture_request().send().promise.await;
            Ok(Capture {
                client: capture?.get()?.get_capture()?,
            })
        }

        pub async fn get_dsp_scale(&self) -> Result<DSPScale, capnp::Error> {
            let dsp_scale = self.client.get_dsp_scale_request().send().promise.await;
            Ok(DSPScale {
                client: dsp_scale?.get()?.get_dsp_scale()?,
            })
        }

        pub async fn get_if_board(&self) -> Result<IFBoard, capnp::Error> {
            let ifboard = self.client.get_if_board_request().send().promise.await;
            Ok(IFBoard {
                client: ifboard?.get()?.get_if_board()?,
            })
        }
    }

    impl DDC {
        pub async fn allocate_channel(
            &self,
            config: DDCChannelConfig,
        ) -> Result<DDCChannel, ChannelAllocationError> {
            let mut request = self.client.allocate_channel_request();
            let mut cp_config = request.get().init_config();
            cp_config.set_source_bin(config.source_bin);
            cp_config.set_ddc_freq(config.ddc_freq);
            cp_config.set_rotation(config.rotation);
            let mut center = cp_config.init_center();
            center.set_real(config.center.re);
            center.set_imag(config.center.im);
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
                Ok(p) => Ok(DDCChannel { client: p? }),
                Err(e) => {
                    let e = e?;
                    Err(e.which()?.into())
                }
            }
        }
        pub async fn retrieve_channel(
            &self,
            config: DDCChannelConfig,
        ) -> Result<Option<DDCChannel>, capnp::Error> {
            let mut request = self.client.retrieve_channel_request();
            let mut cp_config = request.get().init_config();
            cp_config.set_source_bin(config.source_bin);
            cp_config.set_ddc_freq(config.ddc_freq);
            cp_config.set_rotation(config.rotation);
            let mut center = cp_config.init_center();
            center.set_real(config.center.re);
            center.set_imag(config.center.im);
            let response = request.send().promise.await?;
            let option = response
                .get()?
                .get_channel()?
                .get_request()
                .send()
                .promise
                .await?;
            let client: Option<Result<_, _>> = option.get()?.get_option()?.which()?.into();
            client.map_or(Ok(None), |v| v.map(|client| Some(DDCChannel { client })))
        }
    }

    impl Capture {
        pub async fn capture(
            &self,
            tap: CaptureTap<'_>,
            length: u64,
        ) -> Result<Snap, CaptureError> {
            let mut request = self.client.capture_request();
            match tap {
                CaptureTap::RawIQ => {
                    request.get().init_tap().set_raw_iq(());
                }
                CaptureTap::DDCIQ(ddcs) => {
                    let mut taps = request.get().init_tap().init_ddc_iq(ddcs.len() as u32);
                    for (i, ddc) in ddcs.into_iter().enumerate() {
                        taps.set(i as u32, ddc.client.clone().into_client_hook())
                    }
                }
                CaptureTap::Phase(ddcs) => {
                    let mut taps = request.get().init_tap().init_phase(ddcs.len() as u32);
                    for (i, ddc) in ddcs.into_iter().enumerate() {
                        taps.set(i as u32, ddc.client.clone().into_client_hook())
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
                                            c.iter()
                                                .map(|iq| iq.into())
                                                .collect::<Vec<Complex<i16>>>()
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
        pub async fn set_dac_table(
            &mut self,
            data: Box<[Complex<i16>; 524288]>,
        ) -> Result<(), capnp::Error> {
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

        pub async fn get_dac_table(&self) -> Result<Box<[Complex<i16>; 524288]>, capnp::Error> {
            let request = self.client.get_request();
            let response = request.send().promise.await?;

            let mut buf = Box::new([Complex::i(); 524288]);

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
}
