pub mod gen3rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/gen3rpc_capnp.rs"));
}

pub mod client;
pub mod server;
pub mod utils;

use std::fmt::Display;

use capnp::traits::{FromPointerBuilder, SetterInput};
use num::Rational64;
use num_complex::Complex;

pub type Hertz = Rational64;

impl SetterInput<gen3rpc_capnp::void_struct::Owned> for () {
    fn set_pointer_builder(
        _builder: capnp::private::layout::PointerBuilder<'_>,
        _input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        Ok(())
    }
}
impl SetterInput<gen3rpc_capnp::hertz::Owned> for Hertz {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let builder = gen3rpc_capnp::hertz::Builder::init_pointer(builder, 1);
        let mut f = builder.init_frequency();
        f.set_numerator(*input.numer());
        f.set_denominator(*input.denom());
        Ok(())
    }
}

impl SetterInput<gen3rpc_capnp::complex_int32::Owned> for Complex<i32> {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::complex_int32::Builder::init_pointer(builder, 1);
        builder.set_real(input.re);
        builder.set_imag(input.im);
        Ok(())
    }
}

impl SetterInput<gen3rpc_capnp::complex_int16::Owned> for Complex<i16> {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::complex_int16::Builder::init_pointer(builder, 1);
        builder.set_real(input.re);
        builder.set_imag(input.im);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Gen3RpcError {
    CapnProto(capnp::Error),
    Capture(CaptureError),
    ChannelConfig(ChannelConfigError),
    ChannelAllocation(ChannelAllocationError),
    Frequency(FrequencyError),
    Atten(AttenError),
    DSPScale(DSPScaleError),
}

#[derive(Debug, Clone)]
pub enum CaptureError {
    CapnProto(capnp::Error),
    UnsupportedTap,
    MemoryUnavailable,
}

impl<T: Into<capnp::Error>> From<T> for CaptureError {
    fn from(value: T) -> Self {
        CaptureError::CapnProto(value.into())
    }
}

impl From<CaptureError> for Gen3RpcError {
    fn from(value: CaptureError) -> Self {
        Gen3RpcError::Capture(value)
    }
}

impl SetterInput<gen3rpc_capnp::capture::capture_error::Owned> for CaptureError {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::capture::capture_error::Builder::init_pointer(builder, 1);
        match input {
            CaptureError::CapnProto(e) => return Err(e),
            Self::UnsupportedTap => builder.set_unsupported_tap(()),
            Self::MemoryUnavailable => builder.set_memory_unavilable(()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ChannelConfigError {
    CapnProto(capnp::Error),
    SourceDestIncompatability,
    UsedTooManyBits,
}

impl<T: Into<capnp::Error>> From<T> for ChannelConfigError {
    fn from(value: T) -> Self {
        ChannelConfigError::CapnProto(value.into())
    }
}

impl From<ChannelConfigError> for Gen3RpcError {
    fn from(value: ChannelConfigError) -> Self {
        Gen3RpcError::ChannelConfig(value)
    }
}

impl SetterInput<gen3rpc_capnp::ddc_channel::channel_config_error::Owned> for ChannelConfigError {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            gen3rpc_capnp::ddc_channel::channel_config_error::Builder::init_pointer(builder, 1);

        match input {
            Self::CapnProto(e) => return Err(e),
            Self::SourceDestIncompatability => builder.set_source_dest_incompatible(()),
            Self::UsedTooManyBits => builder.set_used_too_many_bits(()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ChannelAllocationError {
    CapnProto(capnp::Error),
    OutOfChannels,
    DestinationInUse,
    ConfigError(ChannelConfigError),
}

impl From<ChannelConfigError> for ChannelAllocationError {
    fn from(value: ChannelConfigError) -> Self {
        Self::ConfigError(value)
    }
}

impl<T: Into<capnp::Error>> From<T> for ChannelAllocationError {
    fn from(value: T) -> Self {
        ChannelAllocationError::CapnProto(value.into())
    }
}

impl From<ChannelAllocationError> for Gen3RpcError {
    fn from(value: ChannelAllocationError) -> Self {
        Gen3RpcError::ChannelAllocation(value)
    }
}

impl SetterInput<gen3rpc_capnp::ddc::channel_allocation_error::Owned> for ChannelAllocationError {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            gen3rpc_capnp::ddc::channel_allocation_error::Builder::init_pointer(builder, 1);
        match input {
            Self::CapnProto(e) => return Err(e),
            Self::OutOfChannels => builder.set_out_of_channels(()),
            Self::DestinationInUse => builder.set_destination_in_use(()),
            Self::ConfigError(ChannelConfigError::SourceDestIncompatability) => {
                builder.set_source_dest_incompatible(())
            }
            Self::ConfigError(ChannelConfigError::UsedTooManyBits) => {
                builder.set_used_too_many_bits(())
            }
            Self::ConfigError(ChannelConfigError::CapnProto(e)) => return Err(e),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum FrequencyError {
    CapnProto(capnp::Error),
    CouldntLock,
    Unachievable,
}

impl<T: Into<capnp::Error>> From<T> for FrequencyError {
    fn from(value: T) -> Self {
        FrequencyError::CapnProto(value.into())
    }
}

impl From<FrequencyError> for Gen3RpcError {
    fn from(value: FrequencyError) -> Self {
        Gen3RpcError::Frequency(value)
    }
}

impl SetterInput<gen3rpc_capnp::if_board::freq_error::Owned> for FrequencyError {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::if_board::freq_error::Builder::init_pointer(builder, 1);
        match input {
            Self::CapnProto(e) => return Err(e),
            Self::Unachievable => builder.set_unachievable(()),
            Self::CouldntLock => builder.set_couldnt_lock(()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum AttenError {
    CapnProto(capnp::Error),
    Unachievable,
    Unsafe,
}

impl<T: Into<capnp::Error>> From<T> for AttenError {
    fn from(value: T) -> Self {
        AttenError::CapnProto(value.into())
    }
}

impl From<AttenError> for Gen3RpcError {
    fn from(value: AttenError) -> Self {
        Gen3RpcError::Atten(value)
    }
}

impl SetterInput<gen3rpc_capnp::if_board::atten_error::Owned> for AttenError {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::if_board::atten_error::Builder::init_pointer(builder, 1);
        match input {
            Self::CapnProto(e) => return Err(e),
            Self::Unachievable => builder.set_unachievable(()),
            Self::Unsafe => builder.set_unsafe(()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum DSPScaleError {
    CapnProto(capnp::Error),
    Clamped(u16),
}

impl<T: Into<capnp::Error>> From<T> for DSPScaleError {
    fn from(value: T) -> Self {
        DSPScaleError::CapnProto(value.into())
    }
}

impl From<DSPScaleError> for Gen3RpcError {
    fn from(value: DSPScaleError) -> Self {
        Gen3RpcError::DSPScale(value)
    }
}

#[derive(Debug, Clone)]
pub enum Snap {
    Raw(Vec<Complex<i16>>),
    DdcIQ(Vec<Vec<Complex<i16>>>),
    Phase(Vec<Vec<i16>>),
}

impl SetterInput<gen3rpc_capnp::snap::snap::Owned> for Snap {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let builder = gen3rpc_capnp::snap::snap::Builder::init_pointer(builder, 1);
        match input {
            Self::Raw(v) => {
                let mut bv = builder.init_raw_iq(v.len() as u32);
                for (i, c) in v.iter().enumerate() {
                    let mut bc = bv.reborrow().get(i as u32);
                    bc.set_real(c.re);
                    bc.set_imag(c.im);
                }
            }
            Self::DdcIQ(v) => {
                let mut bv = builder.init_ddc_iq(v.len() as u32);
                for (i, cv) in v.iter().enumerate() {
                    let mut bcv = bv.reborrow().init(i as u32, cv.len() as u32);
                    for (j, c) in cv.iter().enumerate() {
                        let mut bc = bcv.reborrow().get(j as u32);
                        bc.set_real(c.re);
                        bc.set_imag(c.im);
                    }
                }
            }
            Self::Phase(v) => {
                let mut bv = builder.init_phase(v.len() as u32);
                for (i, pv) in v.iter().enumerate() {
                    let mut bpv = bv.reborrow().init(i as u32, pv.len() as u32);
                    for (j, p) in pv.iter().enumerate() {
                        bpv.set(j as u32, *p);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum SnapAvg {
    Raw(Complex<f64>),
    DdcIQ(Vec<Complex<f64>>),
    Phase(Vec<f64>),
}

impl SetterInput<gen3rpc_capnp::snap::snap_avg::Owned> for SnapAvg {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let builder = gen3rpc_capnp::snap::snap_avg::Builder::init_pointer(builder, 1);
        match input {
            Self::Raw(c) => {
                let mut bc = builder.init_raw_iq();
                bc.set_real(c.re);
                bc.set_imag(c.im);
            }
            Self::DdcIQ(v) => {
                let mut bv = builder.init_ddc_iq(v.len() as u32);
                for (i, bc) in v.iter().enumerate() {
                    bv.reborrow().get(i as u32).set_real(bc.re);
                    bv.reborrow().get(i as u32).set_imag(bc.im);
                }
            }
            Self::Phase(v) => {
                let mut bv = builder.init_phase(v.len() as u32);
                for (i, pv) in v.iter().enumerate() {
                    bv.set(i as u32, *pv);
                }
            }
        }
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum BinControl {
    FullSwizzle,
    None,
}

#[derive(Clone, Debug)]
pub struct ErasedDDCChannelConfig {
    pub source_bin: u32,
    pub ddc_freq: i32,
    pub rotation: i32,
    pub center: Complex<i32>,
}

impl ErasedDDCChannelConfig {
    pub fn with_dest(&self, dest_bin: u32) -> ActualizedDDCChannelConfig {
        ActualizedDDCChannelConfig {
            source_bin: self.source_bin,
            ddc_freq: self.ddc_freq,
            dest_bin,
            rotation: self.rotation,
            center: self.center,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActualizedDDCChannelConfig {
    pub source_bin: u32,
    pub ddc_freq: i32,
    pub dest_bin: u32,
    pub rotation: i32,
    pub center: Complex<i32>,
}

impl ActualizedDDCChannelConfig {
    pub fn erase(self) -> ErasedDDCChannelConfig {
        ErasedDDCChannelConfig {
            source_bin: self.source_bin,
            ddc_freq: self.ddc_freq,
            rotation: self.rotation,
            center: self.center,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DDCChannelConfig {
    pub source_bin: u32,
    pub ddc_freq: i32,
    pub dest_bin: Option<u32>,
    pub rotation: i32,
    pub center: Complex<i32>,
}

impl DDCChannelConfig {
    pub fn erase(self) -> ErasedDDCChannelConfig {
        ErasedDDCChannelConfig {
            source_bin: self.source_bin,
            ddc_freq: self.ddc_freq,
            rotation: self.rotation,
            center: self.center,
        }
    }

    pub fn actualize(self) -> Result<ActualizedDDCChannelConfig, ErasedDDCChannelConfig> {
        if let Some(dest) = self.dest_bin {
            Ok(ActualizedDDCChannelConfig {
                source_bin: self.source_bin,
                ddc_freq: self.ddc_freq,
                dest_bin: dest,
                rotation: self.rotation,
                center: self.center,
            })
        } else {
            Err(self.erase())
        }
    }
}

impl SetterInput<gen3rpc_capnp::ddc_channel::channel_config::Owned> for DDCChannelConfig {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            gen3rpc_capnp::ddc_channel::channel_config::Builder::init_pointer(builder, 1);
        builder.set_source_bin(input.source_bin);
        builder.set_ddc_freq(input.ddc_freq);
        builder.set_rotation(input.rotation);

        let mut cb = builder.reborrow().init_center();
        cb.set_real(input.center.re);
        cb.set_imag(input.center.im);

        let mut db = builder.reborrow().init_destination_bin();
        match input.dest_bin {
            Some(i) => db.set_some(i),
            None => db.set_none(()),
        }
        Ok(())
    }
}

impl TryFrom<gen3rpc_capnp::ddc_channel::channel_config::Reader<'_>> for DDCChannelConfig {
    type Error = capnp::Error;
    fn try_from(
        value: gen3rpc_capnp::ddc_channel::channel_config::Reader<'_>,
    ) -> Result<Self, Self::Error> {
        Ok(DDCChannelConfig {
            source_bin: value.get_source_bin(),
            ddc_freq: value.get_ddc_freq(),
            dest_bin: match value.get_destination_bin().which()? {
                gen3rpc_capnp::ddc_channel::channel_config::destination_bin::Which::None(()) => {
                    None
                }
                gen3rpc_capnp::ddc_channel::channel_config::destination_bin::Which::Some(t) => {
                    Some(t)
                }
            },
            rotation: value.get_rotation(),
            center: Complex {
                re: value.get_center()?.get_real(),
                im: value.get_center()?.get_imag(),
            },
        })
    }
}

#[derive(Copy, Clone)]
pub struct DDCCapabilities {
    pub freq_resolution: Rational64,
    pub freq_bits: u16,
    pub rotation_bits: u16,
    pub center_bits: u16,
    pub bin_control: BinControl,
}

impl SetterInput<gen3rpc_capnp::ddc::capabilities::Owned> for DDCCapabilities {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::ddc::capabilities::Builder::init_pointer(builder, 1);
        builder.set_freq_bits(input.freq_bits);
        builder.set_rotation_bits(input.rotation_bits);
        builder.set_center_bits(input.center_bits);
        match input.bin_control {
            BinControl::FullSwizzle => builder.reborrow().init_bin_control().set_full_swizzle(()),
            BinControl::None => builder.reborrow().init_bin_control().set_none(()),
        }

        let mut fr = builder.reborrow().init_freq_resolution();
        fr.set_numerator(*input.freq_resolution.numer());
        fr.set_denominator(*input.freq_resolution.denom());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Attens {
    pub input: f32,
    pub output: f32,
}

impl Display for Attens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("o{}dB->i{}dB", self.input, self.output))
    }
}

impl SetterInput<gen3rpc_capnp::if_board::attens::Owned> for Attens {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = gen3rpc_capnp::if_board::attens::Builder::init_pointer(builder, 1);
        builder.set_input(input.input);
        builder.set_output(input.output);
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct Scale16 {
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
