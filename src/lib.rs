pub mod gen3rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/gen3rpc_capnp.rs"));
}

pub mod client;

use capnp::traits::{FromPointerBuilder, SetterInput};
use num::Rational64;
use num_complex::Complex;

pub type Hertz = Rational64;

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
pub enum ChannelAllocationError {
    CapnProto(capnp::Error),
    OutOfChannels,
    DestinationInUse,
    SourceDestIncompatability,
    UsedTooManyBits,
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
            Self::SourceDestIncompatability => builder.set_source_dest_incompatible(()),
            Self::UsedTooManyBits => builder.set_used_too_many_bits(()),
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

#[derive(Debug)]
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

pub enum BinControl {
    FullSwizzle,
    None,
}

#[derive(Clone, Debug)]
pub struct DDCChannelConfig {
    pub source_bin: u32,
    pub ddc_freq: i32,
    pub dest_bin: Option<u32>,
    pub rotation: i32,
    pub center: Complex<i32>,
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

pub struct Attens {
    pub input: f32,
    pub output: f32,
}
