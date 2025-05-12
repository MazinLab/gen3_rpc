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

pub mod client {
    use futures::{
        future::{try_join, try_join_all},
        TryFutureExt,
    };
    use num_complex::Complex64;
    use std::sync::mpsc::Sender;

    use crate::{
        client::{self, Capture, CaptureTap, DACTable, DSPScale, IFBoard, RFChain},
        ActualizedDDCChannelConfig, AttenError, Attens, Gen3RpcError, Hertz, SnapAvg,
    };

    use num::{traits::Inv, Complex};

    use rand::prelude::*;
    use rustfft::FftPlanner;

    pub const FFT_AGC_OPTIONS: [u16; 13] = [
        0xFFF, 0xF7F, 0x77F, 0x777, 0x757, 0x755, 0x555, 0x515, 0x115, 0x111, 0x101, 0x001, 0x000,
    ];

    pub async fn agc(
        mut output: f32,
        start: f32,
        step: f32,
        dynamic_range: f32,
        tap: client::Tap<'_>,
        capture: &Capture,
        dac_table: &DACTable,
        if_board: &mut IFBoard,
        dsp_scale: &mut DSPScale,
    ) -> Result<PowerSetting, Gen3RpcError> {
        match tap {
            client::Tap::RawIQ => todo!(),
            client::Tap::Phase(_) => todo!(),
            client::Tap::DDCIQ(_) => (),
        };
        let mut input = start;
        loop {
            let att = if_board.set_attens(Attens { input, output }).await;
            match att {
                Ok(att) => {
                    output = att.output;
                    input = att.input;
                }
                Err(AttenError::Unachievable) => {
                    break;
                }
                Err(e) => {
                    if_board
                        .set_attens(Attens {
                            input: 63.5,
                            output: 63.5,
                        })
                        .await?;
                    Err(e)?;
                }
            }
            let snap = capture
                .capture(
                    CaptureTap {
                        rfchain: &RFChain {
                            dac_table,
                            if_board,
                            dsp_scale,
                        },
                        tap: client::Tap::RawIQ,
                    },
                    1 << 19,
                )
                .await?;
            let m = match snap {
                crate::Snap::Raw(r) => r.into_iter().fold(0, |a, j| {
                    a.max((j.re as i32).abs()).max((j.im as i32).abs())
                }),
                _ => {
                    unreachable!()
                }
            };
            let range = m as f32 / 32764.;
            if range > dynamic_range {
                input += step;
                break;
            }
            input -= step;
        }
        let att = if_board.set_attens(Attens { input, output }).await?;
        let mut idx = 0;
        while idx < FFT_AGC_OPTIONS.len() {
            dsp_scale.set_fft_scale(FFT_AGC_OPTIONS[idx]).await?;
            let snap = capture
                .capture(
                    CaptureTap {
                        rfchain: &RFChain {
                            dac_table,
                            if_board,
                            dsp_scale,
                        },
                        tap: tap.clone(),
                    },
                    1 << 19,
                )
                .await?;
            let m = match snap {
                crate::Snap::DdcIQ(d) => d
                    .into_iter()
                    .map(|r| {
                        r.into_iter().fold(0, |a, j| {
                            a.max((j.re as i32).abs()).max((j.im as i32).abs())
                        })
                    })
                    .max()
                    .unwrap_or(0),
                _ => {
                    unreachable!()
                }
            };
            let range = m as f32 / 32764.;
            if range > dynamic_range {
                idx = idx.saturating_sub(1);
                break;
            }
            idx += 1
        }
        idx = idx.min(FFT_AGC_OPTIONS.len());
        Ok(PowerSetting {
            attens: att,
            fft_scale: FFT_AGC_OPTIONS[idx],
        })
    }

    #[derive(PartialEq, Debug, Clone)]
    pub enum Tone<T: PartialEq> {
        Single { freq: T, amplitude: f64, phase: f64 },
    }

    impl<T: PartialEq> Tone<T> {
        pub fn randomize_phase(&mut self, rng: &mut ThreadRng) {
            match self {
                Tone::Single {
                    freq: _,
                    amplitude: _,
                    phase,
                } => *phase = rng.random_range(0.0..(2. * std::f64::consts::PI)),
            }
        }

        pub fn apply_gain(&mut self, gain: f64) {
            match self {
                Tone::Single {
                    freq: _,
                    amplitude,
                    phase: _,
                } => *amplitude *= gain,
            }
        }
    }

    pub type QuantizedTone = Tone<usize>;
    pub type ExactTone = Tone<Hertz>;
    pub type ImpreciseTone = Tone<f64>;

    pub trait Quantizable: Sized {
        fn quantize(self, capabilities: &DACCapabilities) -> Option<QuantizedTone>;
    }

    impl Quantizable for QuantizedTone {
        fn quantize(self, _capabilities: &DACCapabilities) -> Option<QuantizedTone> {
            Some(self)
        }
    }

    impl QuantizedTone {
        fn add_to(&self, ifft: &mut [Complex64]) {
            match self {
                Tone::Single {
                    freq,
                    amplitude,
                    phase,
                } => {
                    ifft[*freq] += amplitude
                        * Complex64::exp(2. * std::f64::consts::PI * Complex64::i() * phase)
                }
            }
        }

        pub fn to_exact(self, capabilties: &DACCapabilities) -> ExactTone {
            match self {
                Self::Single {
                    freq,
                    amplitude,
                    phase,
                } => {
                    let spacing = capabilties.bw * Hertz::new(1, capabilties.length as i64);

                    let freq = Hertz::new(freq as i64, 1) * spacing;
                    let freq = freq - capabilties.bw / 2;
                    ExactTone::Single {
                        freq,
                        amplitude,
                        phase,
                    }
                }
            }
        }
    }

    impl Quantizable for ExactTone {
        fn quantize(self, capabilities: &DACCapabilities) -> Option<QuantizedTone> {
            match self {
                Self::Single {
                    freq,
                    amplitude,
                    phase,
                } => {
                    // println!("{} {}", freq, capabilities.bw);
                    let freq = capabilities.bw / 2 + freq;
                    if freq > capabilities.bw {
                        return None;
                    }
                    let freq = freq * (capabilities.length as i64) / capabilities.bw;
                    let freq = freq.floor().reduced();
                    assert!(*freq.denom() == 1);
                    let freq = *freq.numer();
                    assert!(freq >= 0);
                    Some(QuantizedTone::Single {
                        freq: freq as usize,
                        amplitude,
                        phase,
                    })
                }
            }
        }
    }

    impl Quantizable for ImpreciseTone {
        fn quantize(self, capabilities: &DACCapabilities) -> Option<QuantizedTone> {
            self.to_exact()?.quantize(capabilities)
        }
    }

    impl ImpreciseTone {
        pub fn to_exact(self) -> Option<ExactTone> {
            match self {
                Self::Single {
                    freq,
                    amplitude,
                    phase,
                } => Some(ExactTone::Single {
                    freq: Hertz::approximate_float(freq)?,
                    amplitude,
                    phase,
                }),
            }
        }
    }

    #[derive(Copy, Clone)]
    pub struct DACCapabilities {
        pub bw: Hertz,
        pub length: usize,
    }

    impl DACCapabilities {
        pub fn resolution(&self) -> Hertz {
            self.bw / (self.length as i64)
        }
    }

    pub struct DACBuilder {
        pub capabilities: DACCapabilities,
        pub tones: Vec<QuantizedTone>,
    }

    impl DACBuilder {
        pub fn new(caps: DACCapabilities) -> Self {
            DACBuilder {
                capabilities: caps,
                tones: vec![],
            }
        }
        pub fn construct(&self) -> Vec<Complex64> {
            let mut planer = FftPlanner::new();
            let fft = planer.plan_fft_inverse(self.capabilities.length);
            let mut arr = vec![Complex::new(0., 0.); self.capabilities.length];
            for tone in self.tones.iter() {
                tone.add_to(&mut arr);
            }
            fft.process(&mut arr);
            arr
        }

        pub fn add_tones<T: IntoIterator<Item = QuantizedTone>>(mut self, i: T) -> Self {
            for tone in i.into_iter() {
                self.tones.push(tone);
            }
            self
        }

        pub fn try_add_tones<T: IntoIterator<Item = QuantizedTone>>(mut self, i: T) -> Self {
            for tone in i.into_iter() {
                self.tones.push(tone);
            }
            self
        }

        pub fn apply_gain(mut self, gain: f64) -> Self {
            for tone in self.tones.iter_mut() {
                tone.apply_gain(gain);
            }
            self
        }

        pub fn randomize_phases(mut self) -> Self {
            let mut rng = rand::rng();
            self.tones
                .iter_mut()
                .for_each(|t| t.randomize_phase(&mut rng));
            self
        }

        pub fn build(&self) -> Option<Vec<Complex<i16>>> {
            let p = self.construct();
            p.into_iter()
                .map(|c| {
                    Some(Complex::<i16>::new(
                        num::cast(c.re * 32764.)?,
                        num::cast(c.im * 32764.)?,
                    ))
                })
                .collect()
        }

        pub fn build_dynamic_range(&mut self, dynamic_range: f64) -> (f64, Vec<Complex<i16>>) {
            let mut max: f64 = 0.0;
            println!("{:?}", self.tones);
            let p = self.construct();
            for c in p.iter() {
                max = max.max(c.re.abs()).max(c.im.abs());
            }
            let gain = dynamic_range * max.inv();
            println!("{} {}", max, gain);
            (
                gain,
                p.into_iter()
                    .map(|c| {
                        Complex::<i16>::new(
                            num::cast(c.re * 32764. * gain).unwrap(),
                            num::cast(c.im * 32764. * gain).unwrap(),
                        )
                    })
                    .collect(),
            )
        }
    }

    pub mod test {
        #[test]
        fn test_scale() {
            use super::*;
            use num_complex::ComplexFloat;

            let caps = DACCapabilities {
                bw: Hertz::new(4_096_000_000, 1),
                length: 1 << 19,
            };
            let tone = QuantizedTone::Single {
                freq: 1024,
                amplitude: 1.0,
                phase: 0.0,
            };
            let builder = DACBuilder {
                capabilities: caps,
                tones: vec![tone],
            };
            let buf = builder.construct();
            assert_eq!(buf[0].re(), 1.0);
            assert_eq!(buf[0].im(), 0.0);
        }

        // #[test]
        // fn test_quantize() {
        //     use super::*;
        //     let caps = DACCapabilities {
        //         bw: Hertz::new(4_096_000_000, 1),
        //         length: 1 << 19,
        //     };

        //     assert_eq!(caps.resolution(), Hertz::new(78125, 10));
        //     let f = ExactTone::Single {
        //         freq: caps.resolution() * 16,
        //         amplitude: 1.,
        //         phase: 1.,
        //     };

        //     let f = f.quantize(&caps);
        //     assert_eq!(
        //         f.unwrap(),
        //         QuantizedTone::Single {
        //             freq: caps.length / 2 + 16,
        //             amplitude: 1.,
        //             phase: 1.
        //         }
        //     );
        // }
    }

    #[derive(Debug)]
    pub enum SweepResult {
        RawIQ(Vec<(Attens, Vec<Complex64>)>),
        DdcIQ(Vec<(PowerSetting, Vec<Vec<Complex64>>)>),
        Phase(Vec<(PowerSetting, Vec<Vec<f64>>)>),
    }

    impl SweepResult {
        pub fn push(&mut self, snap: SnapAvg, setting: PowerSetting, capacity: usize) {
            match (snap, self) {
                (SnapAvg::Raw(r), SweepResult::RawIQ(rh)) => {
                    for (a, v) in rh.iter_mut().rev() {
                        if *a == setting.attens {
                            v.push(r);
                            assert!(
                                v.len() < capacity,
                                "More samples recieved than should have been"
                            );
                            return;
                        }
                    }
                    let mut new = Vec::with_capacity(capacity);
                    new.push(r);
                    rh.push((setting.attens, new));
                }
                (SnapAvg::DdcIQ(d), SweepResult::DdcIQ(dh)) => {
                    for (a, v) in dh.iter_mut().rev() {
                        if *a == setting {
                            v.push(d);
                            assert!(
                                v.len() < capacity,
                                "More samples recieved than should have been"
                            );
                            return;
                        }
                    }
                    let mut new = Vec::with_capacity(capacity);
                    new.push(d);
                    dh.push((setting, new));
                }
                (SnapAvg::Phase(p), SweepResult::Phase(ph)) => {
                    for (a, v) in ph.iter_mut().rev() {
                        if *a == setting {
                            v.push(p);
                            assert!(
                                v.len() < capacity,
                                "More samples recieved than should have been"
                            );
                            return;
                        }
                    }
                    let mut new = Vec::with_capacity(capacity);
                    new.push(p);
                    ph.push((setting, new));
                }
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug)]
    pub struct Sweep {
        pub config: SweepConfig,
        pub sweep_result: SweepResult,
        pub freqs: Vec<Hertz>,
        pub dactable: Vec<Complex<i16>>,
        pub ddc: Vec<ActualizedDDCChannelConfig>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct PowerSetting {
        pub attens: Attens,
        pub fft_scale: u16,
    }

    #[derive(Debug, Clone)]
    pub struct SweepConfig {
        pub freqs: Vec<Hertz>,
        pub settings: Vec<PowerSetting>,
        pub average: u64,
    }

    impl SweepConfig {
        pub async fn sweep(
            &self,
            capture: &client::Capture,
            tap: client::Tap<'_>,
            if_board: &mut client::IFBoard,
            dsp_scale: &mut client::DSPScale,
            dac_table: &client::DACTable,
            channel: Option<Sender<(Hertz, PowerSetting, SnapAvg)>>,
        ) -> Result<Sweep, Gen3RpcError> {
            let (mut sweep_result, ddc) = match tap {
                client::Tap::RawIQ => (
                    SweepResult::RawIQ(Vec::with_capacity(self.settings.len())),
                    vec![],
                ),
                client::Tap::DDCIQ(t) => {
                    let ts = try_join_all(t.iter().map(|f| f.get())).await?;
                    (
                        SweepResult::DdcIQ(Vec::with_capacity(self.settings.len())),
                        ts,
                    )
                }
                client::Tap::Phase(t) => {
                    let ts = try_join_all(t.iter().map(|f| f.get())).await?;
                    (
                        SweepResult::Phase(Vec::with_capacity(self.settings.len())),
                        ts,
                    )
                }
            };

            let mut freqs = Vec::with_capacity(self.freqs.len());
            for (i, setting) in self.settings.iter().enumerate() {
                let (attens, fft_scale) = try_join(
                    if_board
                        .set_attens(setting.attens)
                        .map_err(Gen3RpcError::from),
                    dsp_scale
                        .set_fft_scale(setting.fft_scale)
                        .map_err(Gen3RpcError::from),
                )
                .await?;
                let true_setting = PowerSetting { attens, fft_scale };

                for freq in self.freqs.iter() {
                    let h = if_board.set_freq(*freq).await?;
                    if i == 0 {
                        freqs.push(h);
                    }
                    let ct = CaptureTap {
                        rfchain: &RFChain {
                            dac_table,
                            if_board,
                            dsp_scale,
                        },
                        tap: tap.clone(),
                    };
                    let a = capture.average(ct, self.average).await?;
                    if let Some(c) = &channel {
                        if c.send((h, true_setting.clone(), a.clone())).is_err() {
                            return Err(Gen3RpcError::Interupted);
                        }
                    }
                    sweep_result.push(a, true_setting.clone(), self.freqs.len());
                }
            }
            assert!(freqs.len() == self.freqs.len());
            Ok(Sweep {
                config: self.clone(),
                sweep_result,
                freqs,
                dactable: dac_table.get_dac_table().await?,
                ddc,
            })
        }
    }
}

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
