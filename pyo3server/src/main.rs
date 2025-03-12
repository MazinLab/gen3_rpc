use env_logger::Env;
use log::{debug, error, info};

use numpy::{Complex32, Ix2, Ix3, PyArrayLike2, PyArrayLike3, ToPyArray};
use pyo3::{
    prelude::*,
    types::{PyDict, PyNone},
};

use gen3_rpc::{
    ActualizedDDCChannelConfig, AttenError, Attens, CaptureError, DDCCapabilities, FrequencyError,
    Hertz, SnapAvg,
    gen3rpc_capnp::{self},
    utils::{little_fixed::*, server::ChannelAllocator},
};

use num::Complex;

use gen3_rpc::server::*;
use tokio::runtime::Runtime;

use std::{
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    ops::Shl,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use capnp::capability::Promise;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::{AsyncReadExt, TryFutureExt};

pub struct MMIO {
    mmio: PyObject,
}

impl MMIO {
    pub fn read(&mut self, address: usize) -> u32 {
        Python::with_gil(|py| -> u32 {
            self.mmio
                .call_method1(py, "read", (address,))
                .unwrap()
                .extract(py)
                .unwrap()
        })
    }

    pub fn write(&mut self, address: usize, data: u32) {
        Python::with_gil(|py| {
            self.mmio
                .call_method1(py, "write", (address, data))
                .unwrap()
        });
    }

    /// # Safety
    /// Address and range must correspond to a pynq mappable range,
    /// in which no reads or writes could cause a bus fault
    pub unsafe fn new(address: usize, range: usize) -> MMIO {
        Python::with_gil(|py| -> MMIO {
            MMIO {
                mmio: py
                    .import("pynq")
                    .unwrap()
                    .getattr("MMIO")
                    .unwrap()
                    .call1((address, range))
                    .unwrap()
                    .unbind(),
            }
        })
    }
}

#[derive(Clone)]
struct TripartiteDDC {
    mmio: Arc<Mutex<MMIO>>,
    inner: (ActualizedDDCChannelConfig, DDCCapabilities),
}

impl TripartiteDDC {
    fn sync(&self) {
        let i =
            LittleFixedDynI32::try_new(self.inner.0.center.re, self.inner.1.center_bits as usize)
                .unwrap();
        let q =
            LittleFixedDynI32::try_new(self.inner.0.center.im, self.inner.1.center_bits as usize)
                .unwrap();
        let freq =
            LittleFixedDynI32::try_new(self.inner.0.ddc_freq, self.inner.1.freq_bits as usize)
                .unwrap();
        let rotation =
            LittleFixedDynI32::try_new(self.inner.0.rotation, self.inner.1.rotation_bits as usize)
                .unwrap();
        let center: i32 = i.mask() | q.mask().shl(16);
        let tone: i32 = freq.mask() | rotation.mask().shl(self.inner.1.freq_bits);

        // Safe bitcast:
        let center: u32 = u32::from_ne_bytes(center.to_ne_bytes());
        let tone: u32 = u32::from_ne_bytes(tone.to_ne_bytes());

        let group_offset = self.get_dest() % 8;
        let group = self.get_dest() / 8;
        let group_address = group * 16 * 4;

        let mut mmio = self.mmio.lock().unwrap();
        mmio.write((group_address + group_offset * 4) as usize, tone);
        mmio.write((group_address + group_offset * 4 + 4 * 8) as usize, center);
    }
}

#[derive(Clone)]
struct PyO3Capture {
    capture: Arc<Mutex<PyObject>>,
}

impl PyO3Capture {
    fn new(ol: &PyObject) -> Self {
        Python::with_gil(|py| -> Self {
            PyO3Capture {
                capture: Arc::new(Mutex::new(ol.getattr(py, "capture").unwrap())),
            }
        })
    }
}

impl Capture<gen3_rpc::Snap> for PyO3Capture {
    fn capture(
        &self,
        tap: CaptureTap,
        length: u64,
    ) -> Promise<
        Result<
            DroppableReferenceImpl<gen3_rpc::Snap, gen3_rpc::gen3rpc_capnp::snap::Client>,
            CaptureError,
        >,
        capnp::Error,
    > {
        let t = tap.dest_bins();
        let capture = self.capture.clone();
        Promise::from_future(t.map_ok(move |db| {
            let snap = match db {
                CaptureTapDestBins::RawIQ => {
                    info!("Capturing {} Raw IQ Samples", length);
                    let cap = capture.lock().unwrap();
                    Python::with_gil(|py| -> Result<gen3_rpc::Snap, PyErr> {
                        let npar = py.import("numpy").unwrap().getattr("array").unwrap();
                        let snap =
                            cap.call_method(py, "capture_adc", (length, false, false, true), None)?;
                        let snaparr: PyArrayLike2<i16> =
                            npar.call1((&snap,)).unwrap().extract().unwrap();
                        let s = gen3_rpc::Snap::Raw(
                            (0..length as usize)
                                .map(|i| {
                                    Complex::new(
                                        *snaparr.get(Ix2(i, 0)).unwrap(),
                                        *snaparr.get(Ix2(i, 1)).unwrap(),
                                    )
                                })
                                .collect(),
                        );
                        snap.call_method0(py, "freebuffer").unwrap();
                        Ok(s)
                    })
                }
                CaptureTapDestBins::DdcIQ(d) => {
                    info!("Capturing {} DDC IQ samples from {} taps", length, d.len());
                    let cap = capture.lock().unwrap();
                    Python::with_gil(|py| -> Result<gen3_rpc::Snap, PyErr> {
                        let npar = py.import("numpy").unwrap().getattr("array").unwrap();
                        let snap = cap.call_method(py, "capture", (length, "ddciq"), None)?;
                        let snaparr: PyArrayLike3<i16> =
                            npar.call1((&snap,)).unwrap().extract().unwrap();
                        let s = gen3_rpc::Snap::DdcIQ(
                            d.into_iter()
                                .map(|d| {
                                    (0..length as usize)
                                        .map(|i| {
                                            Complex::new(
                                                *snaparr.get(Ix3(d as usize, i, 0)).unwrap(),
                                                *snaparr.get(Ix3(d as usize, i, 1)).unwrap(),
                                            )
                                        })
                                        .collect()
                                })
                                .collect(),
                        );
                        snap.call_method0(py, "freebuffer").unwrap();
                        Ok(s)
                    })
                }
                CaptureTapDestBins::Phase(p) => {
                    info!("Capturing {} phase samples from {} taps", length, p.len());
                    let cap = capture.lock().unwrap();
                    Python::with_gil(|py| -> Result<gen3_rpc::Snap, PyErr> {
                        let npar = py.import("numpy").unwrap().getattr("array").unwrap();
                        let snap = cap.call_method(py, "capture", (length, "filtphase"), None)?;
                        let snaparr: PyArrayLike2<i16> =
                            npar.call1((&snap,)).unwrap().extract().unwrap();
                        let s = gen3_rpc::Snap::Phase(
                            p.into_iter()
                                .map(|d| {
                                    (0..length as usize)
                                        .map(|i| *snaparr.get(Ix2(d as usize, i)).unwrap())
                                        .collect()
                                })
                                .collect(),
                        );
                        snap.call_method0(py, "freebuffer").unwrap();
                        Ok(s)
                    })
                }
            };
            let snap = snap.map_err(|_| CaptureError::MemoryUnavailable)?;
            Ok(DroppableReferenceImpl {
                state: Arc::new(RwLock::new(DRState::Exclusive)),
                inner: Arc::new(RwLock::new(snap)),
                stale: false,
                phantom: PhantomData,
            })
        }))
    }
    fn average(
        &self,
        tap: CaptureTap,
        length: u64,
    ) -> Promise<Result<SnapAvg, CaptureError>, capnp::Error> {
        let dr = self.capture(tap, length);
        let avg = dr.map_ok(|res| res.map(|dr| dr.inner.read().unwrap().average()));
        Promise::from_future(avg)
    }
}

impl DDCChannel for TripartiteDDC {
    type Shared = Arc<Mutex<MMIO>>;
    fn from_actualized(
        setup: ActualizedDDCChannelConfig,
        caps: DDCCapabilities,
        shared: Self::Shared,
    ) -> Result<Self, gen3_rpc::ChannelConfigError> {
        assert!(2 * caps.center_bits <= 32);
        assert!(caps.rotation_bits + caps.freq_bits <= 32);
        let mut tp = TripartiteDDC {
            mmio: shared,
            inner: (setup.clone(), caps),
        };
        tp.set(setup.erase())?;
        Ok(tp)
    }
    fn get(&self) -> ActualizedDDCChannelConfig {
        self.inner.get()
    }
    fn set(
        &mut self,
        replacement: gen3_rpc::ErasedDDCChannelConfig,
    ) -> Result<(), gen3_rpc::ChannelConfigError> {
        self.inner.set(replacement)?;
        self.sync();
        Ok(())
    }
    fn set_source(&mut self, source_bin: u32) -> Result<(), gen3_rpc::ChannelConfigError> {
        self.inner.set_source(source_bin)?;
        self.sync();
        Ok(())
    }
    fn set_ddc_freq(&mut self, ddc_freq: i32) -> Result<(), gen3_rpc::ChannelConfigError> {
        self.inner.set_ddc_freq(ddc_freq)?;
        self.sync();
        Ok(())
    }
    fn set_rotation(&mut self, rotation: i32) -> Result<(), gen3_rpc::ChannelConfigError> {
        self.inner.set_rotation(rotation)?;
        self.sync();
        Ok(())
    }
    fn set_center(&mut self, center: Complex<i32>) -> Result<(), gen3_rpc::ChannelConfigError> {
        self.inner.set_center(center)?;
        self.sync();
        Ok(())
    }
    fn get_baseband_freq(&self) -> Hertz {
        self.inner.get_baseband_freq()
    }
    fn get_dest(&self) -> u32 {
        self.inner.get_dest()
    }
}

struct DSPScaleImpl {
    fft: u16,
    fft_gpio: PyObject,
}

impl DSPScaleImpl {
    fn new(ol: &PyObject) -> DSPScaleImpl {
        let mut dsp = Python::with_gil(|py| -> DSPScaleImpl {
            DSPScaleImpl {
                fft: 0xfff,
                fft_gpio: ol
                    .getattr(py, "photon_pipe")
                    .unwrap()
                    .getattr(py, "opfb")
                    .unwrap()
                    .getattr(py, "fft")
                    .unwrap()
                    .getattr(py, "axi_gpio_0")
                    .unwrap()
                    .getattr(py, "channel1")
                    .unwrap(),
            }
        });
        dsp.set(0xfff).unwrap();
        dsp
    }
}

impl DSPScale for DSPScaleImpl {
    fn set(&mut self, v: u16) -> Result<u16, u16> {
        let vp = v & 0xfff;
        self.fft = vp;
        Python::with_gil(|py| {
            self.fft_gpio
                .call_method1(py, "write", (self.fft, 0xfff))
                .unwrap()
        });
        if vp == v { Ok(vp) } else { Err(vp) }
    }

    fn get(&self) -> u16 {
        self.fft
    }
}

struct IFBoardG2 {
    handle: PyObject,
    frac_handle: PyObject,
    lo: Hertz,
    attens: Attens,
}

impl IFBoardG2 {
    fn new() -> IFBoardG2 {
        let mut handle: Option<PyObject> = None;
        let mut frac_handle: Option<PyObject> = None;
        Python::with_gil(|py| {
            let ifb = py.import("mkidgen3.equipment_drivers.ifboard").unwrap();
            let ifb = ifb.getattr("IFBoard").unwrap().call0().unwrap();
            ifb.getattr("power_on").unwrap().call0().unwrap();
            handle = Some(ifb.unbind());

            let fracs = py.import("fractions").unwrap();
            let frac = fracs.getattr("Fraction").unwrap();
            frac_handle = Some(frac.unbind());
        });
        let mut ifb = IFBoardG2 {
            handle: handle.unwrap(),
            frac_handle: frac_handle.unwrap(),
            lo: Hertz::new(0, 1),
            attens: Attens {
                input: 60.,
                output: 60.,
            },
        };
        ifb.set_lo(Hertz::new(6_000_000_000, 1)).unwrap();
        ifb.set_attens(Attens {
            input: 60.0,
            output: 60.0,
        })
        .unwrap();
        ifb
    }
}

impl IFBoard for IFBoardG2 {
    fn set_lo(&mut self, v: Hertz) -> Result<Hertz, FrequencyError> {
        info!("Setting LO to {:?}", v);
        self.lo = v;
        Python::with_gil(|py| {
            let kwargs = PyDict::new(py);
            let frac = self
                .frac_handle
                .call(py, (*v.numer(), *v.denom() * 1000 * 1000), None)
                .unwrap();
            kwargs.set_item("freq", &frac).unwrap();
            let p = self.handle.call_method(py, "set_lo", (), Some(&kwargs));
            p.unwrap();
        });
        Ok(v)
    }
    fn get_lo(&self) -> Hertz {
        self.lo
    }
    fn set_attens(&mut self, a: Attens) -> Result<Attens, AttenError> {
        let a = Attens {
            input: (a.input * 4.).round() / 4.,
            output: (a.output * 4.).round() / 4.,
        };
        if a.input < 0. || a.output < 0. || a.input > 63.5 || a.output > 63.5 {
            error!("Attempted to set unachievable attenuation {:?}", a);
            return Err(AttenError::Unachievable);
        }
        info!("Setting attens to {:?}", a);
        self.attens = a;
        Python::with_gil(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("input_attens", a.input).unwrap();
            kwargs.set_item("output_attens", a.output).unwrap();
            let p = self.handle.call_method(py, "set_attens", (), Some(&kwargs));
            assert!(p.is_ok());
        });
        Ok(a)
    }
    fn get_attens(&self) -> Attens {
        self.attens
    }
}

struct DACTableImpl {
    dactable: PyObject,
    values: Box<[Complex<i16>; 524288]>,
    values_f32: Box<[Complex32; 524288]>,
}

impl DACTableImpl {
    fn new(ol: &PyObject) -> Self {
        let mut di = Python::with_gil(|py| -> DACTableImpl {
            DACTableImpl {
                dactable: ol.getattr(py, "dactable").unwrap(),
                values: Box::new([Complex::i(); 524288]),
                values_f32: Box::new([Complex::i(); 524288]),
            }
        });
        di.set(di.values.clone());
        di
    }
}

impl DACTable for DACTableImpl {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>) {
        info!("Setting DAC Table");

        self.values = v;
        // Can't map cause we need to keep this off the stack
        for (i, v) in self.values.iter().enumerate() {
            self.values_f32[i] = Complex::new(v.re as f32, v.im as f32);
        }
        Python::with_gil(|py| {
            let pyarr = self.values_f32.to_pyarray(py);
            let kwargs = PyDict::new(py);
            kwargs.set_item("data", pyarr).unwrap();
            kwargs.set_item("fpgen", PyNone::get(py)).unwrap();
            self.dactable
                .call_method(py, "replay", (), Some(&kwargs))
                .unwrap();
        });
    }
    fn get(&self) -> Box<[Complex<i16>; 524288]> {
        self.values.clone()
    }
}

pub async fn pyo3server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            // Start the board, clocks and MTS
            let ol = Python::with_gil(|py| -> PyObject {
                // Import MKIDGen3 to register the drivers and start the clocks
                info!("Loading mkidgen3 python library");
                let mkidgen3 = py.import("mkidgen3").unwrap();

                info!("Configuring RFDC Clocking");
                let kwargs = PyDict::new(py);
                kwargs
                    .set_item("programming_key", "4.096GSPS_MTS_direct")
                    .unwrap();
                kwargs.set_item("clock_source", "external").unwrap();
                mkidgen3
                    .getattr("drivers")
                    .unwrap()
                    .getattr("rfdcclock")
                    .unwrap()
                    .getattr("configure")
                    .unwrap()
                    .call((), Some(&kwargs))
                    .unwrap();

                info!("Loading pynq python library");
                let pynq = py.import("pynq").unwrap();
                let overlay = pynq.getattr("Overlay").unwrap();
                let kwargs = PyDict::new(py);
                kwargs.set_item("download", true).unwrap();
                kwargs.set_item("ignore_version", true).unwrap();
                kwargs
                    .set_item("bitfile_name", "/home/xilinx/8tap.bit")
                    .unwrap();
                info!("Loading Bitstream");
                let ol = overlay.call((), Some(&kwargs)).unwrap();

                debug!("Installing Overlay Quirks");
                mkidgen3
                    .getattr("quirks")
                    .unwrap()
                    .getattr("Overlay")
                    .unwrap()
                    .call1((&ol,))
                    .unwrap()
                    .call_method0("post_configure")
                    .unwrap();

                info!("Configuring MTS");
                ol.getattr("rfdc")
                    .unwrap()
                    .call_method0("enable_mts")
                    .unwrap();
                ol.unbind()
            });

            info!("Initilizing DDC Control MMIO");
            let (ddc_addr, ddc_range) = Python::with_gil(|py| -> (usize, usize) {
                let ddcmmio = ol
                    .getattr(py, "photon_pipe")
                    .unwrap()
                    .getattr(py, "reschan")
                    .unwrap()
                    .getattr(py, "ddccontrol_0")
                    .unwrap()
                    .getattr(py, "mmio")
                    .unwrap();
                (
                    ddcmmio
                        .getattr(py, "base_addr")
                        .unwrap()
                        .extract(py)
                        .unwrap(),
                    ddcmmio.getattr(py, "length").unwrap().extract(py).unwrap(),
                )
            });

            let mmio = unsafe { Arc::new(Mutex::new(MMIO::new(ddc_addr, ddc_range))) };

            // Open a socket
            let listener =
                tokio::net::TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port))
                    .await?;

            let board = Gen3Board::new(
                DACTableImpl::new(&ol),
                IFBoardG2::new(),
                DSPScaleImpl::new(&ol),
                ChannelAllocator::<TripartiteDDC, 2048>::new(
                    DDCCapabilities {
                        freq_resolution: Hertz::new(2_000_000, 1 << 11),
                        freq_bits: 11,
                        rotation_bits: 21,
                        center_bits: 16,
                        bin_control: gen3_rpc::BinControl::FullSwizzle,
                    },
                    mmio,
                ),
                PyO3Capture::new(&ol),
            );
            let client: gen3rpc_capnp::gen3_board::Client = capnp_rpc::new_client(board);
            tokio::task::spawn_local(async {
                loop {
                    if Python::with_gil(|py| -> _ { py.check_signals() }).is_err() {
                        error!("Recieved keyboard interrupt, exiting");
                        std::process::exit(-1);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            info!("Waiting For Connections...");
            loop {
                let (stream, _) = listener.accept().await?;
                info!("Recieved Connection From {:?}", stream.peer_addr());
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Launching python interpereter");
    pyo3::prepare_freethreaded_python();
    let rt = Runtime::new()?;
    rt.block_on(async { pyo3server(4242).await })
}
