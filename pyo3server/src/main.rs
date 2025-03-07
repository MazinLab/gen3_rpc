use numpy::{Complex32, ToPyArray};
use pyo3::{
    prelude::*,
    types::{PyDict, PyNone},
};

use gen3_rpc::{
    ActualizedDDCChannelConfig, AttenError, Attens, CaptureError, DDCCapabilities, FrequencyError,
    Hertz, SnapAvg,
    gen3rpc_capnp::{self},
    utils::server::ChannelAllocator,
};

use num::Complex;

use gen3_rpc::server::*;

use std::{
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    ops::Shr,
    sync::{Arc, RwLock},
};

use capnp::capability::Promise;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::{AsyncReadExt, TryFutureExt};

struct DSPScaleMock {
    fft: u16,
}

impl DSPScale for DSPScaleMock {
    fn set(&mut self, v: u16) -> Result<u16, u16> {
        let vp = v & 0xfff;
        self.fft = vp;
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
            let ifb = py.import("mkidgen3.drivers.ifboard").unwrap();
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
        self.lo = v;
        Python::with_gil(|py| {
            let kwargs = PyDict::new(py);
            let frac = self
                .frac_handle
                .call(py, (*v.numer(), *v.denom() * 1000 * 1000), None)
                .unwrap();
            println!("Here");
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
}

impl DACTableImpl {
    fn new(ol: PyObject) -> Self {
        let mut di = Python::with_gil(|py| -> DACTableImpl {
            DACTableImpl {
                dactable: ol.getattr(py, "dactable").unwrap(),
                values: Box::new([Complex::i(); 524288]),
            }
        });
        di.set(di.values.clone());
        di
    }
}

impl DACTable for DACTableImpl {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>) {
        self.values = v;
        let p = Box::new(
            self.values
                .map(|c| Complex32::new(c.re as f32, c.im as f32)),
        );
        Python::with_gil(|py| {
            let pyarr = p.to_pyarray(py);
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

#[derive(Clone)]
struct CaptureImpl;

impl Capture<gen3_rpc::Snap> for CaptureImpl {
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
        let dbp = tap.dest_bins();
        let dbp = dbp.map_ok(move |dbs| {
            Ok(match dbs {
                CaptureTapDestBins::RawIQ => gen3_rpc::Snap::Raw(
                    (0..length)
                        .map(|i| Complex::new((i & 0x7fff) as i16, (i.shr(16) & 0x7fffu64) as i16))
                        .collect(),
                ),
                CaptureTapDestBins::DdcIQ(dbs) => gen3_rpc::Snap::DdcIQ(
                    dbs.into_iter()
                        .map(|db| {
                            (0..length)
                                .map(|i| {
                                    Complex::new(
                                        (db as i16) ^ (i & 0x7fff) as i16,
                                        (db as i16) ^ (i.shr(16) & 0x7fffu64) as i16,
                                    )
                                })
                                .collect()
                        })
                        .collect(),
                ),
                CaptureTapDestBins::Phase(dbs) => gen3_rpc::Snap::Phase(
                    dbs.into_iter()
                        .map(|db| {
                            (0..length)
                                .map(|i| (db as i16) ^ (i & 0x7fff) as i16)
                                .collect()
                        })
                        .collect(),
                ),
            })
            .map(|d| DroppableReferenceImpl {
                state: Arc::new(RwLock::new(DRState::Exclusive)),
                inner: Arc::new(RwLock::new(d)),
                stale: false,
                phantom: PhantomData,
            })
        });
        Promise::from_future(dbp)
    }
    fn average(
        &self,
        tap: CaptureTap,
        _length: u64,
    ) -> Promise<Result<gen3_rpc::SnapAvg, CaptureError>, capnp::Error> {
        let taps = tap.dest_bins();
        let res = taps.map_ok(move |dbs| {
            Ok(match dbs {
                CaptureTapDestBins::RawIQ => SnapAvg::Raw(Complex::new(0.0, 0.0)),
                CaptureTapDestBins::DdcIQ(dbs) => SnapAvg::DdcIQ(
                    dbs.into_iter()
                        .map(|a| Complex::new(a as f64, a as f64))
                        .collect(),
                ),
                CaptureTapDestBins::Phase(dbs) => {
                    SnapAvg::Phase(dbs.into_iter().map(|a| a as f64).collect())
                }
            })
        });
        Promise::from_future(res)
    }
}

pub async fn pyo3server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            // Start the board, clocks and MTS
            let ol = Python::with_gil(|py| -> PyObject {
                // Import MKIDGen3 to register the drivers and start the clocks
                let mkidgen3 = py.import("mkidgen3").unwrap();

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

                let pynq = py.import("pynq").unwrap();
                let overlay = pynq.getattr("Overlay").unwrap();
                let kwargs = PyDict::new(py);
                kwargs.set_item("download", true).unwrap();
                kwargs.set_item("ignore_version", true).unwrap();
                kwargs
                    .set_item("bitfile_name", "/home/xilinx/8tap.bit")
                    .unwrap();
                let ol = overlay.call((), Some(&kwargs)).unwrap();
                mkidgen3
                    .getattr("quirks")
                    .unwrap()
                    .getattr("Overlay")
                    .unwrap()
                    .call1((&ol,))
                    .unwrap()
                    .call_method0("post_configure")
                    .unwrap();
                ol.getattr("rfdc")
                    .unwrap()
                    .call_method0("enable_mts")
                    .unwrap();
                ol.unbind()
            });

            // Open a socket
            let listener =
                tokio::net::TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port))
                    .await?;

            let board = Gen3Board::new(
                DACTableImpl::new(ol),
                IFBoardG2::new(),
                DSPScaleMock { fft: 0xfff },
                ChannelAllocator::<ActualizedDDCChannelConfig, 2048>::new(DDCCapabilities {
                    freq_resolution: Hertz::new(1, 1),
                    freq_bits: 25,
                    rotation_bits: 16,
                    center_bits: 16,
                    bin_control: gen3_rpc::BinControl::FullSwizzle,
                }),
                CaptureImpl,
            );
            let client: gen3rpc_capnp::gen3_board::Client = capnp_rpc::new_client(board);
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pyo3server(4242).await
}
