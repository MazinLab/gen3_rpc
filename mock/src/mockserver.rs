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

struct IFBoardMock {
    lo: Hertz,
    attens: Attens,
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

impl DACTable for DACTableMock {
    fn set(&mut self, v: Box<[Complex<i16>; 524288]>) {
        self.values = v;
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

pub async fn mockserver(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener =
                tokio::net::TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port))
                    .await?;
            let board = Gen3Board::new(
                DACTableMock {
                    values: Box::new([Complex::i(); 524288]),
                },
                IFBoardMock {
                    lo: Hertz::new(6_000_000_000, 1),
                    attens: Attens {
                        input: 20.,
                        output: 20.,
                    },
                },
                DSPScaleMock { fft: 0xfff },
                ChannelAllocator::<(ActualizedDDCChannelConfig, DDCCapabilities), 2048>::new(
                    DDCCapabilities {
                        freq_resolution: Hertz::new(1, 1),
                        freq_bits: 25,
                        rotation_bits: 16,
                        center_bits: 16,
                        bin_control: gen3_rpc::BinControl::FullSwizzle,
                    },
                    (),
                ),
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
