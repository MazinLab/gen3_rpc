use gen3_rpc::{
    gen3rpc_capnp::{self},
    ActualizedDDCChannelConfig, AttenError, Attens, CaptureError, ChannelAllocationError,
    DDCCapabilities, DDCChannelConfig, FrequencyError, Hertz, SnapAvg,
};

use num::Complex;

use gen3_rpc::server::*;

use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    ops::Shr,
    sync::{Arc, Mutex, RwLock},
};

use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};

struct DSPScaleMock {
    fft: u16,
}

impl DSPScale for DSPScaleMock {
    fn set(&mut self, v: u16) -> Result<u16, u16> {
        let vp = v & 0xfff;
        self.fft = vp;
        if vp == v {
            Ok(vp)
        } else {
            Err(vp)
        }
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

struct Gen3BoardImpl {
    ddc: DDCImpl,
    dac_table: DACTableImpl,
    capture: CaptureImpl,
    dsp_scale: DSPScaleImpl,
    if_board: IFBoardImpl,
}

#[derive(Clone)]
struct DDCImpl {
    inner: Arc<Mutex<HashMap<u16, DDCChannelImpl>>>,
}

#[derive(Clone)]
struct CaptureImpl;

type DSPScaleImpl = DroppableReferenceImpl<DSPScaleMock, gen3rpc_capnp::dsp_scale::Client>;
type IFBoardImpl = DroppableReferenceImpl<IFBoardMock, gen3rpc_capnp::if_board::Client>;
type DACTableImpl = DroppableReferenceImpl<DACTableMock, gen3rpc_capnp::dac_table::Client>;
type DDCChannelImpl =
    DroppableReferenceImpl<ActualizedDDCChannelConfig, gen3rpc_capnp::ddc_channel::Client>;

impl DDCImpl {
    fn allocate_channel_inner(
        &self,
        config: DDCChannelConfig,
        exclusive: bool,
        retrieve: bool,
    ) -> Result<
        DroppableReferenceImpl<
            ActualizedDDCChannelConfig,
            gen3_rpc::gen3rpc_capnp::ddc_channel::Client,
        >,
        ChannelAllocationError,
    > {
        let mut l = self.inner.lock().unwrap();
        match config.actualize() {
            Ok(actualized) => {
                let entry = l.entry(actualized.dest_bin as u16);
                match entry {
                    Entry::Occupied(o) => {
                        let mut state = o.get().state.write().unwrap();
                        match *state {
                            DRState::Unshared => {
                                let mut iloc = o.get().inner.write().unwrap();
                                if retrieve && *iloc != actualized {
                                    return Err(ChannelAllocationError::DestinationInUse);
                                }
                                *iloc = actualized;
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
                                return Err(ChannelAllocationError::DestinationInUse)
                            }
                            DRState::Shared(i) => {
                                if exclusive {
                                    return Err(ChannelAllocationError::DestinationInUse);
                                }
                                let iloc = o.get().inner.write().unwrap();
                                if *iloc == actualized {
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
                            inner: Arc::new(RwLock::new(actualized)),
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
                                if *iloc == erased.with_dest(i as u32) {
                                    *slock = DRState::Shared(i + 1);
                                    return Ok(ent.clone_weak());
                                }
                            }
                            DRState::Unshared => {
                                let mut iloc = ent.inner.write().unwrap();
                                if retrieve && *iloc != erased.with_dest(i as u32) {
                                    continue;
                                }
                                if exclusive {
                                    *slock = DRState::Exclusive;
                                } else {
                                    *slock = DRState::Shared(1);
                                }
                                *iloc = erased.with_dest(i as u32);
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
                            inner: Arc::new(RwLock::new(erased.with_dest(i as u32))),
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

impl DDC<ActualizedDDCChannelConfig> for DDCImpl {
    fn capabilities(&self) -> gen3_rpc::DDCCapabilities {
        DDCCapabilities {
            freq_resolution: Hertz::new(1, 1),
            freq_bits: 24,
            rotation_bits: 16,
            center_bits: 16,
            bin_control: gen3_rpc::BinControl::FullSwizzle,
        }
    }

    fn allocate_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        DroppableReferenceImpl<
            ActualizedDDCChannelConfig,
            gen3_rpc::gen3rpc_capnp::ddc_channel::Client,
        >,
        ChannelAllocationError,
    > {
        self.allocate_channel_inner(config, false, false)
    }

    fn allocate_channel_mut(
        &self,
        config: DDCChannelConfig,
    ) -> Result<
        DroppableReferenceImpl<
            ActualizedDDCChannelConfig,
            gen3_rpc::gen3rpc_capnp::ddc_channel::Client,
        >,
        ChannelAllocationError,
    > {
        self.allocate_channel_inner(config, true, false)
    }

    fn retrieve_channel(
        &self,
        config: DDCChannelConfig,
    ) -> Option<
        DroppableReferenceImpl<
            ActualizedDDCChannelConfig,
            gen3_rpc::gen3rpc_capnp::ddc_channel::Client,
        >,
    > {
        self.allocate_channel_inner(config, false, true).ok()
    }
}

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

impl gen3rpc_capnp::gen3_board::Server for Gen3BoardImpl {
    fn get_ddc(
        &mut self,
        _: gen3rpc_capnp::gen3_board::GetDdcParams,
        mut results: gen3rpc_capnp::gen3_board::GetDdcResults,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .set_ddc(capnp_rpc::new_client(PhantomServer::new(self.ddc.clone())));
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
            .set_capture(capnp_rpc::new_client(PhantomServer::new(
                self.capture.clone(),
            )));
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
                    inner: Arc::new(RwLock::new(DACTableMock {
                        values: Box::new([Complex::i(); 524288]),
                    })),
                    stale: false,
                    phantom: PhantomData,
                },
                capture: CaptureImpl,
                dsp_scale: DSPScaleImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(DSPScaleMock { fft: 0 })),
                    stale: false,
                    phantom: PhantomData,
                },
                if_board: IFBoardImpl {
                    state: Arc::new(RwLock::new(DRState::Unshared)),
                    inner: Arc::new(RwLock::new(IFBoardMock {
                        lo: Hertz::new(6_000_000_000, 1),
                        attens: Attens {
                            input: 10.,
                            output: 20.,
                        },
                    })),
                    stale: false,
                    phantom: PhantomData,
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
