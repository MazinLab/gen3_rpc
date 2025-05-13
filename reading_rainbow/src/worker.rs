use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use gen3_rpc::client::{CaptureTap, RFChain, Tap};
use gen3_rpc::utils::client::{DACCapabilities, PowerSetting, Sweep, SweepConfig};
use gen3_rpc::{
    client::ExclusiveDroppableReference, ActualizedDDCChannelConfig, DDCChannelConfig, Hertz,
};
use gen3_rpc::{gen3rpc_capnp, DDCCapabilities, Snap};
use log::{error, info, warn};
use num::Complex;
use std::ops::Deref;
use std::slice::Iter;
use std::{
    fmt::Display,
    net::ToSocketAddrs,
    sync::{
        mpsc::{Receiver, Sender},
        {Arc, RwLock},
    },
};
use tokio::runtime::Runtime;

/// Define RPC commands
pub enum RPCCommand {
    Exit,
    LoadSetup(BoardSetup),
    SweepConfig(SweepConfig),
    PerformCapture(usize, CaptureType),
}

/// Define RPC responses
pub enum RPCResponse {
    Connected(Arc<RwLock<BoardState>>, DACCapabilities, DDCCapabilities),
    Sweep(Sweep),
    CaptureResult(Snap),
}

pub struct BoardSetup {
    pub lo: Hertz,
    pub power_setting: PowerSetting,
    pub dac_table: Vec<Complex<i16>>,
    pub ddc_config: Vec<DDCChannelConfig>,
}

#[derive(Clone)]
pub struct BoardStateInner {
    pub lo: Hertz,
    pub power_setting: PowerSetting,
    pub dac_table: Vec<Complex<i16>>,
    pub ddc_config: Vec<ActualizedDDCChannelConfig>,
}

#[derive(Clone)]
pub enum BoardState {
    Operating(BoardStateInner),
    Moving,
}

pub enum CaptureType {
    RawIQ,
    DDCIQ,
    Phase,
}

pub fn worker_thread<T: ToSocketAddrs + Display>(
    addr: T,
    command: Receiver<RPCCommand>,
    response: Sender<RPCResponse>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        tokio::task::LocalSet::new()
            .run_until(async move {
                info!("Attempting to connect to server at {}", addr);
                let addr = addr.to_socket_addrs().unwrap().next().unwrap();
                let stream = tokio::net::TcpStream::connect(addr).await?;
                info!("Successfully connected to server");
                stream.set_nodelay(true)?;
                let (reader, writer) =
                    tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network = twoparty::VatNetwork::new(
                    futures::io::BufReader::new(reader),
                    futures::io::BufWriter::new(writer),
                    rpc_twoparty_capnp::Side::Client,
                    capnp::message::ReaderOptions {
                        traversal_limit_in_words: Some(usize::MAX),
                        nesting_limit: i32::MAX,
                    },
                );

                // RPC System initializes communication between us and the board
                let mut rpc_system = RpcSystem::new(Box::new(network), None);

                let board = gen3_rpc::client::Gen3Board {
                    client: rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server),
                };

                tokio::task::spawn_local(rpc_system);

                // Get DSP Scale, DAC Table, IF Board from board
                let mut dsp_scale = board
                    .get_dsp_scale()
                    .await?
                    .try_into_mut()
                    .await?
                    .unwrap_or_else(|_| todo!());
                let mut dac_table = board
                    .get_dac_table()
                    .await?
                    .try_into_mut()
                    .await?
                    .unwrap_or_else(|_| todo!());
                let dac_caps = dac_table.capabilties().await?;
                let mut if_board = board
                    .get_if_board()
                    .await?
                    .try_into_mut()
                    .await?
                    .unwrap_or_else(|_| todo!());
                let capture = board.get_capture().await?;
                let ddc = board.get_ddc().await?;

                let mut ddc_channels: Vec<
                    ExclusiveDroppableReference<gen3rpc_capnp::ddc_channel::Client, ()>,
                > = vec![];

                let mut bsi = BoardStateInner {
                    lo: if_board.get_freq().await?,
                    power_setting: PowerSetting {
                        attens: if_board.get_attens().await?,
                        fft_scale: dsp_scale.get_fft_scale().await?,
                    },
                    dac_table: dac_table.get_dac_table().await?,
                    ddc_config: vec![],
                };
                let board_state = Arc::new(RwLock::new(BoardState::Operating(bsi.clone())));
                response.send(RPCResponse::Connected(board_state.clone(), dac_caps, ddc.capabilities)).unwrap();

                loop {
                    match command.recv().unwrap() {
                        RPCCommand::Exit => {
                            let mut bs = board_state.write().unwrap();
                            *bs = BoardState::Moving;
                            return Ok(());
                        }
                        RPCCommand::LoadSetup(setup) => {
                            info!("Loading Setup");
                            {
                                let mut bs = board_state.write().unwrap();
                                *bs = BoardState::Moving;
                            }
                            if bsi.lo != setup.lo {
                                bsi.lo = if_board.set_freq(setup.lo).await?;
                            }
                            if bsi.power_setting.attens != setup.power_setting.attens {
                                bsi.power_setting.attens = if_board.set_attens(setup.power_setting.attens).await?;
                            }
                            if bsi.power_setting.fft_scale != setup.power_setting.fft_scale {
                                bsi.power_setting.fft_scale = dsp_scale
                                    .set_fft_scale(setup.power_setting.fft_scale)
                                    .await?;
                            }
                            if bsi.dac_table != setup.dac_table {
                                dac_table.set_dac_table(setup.dac_table.clone()).await?;
                                bsi.dac_table = setup.dac_table;
                            }
                            if setup.ddc_config.len() < ddc_channels.len() {
                                ddc_channels.truncate(setup.ddc_config.len());
                                bsi.ddc_config.truncate(setup.ddc_config.len());
                            }
                            for (i, (setup, real)) in setup
                                .ddc_config
                                .iter()
                                .zip(bsi.ddc_config.iter())
                                .enumerate()
                            {
                                if setup.dest_bin.is_some_and(|db| db != real.dest_bin) {
                                    warn!(
                                        "Dest bins don't match in requested and actual setup at {}, reallocating",
                                        i
                                    );
                                    ddc_channels.truncate(i);
                                    bsi.ddc_config.truncate(i);
                                    break;
                                }
                            }
                            for i in 0..(setup.ddc_config.len().min(bsi.ddc_config.len())) {
                                ddc_channels[i].set(setup.ddc_config[i].clone().erase()).await?;
                                bsi.ddc_config[i] = ddc_channels[i].get().await?;
                            }
                            for i in (setup.ddc_config.len().min(bsi.ddc_config.len()))..setup.ddc_config.len() {
                                ddc_channels.push(ddc.allocate_channel(setup.ddc_config[i].clone()).await?.try_into_mut().await.unwrap().unwrap_or_else(|_| todo!()));
                                bsi.ddc_config.push(ddc_channels[i].get().await?);
                            }
                            {
                                let mut bs = board_state.write().unwrap();
                                *bs = BoardState::Operating(bsi.clone())
                            }
                        }
                        RPCCommand::PerformCapture(count, capture_type) => {
                            info!("Performing Capture:");
                            // Perform the capture
                            let rfchain = RFChain {
                                dac_table: &dac_table,
                                if_board: &if_board,
                                dsp_scale: &dsp_scale,
                            };

                            let it = ddc_channels.iter().map(|f| f.deref());
                            let tap = match capture_type {
                                CaptureType::RawIQ => CaptureTap::new(rfchain, Tap::RawIQ),
                                CaptureType::DDCIQ => {
                                    CaptureTap::new(rfchain, Tap::DDCIQ(it))
                                },
                                CaptureType::Phase => {
                                    CaptureTap::new(rfchain, Tap::Phase(it))
                                },
                            };


                            let result = capture.capture(tap, count as u64).await?;
                            response.send(RPCResponse::CaptureResult(result)).unwrap();
                        }
                        // Handle the SweepConfig command
                        RPCCommand::SweepConfig(config) => {
                            info!("Performing Sweep:");

                            let result = config
                                .sweep(
                                    &capture,
                                    Tap::<'_, Iter<_>>::RawIQ,
                                    &mut if_board,
                                    &mut dsp_scale,
                                    &dac_table,
                                    None,
                                )
                                .await;

                            match result {
                                Ok(sweep) => {
                                    info!("Sweep successful");
                                    response.send(RPCResponse::Sweep(sweep)).unwrap();
                                }
                                Err(e) => {
                                    error!("Sweep failed: {:?}", e);
                                }
                            }
                        }
                    }
                }
            })
            .await
    })
}
