use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use gen3_rpc::client::Tap;
use gen3_rpc::utils::client::Sweep;
use gen3_rpc::utils::client::SweepConfig;
use gen3_rpc::{client::ExclusiveDroppableReference, Attens, DSPScaleError, Hertz};
use log::{error, info};
use num::Complex;
use std::{
    fmt::Display,
    net::ToSocketAddrs,
    sync::mpsc::{Receiver, Sender},
    time::SystemTime,
};
use tokio::runtime::Runtime;

/// Define RPC commands
pub enum RPCCommand {
    Exit,
    SetFFTScale(u16),
    GetFFTScale,
    GetDACTable,
    SetDACTable(Box<[Complex<i16>; 524288]>),
    GetIFFreq,
    SetIFFreq(Hertz),
    GetIFAttens,
    SetIFAttens(Attens),
    SweepConfig(SweepConfig),
    PerformCapture,
}

/// Define RPC responses
pub enum RPCResponse {
    Connected(SystemTime),
    FFTScale(Option<u16>),
    DACTable(Option<Box<[Complex<i16>; 524288]>>),
    IFFreq(Option<Hertz>),
    IFAttens(Option<Attens>),
    Sweep(Sweep),
    CaptureResult(Vec<Complex<i16>>),
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

                // Send a connected response to the GUI with the current timestamp
                let start_time = SystemTime::now();
                response.send(RPCResponse::Connected(start_time)).unwrap();

                tokio::task::spawn_local(rpc_system);

                // Get DSP Scale, DAC Table, IF Board from board
                let mut dsp_scale: ExclusiveDroppableReference<_, _> = board
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
                let mut if_board = board
                    .get_if_board()
                    .await?
                    .try_into_mut()
                    .await?
                    .unwrap_or_else(|_| todo!());
                let capture = board.get_capture().await?;

                loop {
                    match command.recv().unwrap() {
                        RPCCommand::Exit => return Ok(()),
                        // Handle the SetFFTScale command
                        RPCCommand::SetFFTScale(i) => {
                            info!("Received SetFFTScale command with value: {}", i);
                            let r = dsp_scale.set_fft_scale(i).await;
                            match r {
                                Ok(i) => response.send(RPCResponse::FFTScale(Some(i))).unwrap(),
                                Err(DSPScaleError::Clamped(i)) => {
                                    response.send(RPCResponse::FFTScale(Some(i))).unwrap()
                                }
                                Err(_) => response.send(RPCResponse::FFTScale(None)).unwrap(),
                            }
                        }
                        // Handle the GetFFTScale command
                        RPCCommand::GetFFTScale => {
                            let r = dsp_scale.get_fft_scale().await;
                            response.send(RPCResponse::FFTScale(r.ok())).unwrap()
                        }
                        // Handle the GetDACTable command
                        RPCCommand::GetDACTable => {
                            let r = dac_table.get_dac_table().await;
                            match r {
                                Ok(d) => response.send(RPCResponse::DACTable(Some(d))).unwrap(),
                                Err(e) => {
                                    error!("Failed to get DAC table: {}", e);
                                    response.send(RPCResponse::DACTable(None)).unwrap()
                                }
                            }
                        }
                        // Handle the SetDACTable command
                        RPCCommand::SetDACTable(data) => {
                            let data_clone = data.clone();
                            let r = dac_table.set_dac_table(data).await;
                            match r {
                                Ok(_) => response
                                    .send(RPCResponse::DACTable(Some(data_clone)))
                                    .unwrap(),
                                Err(e) => {
                                    error!("Failed to set DAC table: {}", e);
                                    response.send(RPCResponse::DACTable(None)).unwrap()
                                }
                            }
                        }
                        // Handle the GetIFFreq command
                        RPCCommand::GetIFFreq => {
                            let r = if_board.get_freq().await;
                            response.send(RPCResponse::IFFreq(r.ok())).unwrap()
                        }
                        // Handle the SetIFFreq command
                        RPCCommand::SetIFFreq(freq) => {
                            let r = if_board.set_freq(freq).await;
                            match r {
                                Ok(f) => response.send(RPCResponse::IFFreq(Some(f))).unwrap(),
                                Err(e) => {
                                    error!("Failed to set IF frequency: {:?}", e);
                                    response.send(RPCResponse::IFFreq(None)).unwrap()
                                }
                            }
                        }
                        // Handle the GetIFAttens command
                        RPCCommand::GetIFAttens => {
                            info!("Received GetIFAttens command");
                            let r = if_board.get_attens().await;
                            match r {
                                Ok(a) => response.send(RPCResponse::IFAttens(Some(a))).unwrap(),
                                Err(e) => {
                                    error!("Failed to get IF attenuations: {:?}", e);
                                    response.send(RPCResponse::IFAttens(None)).unwrap()
                                }
                            }
                        }
                        // Handle the SetIFAttens command
                        RPCCommand::SetIFAttens(attens) => {
                            let r = if_board.set_attens(attens).await;
                            match r {
                                Ok(a) => response.send(RPCResponse::IFAttens(Some(a))).unwrap(),
                                Err(e) => {
                                    error!("Failed to set IF attenuations: {:?}", e);
                                    response.send(RPCResponse::IFAttens(None)).unwrap()
                                }
                            }
                        }
                        // Handle the PerformCapture command
                        RPCCommand::PerformCapture => {
                            info!("Performing Capture:");

                            // Perform the capture
                            let rfchain = gen3_rpc::client::RFChain {
                                dac_table: &dac_table,
                                if_board: &if_board,
                                dsp_scale: &dsp_scale,
                            };

                            let tap = gen3_rpc::client::CaptureTap::new(
                                &rfchain,
                                gen3_rpc::client::Tap::RawIQ,
                            );

                            let result = capture.capture(tap, 1).await;

                            match result {
                                Ok(snap) => {
                                    match snap {
                                        gen3_rpc::Snap::Raw(data) => {
                                            info!("Capture successful");
                                            response
                                                .send(RPCResponse::CaptureResult(data))
                                                .unwrap();
                                        }
                                        _ => {
                                            error!("Unexpected Snap type: {:?}", snap);
                                            // Account for improper capture input
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Capture failed: {:?}", e); // Error to prevent gui from crashing
                                    response
                                        .send(RPCResponse::CaptureResult(vec![]))
                                        .unwrap_or_else(|err| {
                                            error!("Failed to send error response: {:?}", err)
                                        }); // Error to prevent gui panic
                                }
                            }
                        }
                        // Handle the SweepConfig command
                        RPCCommand::SweepConfig(config) => {
                            info!("Performing Sweep:");

                            let result = config
                                .sweep(
                                    &capture,
                                    Tap::RawIQ,
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
