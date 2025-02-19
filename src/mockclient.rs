use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use gen3_rpc::{client, DDCChannelConfig};
use num_complex::Complex;
use std::net::{Ipv4Addr, SocketAddrV4};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            let stream = tokio::net::TcpStream::connect(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                54321,
            ))
            .await?;
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

            let mut rpc_system = RpcSystem::new(Box::new(network), None);
            let board = client::Gen3Board {
                client: rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server),
            };

            tokio::task::spawn_local(rpc_system);

            let mut dactable = board.get_dac_table().await?;
            let mut dsp_scale = board.get_dsp_scale().await?;
            let ddc = board.get_ddc().await?;
            let capture = board.get_capture().await?;

            let mut d = dactable.get_dac_table().await?;

            println!("Before: {:?}", d[..16].iter().collect::<Vec<_>>());
            d[0].re = 8;
            d[1].im = 32;
            d[2].re = 0x55;
            dactable.set_dac_table(d).await?;

            let p = dactable.get_dac_table().await?;
            println!("After: {:?}", p[..16].iter().collect::<Vec<_>>());

            let scale = dsp_scale.get_fft_scale().await?;
            println!("Starting Scale: {:?}", scale);

            let scale = dsp_scale.set_fft_scale(0xF0F).await;
            println!("Set Valid Scale: {:?}", scale);

            let scale = dsp_scale.set_fft_scale(0xF0F0).await;
            println!("Set Invalid Scale: {:?}", scale);

            let channela = ddc
                .allocate_channel(DDCChannelConfig {
                    source_bin: 0,
                    ddc_freq: 0,
                    dest_bin: None,
                    rotation: 0,
                    center: Complex::new(0, 0),
                })
                .await
                .unwrap();

            let channelb = ddc
                .allocate_channel(DDCChannelConfig {
                    source_bin: 0,
                    ddc_freq: 10,
                    dest_bin: Some(11),
                    rotation: 0,
                    center: Complex::i(),
                })
                .await
                .unwrap();

            let channels = vec![&channela, &channelb];

            let raw = capture
                .capture(client::CaptureTap::RawIQ, 16)
                .await
                .unwrap();
            println!("Raw Snap: {:#?}", raw);

            let phase = capture
                .capture(client::CaptureTap::Phase(channels.clone()), 16)
                .await
                .unwrap();
            println!("Phase Snap: {:#?}", phase);

            let ddciq = capture
                .capture(client::CaptureTap::DDCIQ(channels.clone()), 16)
                .await
                .unwrap();
            println!("DDC Snap: {:#?}", ddciq);

            Ok(())
        })
        .await
}
