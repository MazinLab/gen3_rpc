use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::{AsyncReadExt, future::try_join_all};
use gen3_rpc::{
    Attens, DDCChannelConfig, Hertz,
    client::{self, CaptureTap, RFChain, Tap},
    utils::client::{PowerSetting, Sweep},
};

use gen3_rpc::utils::client::SweepConfig;
use num_complex::Complex;
use std::net::{Ipv4Addr, SocketAddrV4};

pub async fn mockclient(address: Ipv4Addr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::LocalSet::new()
        .run_until(async move {
            let stream = tokio::net::TcpStream::connect(SocketAddrV4::new(address, port)).await?;
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

            let dactable = board.get_dac_table().await?;
            let mut dsp_scale = board
                .get_dsp_scale()
                .await?
                .try_into_mut()
                .await?
                .unwrap_or_else(|_| todo!());
            let ddc = board.get_ddc().await?;
            let capture = board.get_capture().await?;
            let mut ifboard = board
                .get_if_board()
                .await?
                .try_into_mut()
                .await?
                .unwrap_or_else(|_| todo!());

            println!(
                "Setting IFBoard Freq: {:#?}",
                ifboard.set_freq(Hertz::new(6_000_000_000, 1)).await
            );

            println!(
                "Setting IFBoard Atten: {:#?}",
                ifboard
                    .set_attens(Attens {
                        input: 61.,
                        output: 61.,
                    })
                    .await
            );

            let mut d = dactable.get_dac_table().await?;

            println!("DAC Table Before: {:?}", d[..16].iter().collect::<Vec<_>>());
            d[0].re = 8;
            d[1].im = 32;
            d[2].re = 0x55;
            let mut dactable = dactable.try_into_mut().await?.unwrap_or_else(|_| todo!());
            dactable.set_dac_table(d).await.unwrap();

            let p = dactable.get_dac_table().await?;
            println!("DAC Table After: {:?}", p[..16].iter().collect::<Vec<_>>());

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

            let channelb = if let Ok(b) = ddc
                .allocate_channel(DDCChannelConfig {
                    source_bin: 0,
                    ddc_freq: 10,
                    dest_bin: Some(11),
                    rotation: 0,
                    center: Complex::i(),
                })
                .await
            {
                b
            } else {
                println!("Allocating DDC Channel with specific bin 11 failed, probably already in use, allocating without specified bin");
                ddc.allocate_channel(DDCChannelConfig {
                    source_bin: 0,
                    ddc_freq: 10,
                    dest_bin: None,
                    rotation: 0,
                    center: Complex::i(),
                })
                .await
                .unwrap()
            };

            let channels = vec![&channela, &channelb];

            let rfchain = RFChain {
                dac_table: &dactable,
                if_board: &ifboard,
                dsp_scale: &dsp_scale,
            };

            let raw = capture
                .capture(CaptureTap::new(&rfchain, Tap::RawIQ), 16)
                .await
                .unwrap();
            println!("Raw Snap: {:#?}", raw);

            let phase = capture
                .capture(CaptureTap::new(&rfchain, Tap::Phase(&channels)), 16)
                .await
                .unwrap();
            println!("Phase Snap: {:#?}", phase);

            let ddciq = capture
                .capture(CaptureTap::new(&rfchain, Tap::DDCIQ(&channels)), 16)
                .await
                .unwrap();
            println!("DDC Snap: {:#?}", ddciq);

            let chans_256 = try_join_all((0..256).map(|i| {
                ddc.allocate_channel(DDCChannelConfig {
                    source_bin: 0,
                    ddc_freq: 0,
                    dest_bin: None,
                    rotation: 0,
                    center: Complex::new(i * 32, i * 32),
                })
            }))
            .await
            .unwrap();

        let config = SweepConfig {
            freqs: vec![Hertz::new(6_000_000_000, 1), Hertz::new(6_020_000_000, 1)],
            settings: vec![PowerSetting {
                attens: Attens { input: 60., output: 60. },
                fft_scale: 0xfff,
            }],
            average: 8192,
        };

        config.sweep(&capture,  Tap::DDCIQ(&chans_256.iter().collect::<Vec<_>>()), &mut ifboard, &mut dsp_scale, &dactable, None).await.unwrap();

            Ok(())
        })
        .await
}
