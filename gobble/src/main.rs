use clap::{Parser, Subcommand};
use gobbler::{ChunkSample, Gen3Photon, GobbleState};
use npy::NpyData;
use serde_json::{from_reader, to_string};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    DumpDir {
        directory: Option<PathBuf>,
    },
    PlayDir {
        directory: Option<PathBuf>,
        rate: u32,
    },
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    match cli.commands {
        Commands::DumpDir { directory } => {
            let path = directory.unwrap_or(std::env::current_dir().unwrap());
            let (state, data) = load_dir(&path);
            dump(state, data);
        }
        Commands::PlayDir { directory, rate } => {
            assert!(rate > 0);
            let path = directory.unwrap_or(std::env::current_dir().unwrap());
            let cses: Arc<Vec<_>> = Arc::new(
                load_dir(&path)
                    .0
                    .consume()
                    .map(|cs| to_string(&cs).unwrap())
                    .collect(),
            );

            let chunk_listener = TcpListener::bind("127.0.0.1:4243").unwrap();
            let data_listener = TcpListener::bind("127.0.0.1:4244").unwrap();
            let chunk = std::thread::spawn(move || {
                for mut stream in chunk_listener.incoming().flatten() {
                    let tcses = cses.clone();
                    std::thread::spawn(move || {
                        for cs in tcses.iter() {
                            stream.write_all(cs.as_bytes()).unwrap();
                            stream.write_all("\n".as_bytes()).unwrap();
                        }
                    });
                }
            });

            for stream in data_listener.incoming().flatten() {
                let (state, data) = load_dir(&path);
                std::thread::spawn(move || play_bytes(stream, rate, state, data).unwrap());
            }

            chunk.join().unwrap();
        }
    }
    Ok(())
}

fn load_dir<'a>(
    path: &Path,
) -> (
    GobbleState<4, impl Iterator<Item = ChunkSample> + use<'a>>,
    impl Iterator<Item = Vec<u8>> + use<'a>,
) {
    let mut meta = vec![];
    let mut data = vec![];

    for f in fs::read_dir(path).unwrap() {
        let entry = f.unwrap();
        let stem = String::from(entry.path().file_stem().unwrap().to_str().unwrap());
        if entry.path().is_file() && stem.contains("frame") {
            match entry.path().extension().map(|a| a.to_str().unwrap()) {
                Some("json") => {
                    let frame_num: usize = stem.split("frame").last().unwrap().parse().unwrap();
                    meta.push((frame_num, entry.path()));
                }
                Some("npy") => {
                    let frame_num: usize = stem.split("frame").last().unwrap().parse().unwrap();
                    data.push((frame_num, entry.path()));
                }
                _ => {}
            }
        }
    }
    assert_eq!(meta.len(), data.len());

    meta.sort_unstable_by_key(|a| a.0);
    data.sort_unstable_by_key(|a| a.0);
    meta.iter().enumerate().for_each(|f| assert_eq!(f.0, f.1.0));
    data.iter().enumerate().for_each(|f| assert_eq!(f.0, f.1.0));

    let chunksampler = meta.into_iter().flat_map(|(_, path)| {
        from_reader::<_, Vec<ChunkSample>>(fs::File::open(path).unwrap())
            .unwrap()
            .into_iter()
    });

    let state: GobbleState<4, _> = GobbleState::new(chunksampler);
    let data = data.into_iter().map(|(_, path)| {
        let mut v = vec![];
        fs::File::open(path).unwrap().read_to_end(&mut v).unwrap();
        let frame_data: NpyData<u64> = NpyData::from_bytes(&v).unwrap();
        frame_data
            .to_vec()
            .into_iter()
            .flat_map(|f| f.to_le_bytes().into_iter())
            .collect::<Vec<u8>>()
    });
    (state, data)
}

fn dump(
    mut state: GobbleState<4, impl Iterator<Item = ChunkSample>>,
    data: impl Iterator<Item = Vec<u8>>,
) {
    println!("[");
    for (j, frame) in data.enumerate() {
        // Todo Semantics here are not guarenteed
        let mut fake_reader = std::io::Cursor::new(&frame[..]);

        while fake_reader.position() < frame.len() as u64 {
            let res = Gen3Photon::gobble(&mut fake_reader, state);
            state = res.1;
            for (i, photon) in res.0.into_iter().enumerate() {
                if i != 0 || j != 0 {
                    println!(",");
                }
                print!("{}", serde_json::to_string(&photon).unwrap())
            }
        }
    }
    println!("\n]");
}

fn play_bytes(
    mut stream: TcpStream,
    rate: u32,
    mut state: GobbleState<4, impl Iterator<Item = ChunkSample>>,
    data: impl Iterator<Item = Vec<u8>>,
) -> std::io::Result<()> {
    let start_rtime = Instant::now();
    let mut start_ptime: Option<Duration> = None;
    for frame in data {
        let mut fake_reader = std::io::Cursor::new(&frame[..]);
        let res = Gen3Photon::gobble(&mut fake_reader, state);
        state = res.1;

        // Time Init
        if let Some(p) = res.0.first()
            && start_ptime.is_none()
        {
            let secs = (p.cycle * 500) / (1000 * 1000 * 1000);
            let nanos = (p.cycle * 500) % (1000 * 1000 * 1000);
            start_ptime = Some(Duration::new(secs, nanos as u32))
        }

        if let Some(start_ptime) = start_ptime {
            for (chunk, photon) in frame.chunks_exact(8).zip(res.0) {
                let secs = (photon.cycle * 500) / (1000 * 1000 * 1000);
                let nanos = (photon.cycle * 500) % (1000 * 1000 * 1000);
                let ptime = Duration::new(secs, nanos as u32);

                let realtime_duration = Instant::now() - start_rtime;
                let faketime_duration = if start_ptime > ptime {
                    Duration::ZERO
                } else {
                    (ptime - start_ptime) * rate
                };

                if faketime_duration > realtime_duration {
                    std::thread::sleep(faketime_duration - realtime_duration);
                }
                stream.write_all(chunk)?;
                stream.flush()?;
            }
        }
    }
    Ok(())
}
