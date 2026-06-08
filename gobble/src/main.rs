use gobbler::{ChunkSample, Gen3Photon, GobbleState};
// use clap::Parser;
use npy::NpyData;
use serde_json::from_reader;
use std::env::args;
use std::fs;
use std::io::Read;

fn main() {
    let a = args().collect::<Vec<_>>();
    assert_eq!(a.len(), 2);

    let mut meta = vec![];
    let mut data = vec![];

    for f in fs::read_dir(&a[1]).unwrap() {
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

    meta.sort_unstable_by_key(|a| a.0);
    data.sort_unstable_by_key(|a| a.0);

    assert_eq!(meta.len(), data.len());
    meta.iter().enumerate().for_each(|f| assert_eq!(f.0, f.1.0));
    data.iter().enumerate().for_each(|f| assert_eq!(f.0, f.1.0));
    let chunksampler = meta.into_iter().flat_map(|(_, path)| {
        from_reader::<_, Vec<ChunkSample>>(fs::File::open(path).unwrap())
            .unwrap()
            .into_iter()
    });
    let mut state: GobbleState<4, _> = GobbleState::new(chunksampler);
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
    for frame in data {
        let mut fake_reader = std::io::Cursor::new(&frame[..]);
        let res = Gen3Photon::gobble(&mut fake_reader, state);
        state = res.1;
        println!("{:#?}", res.0)
    }
}
