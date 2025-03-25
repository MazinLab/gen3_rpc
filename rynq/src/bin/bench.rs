use rynq::udma::Map;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);

    let map = Map::new(&args[1]).unwrap();
    println!("0x{:x} bytes", map.size / 2);

    let mut accumulator: usize = 0;
    let mut total = std::time::Duration::from_secs(0);
    for _ in 0..16usize {
        let start = std::time::Instant::now();
        map.sync_for_cpu().unwrap();
        for bs in map.chunks_exact(2) {
            let bs = [bs[0], bs[1]];
            let acc = u16::from_ne_bytes(bs);
            accumulator = accumulator.wrapping_add(acc as usize);
        }
        let stop = std::time::Instant::now();
        total += stop - start;

        println!("0x{:x} in {:?}", accumulator, stop - start);
    }

    println!("Total time {:?}, got 0x{:x}", total, accumulator);
}
