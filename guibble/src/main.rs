use std::{
    collections::VecDeque,
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{
        Arc, Mutex,
        mpsc::{Sender, channel},
    },
    time::Duration,
};

use colorous::VIRIDIS;

use egui_plot::{Line, Plot, PlotPoints};
use serde_json::from_str;

use gobbler::{ChunkSample, Gen3Photon, GobbleState, Photon};

struct App {
    history: Arc<Mutex<Vec<VecDeque<Photon<i16>>>>>,
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        ui.ctx()
            .request_repaint_after(Duration::from_secs_f64(1. / 60.));
        egui::CentralPanel::default().show_inside(ui, |ui| {
            let (max, heights) = {
                let rezzies = self.history.lock().unwrap();

                let max = rezzies.iter().map(|a| a.len()).max().unwrap_or(1);
                let heights: Vec<_> = rezzies.iter().map(|a| a.len() as f64).collect();
                (max, heights)
            };
            let plot = Plot::new("map");
            plot.show(ui, |ui| {
                ui.line(Line::new("heights", PlotPoints::from_ys_f64(&heights)));
            })
        });
    }
}

fn chunk_recv(stream: TcpStream, send: Sender<ChunkSample>) {
    let mut stream = BufReader::new(stream);
    let mut s = String::new();
    loop {
        s.clear();
        stream.read_line(&mut s).unwrap();
        if let Ok(js) = from_str(s.trim_end()) {
            send.send(js).unwrap();
        }
    }
}

fn main() {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default(),
        ..Default::default()
    };

    let chunkstream = TcpStream::connect("127.0.0.1:4243").unwrap();
    let datastream = TcpStream::connect("127.0.0.1:4244").unwrap();

    let history = Arc::new(Mutex::new(vec![]));
    let updater = history.clone();

    let (cssend, csrecv) = channel();
    let cr = std::thread::spawn(move || chunk_recv(chunkstream, cssend));
    let dr = std::thread::spawn(move || {
        let mut state: GobbleState<4, _> = GobbleState::new(csrecv.iter());
        let mut buf = BufReader::with_capacity(8192 * 1024, datastream);
        let mut current_cycle: u64 = 0;
        loop {
            let res = Gen3Photon::gobble(&mut buf, state);
            state = res.1;
            {
                let mut up = updater.lock().unwrap();
                for photon in res.0.into_iter() {
                    while photon.bin as usize >= up.len() {
                        core::hint::cold_path();
                        up.push(VecDeque::new());
                    }

                    // TODO: Handle a completely dark array
                    current_cycle = current_cycle.max(photon.cycle);
                    up[photon.bin as usize].push_back(photon);
                }
                for bin in up.iter_mut() {
                    while let Some(p) = bin.front() {
                        if (current_cycle - p.cycle) > 1000 * 1000 {
                            bin.pop_front();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    });

    eframe::run_native(
        "guibble",
        options,
        Box::new(|_| Ok(Box::<App>::new(App { history }))),
    )
    .unwrap();

    cr.join().unwrap();
    dr.join().unwrap();
}
