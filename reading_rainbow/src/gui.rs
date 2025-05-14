// Nikki Zivkov 02/06/2025
// This script generates a template for the gui
// Will be filled in later with actual gui elements
// Called to in main

// Importing crates/modules
use crate::worker::{worker_thread, BoardSetup, BoardState, CaptureType, RPCCommand, RPCResponse};

use eframe::{egui, App, CreationContext, NativeOptions};
use egui_plot::{AxisHints, HPlacement, Line, Plot, PlotPoints, Points};
use egui_tiles::Tile;
use gen3_rpc::{
    utils::client::{
        DACBuilder, DACCapabilities, ExactTone, ImpreciseTone, PowerSetting, Quantizable, Sweep,
        SweepResult, FFT_AGC_OPTIONS,
    },
    Attens, DDCCapabilities, DDCChannelConfig, Hertz, Snap, SnapAvg,
};
use num::Complex;
use num_complex::ComplexFloat;

use std::{
    collections::HashMap,
    io::Write,
    net::ToSocketAddrs,
    sync::{
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc, RwLock,
    },
    thread::{spawn, JoinHandle},
};

use log::{error, info};

pub struct BoardConnection {
    command: Sender<RPCCommand>,
    response: Receiver<RPCResponse>,
    state: Arc<RwLock<BoardState>>,
    worker_thread: JoinHandle<()>,
    connection_id: String,
    dac_capabilities: DACCapabilities,
    ddc_capabilities: DDCCapabilities,
}

#[derive(Default)]
struct BoardConnectionUIData {
    latest_capture: Option<Snap>,
    latest_sweep: Option<Sweep>,
    waiting_capture: bool,
    waiting_sweep: bool,
    capture_count: usize,
    setup: SetupUIData,
    snapdata: SnapUIData,
    sweepuidata: SweepUIData,
    sweepsetup: SweepSetupUIData,
}

#[derive(Default)]
struct SweepUIData {
    reciever: Option<Receiver<(Hertz, PowerSetting, SnapAvg)>>,
    current_sweep: Vec<(PowerSetting, Vec<(Hertz, SnapAvg)>)>,
    tones: Vec<Hertz>,
}

#[derive(Default, Clone)]
pub struct SweepSetupUIData {
    pub output_min: f64,
    pub output_max: f64,
    pub output_step: f64,
    pub bandwidth: f64,
    pub count: usize,
}

struct SetupUIData {
    output_atten: f64,
    input_atten: f64,
    fft_scale: u16,
    agc: bool,
    start: f64,
    stop: f64,
    count: usize,
    dynamic_range: f64,
    db: DACBuilder,
}

impl Default for SetupUIData {
    fn default() -> Self {
        SetupUIData {
            output_atten: 63.5,
            input_atten: 63.5,
            fft_scale: 0xfff,
            agc: true,
            start: -128.,
            stop: 128.,
            count: 128,
            dynamic_range: 0.75,
            db: DACBuilder::new(DACCapabilities {
                bw: Hertz::new(4_096_000_000, 1),
                length: 1 << 19,
            }),
        }
    }
}

trait UIAble {
    type UIData;
    fn ui(&mut self, data: &mut Self::UIData, ui: &mut egui::Ui);
}

impl UIAble for Sweep {
    type UIData = SweepUIData;
    fn ui(&mut self, _data: &mut Self::UIData, ui: &mut egui::Ui) {
        Plot::new("Sweep").show(ui, |ui| {});
    }
}

#[derive(Default)]
struct SnapUIData {
    ddc_channel: usize,
    show_phase: bool,
    fft: bool,
}

impl UIAble for Snap {
    type UIData = SnapUIData;
    fn ui(&mut self, data: &mut Self::UIData, ui: &mut egui::Ui) {
        match self {
            Self::Raw(iq) => {
                // if ui.button("Save").clicked() {
                //     let mut w = vec![];
                //     writeln!(&mut w, "snap = [").unwrap();
                //     for c in iq.clone().iter() {
                //         writeln!(&mut w, "({}) + 1.j * ({}),", c.re, c.im).unwrap();
                //     }
                //     writeln!(&mut w, "]").unwrap();
                //     std::fs::File::create("/tmp/snap.py")
                //         .unwrap()
                //         .write_all(&w)
                //         .unwrap();
                // }
                let real_points: PlotPoints =
                    (0..iq.len()).map(|i| [i as f64, iq[i].re as f64]).collect();
                let imag_points: PlotPoints =
                    (0..iq.len()).map(|i| [i as f64, iq[i].im as f64]).collect();

                let real = Line::new("real", real_points);
                let imag = Line::new("imag", imag_points);
                Plot::new(format!("RawIQ{}", iq.len()))
                    .width(ui.available_width())
                    .height(ui.available_width() / 2.)
                    .show(ui, |ui| {
                        ui.line(real);
                        ui.line(imag);
                    });
            }
            Self::DdcIQ(iqs) => {
                ui.horizontal(|ui| {
                    ui.add(
                        egui::Slider::new(&mut data.ddc_channel, 0..=(iqs.len() - 1))
                            .text("Channel"),
                    );
                    ui.checkbox(&mut data.show_phase, "Computed Phase");
                    ui.checkbox(&mut data.fft, "FFT")
                });
                let real_points: PlotPoints = (0..iqs[data.ddc_channel].len())
                    .map(|i| [i as f64, iqs[data.ddc_channel][i].re as f64])
                    .collect();
                let imag_points: PlotPoints = (0..iqs[data.ddc_channel].len())
                    .map(|i| [i as f64, iqs[data.ddc_channel][i].im as f64])
                    .collect();

                let range = iqs[data.ddc_channel]
                    .iter()
                    .map(|c| c.re.abs().max(c.im.abs()).max(1))
                    .max()
                    .unwrap_or(1) as f64;

                let real = Line::new("real", real_points);
                let imag = Line::new("imag", imag_points);
                Plot::new(format!(
                    "DDCIQ{}-{}",
                    iqs.len(),
                    iqs[data.ddc_channel].len()
                ))
                .width(ui.available_width())
                .height(ui.available_width() / 2.)
                .default_y_bounds(-range * 1.1, range * 1.1)
                .custom_y_axes(if data.show_phase {
                    vec![
                        AxisHints::new_y(),
                        AxisHints::new_y()
                            .label("Degrees")
                            .placement(HPlacement::Right)
                            .formatter(|a, _b| format!("{:3.0}", a.value * 180. / range)),
                    ]
                } else {
                    vec![AxisHints::new_y()]
                })
                .show(ui, |ui| {
                    ui.line(real);
                    ui.line(imag);

                    if data.show_phase {
                        let phase_points: PlotPoints = (0..iqs[data.ddc_channel].len())
                            .map(|i| {
                                let c = iqs[data.ddc_channel][i];
                                let c = Complex::new(c.re as f64, c.im as f64);
                                [i as f64, c.to_polar().1 * range / (std::f64::consts::PI)]
                            })
                            .collect();
                        let phase = Line::new("phase", phase_points);
                        ui.line(phase);
                    }
                });
            }
            Self::Phase(ps) => {
                ui.add(
                    egui::Slider::new(&mut data.ddc_channel, 0..=(ps.len() - 1)).text("Channel"),
                );
                let phase_points: PlotPoints = (0..ps[data.ddc_channel].len())
                    .map(|i| [i as f64, ps[data.ddc_channel][i] as f64])
                    .collect();
                let phase = Line::new("phase", phase_points);
                Plot::new(format!("Phase{}-{}", ps.len(), ps[data.ddc_channel].len()))
                    .width(ui.available_width())
                    .height(ui.available_width() / 2.)
                    .show(ui, |ui| {
                        ui.line(phase);
                    });
            }
        }
    }
}

impl BoardConnection {
    fn snap_callback(&mut self, data: &mut BoardConnectionUIData, snap: Snap) -> Option<Pane> {
        data.latest_capture = Some(snap);
        data.waiting_capture = false;
        None
    }

    fn sweep_callback(&mut self, data: &mut BoardConnectionUIData, sweep: Sweep) {
        data.latest_sweep = Some(sweep);
        data.waiting_sweep = false;
    }

    fn setup_callback(&mut self, _data: &mut BoardConnectionUIData) {}
}

impl UIAble for BoardConnection {
    type UIData = BoardConnectionUIData;

    fn ui(&mut self, data: &mut Self::UIData, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            ui.vertical(|ui| {
                // Compiler should optimize out this clone :fingers_crossed:
                let s = self.state.read().unwrap().clone();
                ui.set_width(ui.available_width() / 4.);
                // ui.set_height(ui.available_height());
                match s {
                    BoardState::Moving => {
                        ui.heading("Board Moving");
                        ui.separator();
                    }
                    BoardState::Operating(bsi) => {
                        ui.heading("Board Operating");
                        ui.separator();
                        egui::Grid::new("board_status")
                            .num_columns(2)
                            .striped(true)
                            .spacing([40., 2.])
                            .show(ui, |ui| {
                                ui.label("LO Freq");
                                let re = ui.label(format!(
                                    "{:.5} MHz",
                                    (*bsi.lo.numer() as f64)
                                        / (*bsi.lo.denom() as f64 * 1000. * 1000.)
                                ));
                                re.on_hover_text(format!(
                                    "{} Hz / {}",
                                    bsi.lo.numer(),
                                    bsi.lo.denom()
                                ));
                                ui.end_row();

                                ui.label("Input Atten");
                                ui.label(format!("{:.2} dB", bsi.power_setting.attens.input));
                                ui.end_row();

                                ui.label("Output Atten");
                                ui.label(format!("{:.2} dB", bsi.power_setting.attens.output));
                                ui.end_row();

                                ui.label("FFT Scale");
                                ui.label(format!(
                                    "/2^{} ({:3x})",
                                    bsi.power_setting.fft_scale.count_ones(),
                                    bsi.power_setting.fft_scale,
                                ));
                            });
                    }
                }
            });
            ui.separator();
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.set_width(ui.available_width());
                // ui.set_height(ui.available_height());
                ui.collapsing("Capture", |ui| {
                    ui.horizontal(|ui| {
                        ui.add(egui::DragValue::new(&mut data.capture_count).speed(64));
                        if ui.button("Raw IQ").clicked() {
                            self.command
                                .send(RPCCommand::PerformCapture(
                                    data.capture_count,
                                    CaptureType::RawIQ,
                                ))
                                .unwrap();
                            data.waiting_capture = true;
                        }
                        ui.add_enabled_ui(
                            if let BoardState::Operating(bsi) = self.state.read().unwrap().clone() {
                                !bsi.ddc_config.is_empty()
                            } else {
                                false
                            },
                            |ui| {
                                if ui.button("DDC IQ").clicked() {
                                    self.command
                                        .send(RPCCommand::PerformCapture(
                                            data.capture_count,
                                            CaptureType::DDCIQ,
                                        ))
                                        .unwrap();
                                    data.waiting_capture = true;
                                }
                                if ui.button("Phase").clicked() {
                                    self.command
                                        .send(RPCCommand::PerformCapture(
                                            data.capture_count,
                                            CaptureType::Phase,
                                        ))
                                        .unwrap();
                                    data.waiting_capture = true;
                                }
                            },
                        );
                        if data.waiting_capture {
                            ui.spinner();
                        }
                    });
                    if let Some(snap) = &mut data.latest_capture {
                        snap.ui(&mut data.snapdata, ui);
                    }
                });
                ui.collapsing("Sweep", |ui| {
                    ui.horizontal(|ui| {
                        ui.vertical(|ui| {
                            ui.horizontal(|ui| {
                                if ui.button("Run").clicked() {
                                    let (send, recv) = channel();
                                    self.command
                                        .send(RPCCommand::SweepConfig(
                                            data.sweepsetup.clone(),
                                            send,
                                        ))
                                        .unwrap();

                                    data.sweepuidata.reciever = Some(recv);
                                    data.sweepuidata.current_sweep = vec![];
                                }
                                ui.add_enabled_ui(data.sweepuidata.reciever.is_some(), |ui| {
                                    if ui.button("Cancel").clicked() {
                                        data.sweepuidata.reciever = None;
                                    }
                                });
                            });
                            let mut canceled = false;
                            data.sweepuidata.reciever.iter_mut().for_each(|recv| {
                                while match recv.try_recv() {
                                    Ok((f, p, a)) => {
                                        let mut index = data.sweepuidata.current_sweep.len();
                                        for (i, s) in
                                            data.sweepuidata.current_sweep.iter().enumerate().rev()
                                        {
                                            if s.0 == p {
                                                index = i;
                                                break;
                                            }
                                        }
                                        if index == data.sweepuidata.current_sweep.len() {
                                            data.sweepuidata.current_sweep.push((p, vec![]));
                                        }
                                        data.sweepuidata.current_sweep[index].1.push((f, a));
                                        data.sweepuidata.current_sweep[index]
                                            .1
                                            .sort_by(|a, b| a.0.cmp(&b.0));
                                        true
                                    }
                                    Err(TryRecvError::Disconnected) => {
                                        canceled = true;
                                        false
                                    }
                                    _ => false,
                                } {}
                            });
                            if canceled {
                                data.sweepuidata.reciever = None;
                            }
                            egui::Grid::new("ssettings")
                                .num_columns(2)
                                .spacing([40., 2.])
                                .striped(true)
                                .show(ui, |ui| {
                                    ui.label("Bandwidth (MHz)");
                                    ui.add(
                                        egui::DragValue::new(&mut data.sweepsetup.bandwidth)
                                            .range(7812.5 / 1e6..=512.),
                                    );
                                    ui.end_row();

                                    ui.label("Frequency Count");
                                    ui.add(
                                        egui::DragValue::new(&mut data.sweepsetup.count)
                                            .range(1..=32768),
                                    );

                                    ui.end_row();

                                    ui.label("Frequency Step");
                                    ui.label(format!(
                                        "{} Hz",
                                        data.sweepsetup.bandwidth * 1e6
                                            / data.sweepsetup.count as f64
                                    ));
                                    ui.end_row();

                                    ui.label("Output Atten Start (dB)");
                                    ui.add(
                                        egui::DragValue::new(&mut data.sweepsetup.output_min)
                                            .range(0f64..=data.sweepsetup.output_max),
                                    );
                                    ui.end_row();

                                    ui.label("Output Atten Stop (dB)");
                                    ui.add(
                                        egui::DragValue::new(&mut data.sweepsetup.output_max)
                                            .range(data.sweepsetup.output_min..=63.25),
                                    );
                                    ui.end_row();

                                    ui.label("Output Atten Step (dB)");
                                    ui.add(
                                        egui::DragValue::new(&mut data.sweepsetup.output_step)
                                            .range(0.25..=63.25),
                                    );
                                    ui.end_row();
                                });
                        });
                        ui.separator();
                        ui.vertical(|ui| {
                            // data.latest_sweep
                            //     .iter_mut()
                            //     .for_each(|f| f.ui(&mut data.sweepuidata, ui));
                            ui.horizontal(|ui| {
                                Plot::new(format!(
                                    "SweepWatchLoop{}",
                                    data.sweepuidata.current_sweep.len()
                                ))
                                .width(ui.available_width() / 2.)
                                .data_aspect(1.0)
                                .view_aspect(1.0)
                                .show(ui, |ui| {
                                    for (_p, v) in data.sweepuidata.current_sweep.iter() {
                                        for (f, s) in v.iter() {
                                            match s {
                                                SnapAvg::Raw(c) => {
                                                    ui.points(Points::new(
                                                        format!("{}", f),
                                                        vec![[c.re, c.im]],
                                                    ));
                                                }
                                                SnapAvg::DdcIQ(vc) => {
                                                    for (i, c) in vc.iter().enumerate() {
                                                        ui.points(Points::new(
                                                            format!("{} {}", f, i),
                                                            vec![[c.re, c.im]],
                                                        ));
                                                    }
                                                }
                                                SnapAvg::Phase(_) => {}
                                            }
                                        }
                                    }
                                });
                                Plot::new(format!(
                                    "SweepWatch{}",
                                    data.sweepuidata.current_sweep.len()
                                ))
                                .height(ui.available_width())
                                .view_aspect(1.)
                                .show(ui, |ui| {
                                    for (_p, v) in data.sweepuidata.current_sweep.iter() {
                                        for (f, s) in v.iter() {
                                            match s {
                                                SnapAvg::Raw(c) => {
                                                    let f = 1e-6 * *f.numer() as f64
                                                        / *f.denom() as f64;
                                                    ui.points(Points::new(
                                                        format!("{}", f),
                                                        vec![[f, c.abs()]],
                                                    ));
                                                }
                                                SnapAvg::DdcIQ(vc) => {
                                                    assert!(
                                                        vc.len() == data.sweepuidata.tones.len()
                                                    );
                                                    for (i, c) in vc.iter().enumerate() {
                                                        let f = *f + data.sweepuidata.tones[i];
                                                        let f =
                                                            *f.numer() as f64 / *f.denom() as f64;
                                                        let f = f / 1e6;
                                                        ui.points(Points::new(
                                                            format!("{} {}", f, i),
                                                            vec![[f, c.abs()]],
                                                        ));
                                                    }
                                                }
                                                SnapAvg::Phase(_) => {}
                                            }
                                        }
                                    }
                                });
                            });
                        });
                    });
                });
                ui.collapsing("Setup", |ui| {
                    if ui.button("Load").clicked() {
                        let ddc_config = data
                            .setup
                            .db
                            .tones
                            .clone()
                            .into_iter()
                            .map(|t| -> DDCChannelConfig {
                                //TODO: Use to_central
                                let exact = t.to_exact(&self.dac_capabilities);
                                match exact {
                                    ExactTone::Single {
                                        freq,
                                        amplitude: _,
                                        phase: _,
                                    } => {
                                        let (bin, freq) = self.ddc_capabilities.ddc_freq(freq);
                                        DDCChannelConfig {
                                            source_bin: bin,
                                            ddc_freq: freq,
                                            dest_bin: None,
                                            rotation: 0,
                                            center: Complex::new(0, 0),
                                        }
                                    }
                                }
                            })
                            .collect();
                        data.sweepuidata.tones = data
                            .setup
                            .db
                            .tones
                            .clone()
                            .into_iter()
                            .map(|t| t.to_exact(&self.dac_capabilities).central())
                            .collect();
                        println!("{:?}", data.sweepuidata.tones);
                        self.command
                            .send(RPCCommand::LoadSetup(BoardSetup {
                                lo: Hertz::new(6_000_000_000, 1),
                                power_setting: PowerSetting {
                                    attens: Attens {
                                        input: data.setup.input_atten as f32,
                                        output: data.setup.output_atten as f32,
                                    },
                                    fft_scale: data.setup.fft_scale,
                                },
                                dac_table: data
                                    .setup
                                    .db
                                    .build_dynamic_range(data.setup.dynamic_range)
                                    .1,
                                ddc_config,
                            }))
                            .unwrap();
                    }

                    ui.heading("DAC Table");
                    let mut rebuild = data.setup.db.tones.len() != data.setup.count;
                    egui::Grid::new("dsettings")
                        .num_columns(2)
                        .spacing([40., 2.])
                        .striped(true)
                        .show(ui, |ui| {
                            ui.label("Start");
                            rebuild |= ui
                                .add(
                                    egui::DragValue::new(&mut data.setup.start)
                                        .range(-2048f64..=data.setup.stop),
                                )
                                .changed();
                            ui.end_row();

                            ui.label("Stop");
                            rebuild |= ui
                                .add(
                                    egui::DragValue::new(&mut data.setup.stop)
                                        .range(data.setup.start..=2048f64),
                                )
                                .changed();
                            ui.end_row();

                            ui.label("Count");
                            rebuild |= ui
                                .add(egui::DragValue::new(&mut data.setup.count).range(2..=2048))
                                .changed();
                        });
                    if rebuild {
                        data.setup.db = DACBuilder::new(self.dac_capabilities)
                            .add_tones((0..data.setup.count).map(|i| {
                                let freq = (data.setup.start
                                    + (i as f64) * (data.setup.stop - data.setup.start)
                                        / (data.setup.count as f64))
                                    * 1e6;
                                let t = ImpreciseTone::Single {
                                    freq,
                                    amplitude: 1.0,
                                    phase: 0.0,
                                };
                                t.quantize(&data.setup.db.capabilities).unwrap()
                            }))
                            .randomize_phases();
                    }
                    ui.separator();
                    ui.heading("Power Settings");
                    egui::Grid::new("psettings")
                        .num_columns(2)
                        .spacing([40., 2.])
                        .striped(true)
                        .show(ui, |ui| {
                            ui.label("Output Atten");
                            ui.horizontal(|ui| {
                                ui.add(
                                    egui::DragValue::new(&mut data.setup.output_atten)
                                        .range(0f64..=63.5)
                                        .speed(0.25)
                                        .min_decimals(2)
                                        .max_decimals(2),
                                );
                                ui.checkbox(&mut data.setup.agc, "AGC");
                            });
                            ui.end_row();
                            if data.setup.agc {
                                ui.disable();
                            }

                            ui.label("FFT Scale");
                            egui::ComboBox::new("fftscale", "")
                                .selected_text(format!(
                                    "/2^{} ({:3x})",
                                    data.setup.fft_scale.count_ones(),
                                    data.setup.fft_scale
                                ))
                                .show_ui(ui, |ui| {
                                    for agc in FFT_AGC_OPTIONS {
                                        ui.selectable_value(
                                            &mut data.setup.fft_scale,
                                            agc,
                                            format!("/2^{} ({:3x})", agc.count_ones(), agc),
                                        );
                                    }
                                });
                            ui.end_row();

                            ui.label("Input Atten");
                            ui.add(
                                egui::DragValue::new(&mut data.setup.input_atten)
                                    .range(0f64..=63.5)
                                    .speed(0.25)
                                    .min_decimals(2)
                                    .max_decimals(2),
                            );
                        });
                });
            });
        });
    }
}

enum Pane {
    Board(BoardConnection, BoardConnectionUIData),
    SweepSetup(Sweep, ()),
}

type PendingConnection = (
    Sender<RPCCommand>,
    Receiver<RPCResponse>,
    JoinHandle<()>,
    String,
);

// Defining structs
pub struct ReadingRainbow {
    connection_string: String,
    pending_connections: Vec<PendingConnection>,
    tree: egui_tiles::Tree<Pane>,
    tree_behavior: TreeBehavior,
}

impl ReadingRainbow {
    fn connect(&mut self, addr: String) {
        let (cmd_sender, cmd_receiver) = channel();
        let (rsp_sender, rsp_receiver) = channel();
        let ba = addr.clone();
        let worker = spawn(move || {
            worker_thread(ba, cmd_receiver, rsp_sender).unwrap();
        });
        self.pending_connections
            .push((cmd_sender, rsp_receiver, worker, addr));
    }
}

// Defining each gui pane/clickable functionality
impl App for ReadingRainbow {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut pops = Vec::new();
        for (i, connection) in self.pending_connections.iter().enumerate().rev() {
            if let Ok(RPCResponse::Connected(bs, dacc, ddcc)) = connection.1.try_recv() {
                pops.push((i, (bs, dacc, ddcc)));
            }
        }

        for pop in pops {
            let b = self.pending_connections.remove(pop.0);
            let bc = BoardConnection {
                command: b.0,
                response: b.1,
                state: pop.1 .0,
                worker_thread: b.2,
                connection_id: b.3,
                dac_capabilities: pop.1 .1,
                ddc_capabilities: pop.1 .2,
            };
            info!(
                "Attempting to add {} {:?}",
                bc.connection_id.clone(),
                self.tree.root
            );
            let id = self.tree.tiles.insert_new(Tile::Pane(Pane::Board(
                bc,
                BoardConnectionUIData {
                    setup: SetupUIData {
                        db: DACBuilder::new(pop.1 .1),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )));
            let c = self.tree.tiles.get_mut(self.tree.root.unwrap()).unwrap();
            match c {
                Tile::Container(c) => c.add_child(id),
                _ => unreachable!(),
            }
        }

        for (_id, t) in self.tree.tiles.iter_mut() {
            match t {
                Tile::Pane(p) => {
                    if let Pane::Board(bc, data) = p {
                        let resp = bc.response.try_recv();
                        match resp {
                            Ok(RPCResponse::Connected(..)) => unreachable!(),
                            Ok(RPCResponse::CaptureResult(snap)) => {
                                bc.snap_callback(data, snap);
                            }
                            Ok(RPCResponse::Sweep(sweep)) => {
                                bc.sweep_callback(data, sweep);
                            }
                            _ => {}
                        }
                    }
                }
                Tile::Container(_) => {}
            }
        }

        egui::SidePanel::left("Status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                let te = egui::TextEdit::singleline(&mut self.connection_string)
                    .hint_text("Enter a board address");
                let re = te.show(ui).response;
                let button = ui.button("Connect");
                if button.clicked()
                    || (re.lost_focus() && ui.input(|r| r.key_pressed(egui::Key::Enter)))
                {
                    if let Ok(sa) = self.connection_string.to_socket_addrs() {
                        info!("Connecting to {:?}", sa);
                        self.connect(self.connection_string.clone());
                    } else {
                        let mut content = self.connection_string.clone();
                        content.push_str(":4242");
                        if let Ok(sa) = content.to_socket_addrs() {
                            info!("Connecting to {:?}", sa);
                            self.connect(content);
                        } else {
                            error!("Unable to parse address {}", content);
                        }
                    }
                }
            });
        });
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.set_height(ui.available_height());
            self.tree.ui(&mut self.tree_behavior, ui);
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        info!("Exiting, joining all threads with best-effort...");
        let mut joins = vec![];
        for (id, t) in self.tree.tiles.iter_mut() {
            if let Tile::Pane(Pane::Board(bc, data)) = t {
                // Cancel any active sweep
                data.sweepuidata.reciever = None;
                let _ = bc.command.send(RPCCommand::Exit);
                joins.push(*id);
            }
        }
        for id in joins.into_iter() {
            if let Some(Tile::Pane(Pane::Board(bc, _))) = self.tree.tiles.remove(id) {
                let _ = bc.worker_thread.join();
            }
        }
    }
}

struct TreeBehavior {}

impl egui_tiles::Behavior<Pane> for TreeBehavior {
    fn tab_title_for_pane(&mut self, pane: &Pane) -> egui::WidgetText {
        match pane {
            Pane::Board(b, _) => b.connection_id.clone().into(),
            Pane::SweepSetup(..) => "Setup Sweep".into(),
        }
    }

    fn pane_ui(
        &mut self,
        ui: &mut egui::Ui,
        _tile_id: egui_tiles::TileId,
        pane: &mut Pane,
    ) -> egui_tiles::UiResponse {
        match pane {
            Pane::Board(connection, data) => {
                connection.ui(data, ui);
            }
            Pane::SweepSetup(..) => {}
        }
        Default::default()
    }

    fn simplification_options(&self) -> egui_tiles::SimplificationOptions {
        egui_tiles::SimplificationOptions {
            prune_empty_tabs: false,
            prune_empty_containers: false,
            prune_single_child_tabs: false,
            prune_single_child_containers: false,
            ..Default::default()
        }
    }
}

// Outputting the gui
pub fn run_gui() {
    let native_options = NativeOptions::default();

    let mut tiles = egui_tiles::Tiles::default();
    let root = tiles.insert_tab_tile(vec![]);
    let tree = egui_tiles::Tree::new("Root", root, tiles);

    eframe::run_native(
        "Reading Rainbow",
        native_options,
        Box::new(|_cc: &CreationContext| {
            Ok(Box::new(ReadingRainbow {
                connection_string: "127.0.0.1".into(),
                pending_connections: vec![],
                tree,
                tree_behavior: TreeBehavior {},
            }))
        }),
    )
    .unwrap();
}
