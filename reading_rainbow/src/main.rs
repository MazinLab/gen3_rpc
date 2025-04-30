mod gui;
mod logger;
mod status;
mod worker;

use std::sync::mpsc::channel;
use std::thread;
use worker::worker_thread;

fn main() {
    let (cmd_sender, cmd_receiver) = channel();
    let (rsp_sender, rsp_receiver) = channel();
    let worker = thread::spawn(move || {
        worker_thread(cmd_receiver, rsp_sender).unwrap();
    });

    gui::run_gui(cmd_sender, rsp_receiver);

    worker.join().unwrap();
}
