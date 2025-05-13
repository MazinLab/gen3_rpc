mod gui;
mod status;
mod worker;

// use std::sync::mpsc::channel;
// use std::thread;
// use worker::worker_thread;

fn main() {
    env_logger::init();

    // let (cmd_sender, cmd_receiver) = channel();
    // let (rsp_sender, rsp_receiver) = channel();
    // let worker = thread::spawn(move || {
    //     worker_thread(
    //         "mkidrfsoc4x2.physics.ucsb.edu:4242",
    //         cmd_receiver,
    //         rsp_sender,
    //     )
    //     .unwrap();
    // });

    gui::run_gui();

    // worker.join().unwrap();
}
