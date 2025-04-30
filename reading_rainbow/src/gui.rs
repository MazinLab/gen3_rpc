// Nikki Zivkov 02/06/2025
// This script generates a template for the gui
// Will be filled in later with actual gui elements
// Called to in main

// Importing crates/modules
use crate::logger::Logger;
use crate::status::Status;
use crate::worker::{RPCCommand, RPCResponse};
use eframe::{egui, App, CreationContext, NativeOptions};
use num::Complex;
use std::process::Command; 
use std::sync::mpsc::{Receiver, Sender};
use gen3_rpc::{Hertz, Attens}; 
use gen3_rpc::utils::client::{PowerSetting, SweepConfig}; 
use std::time::{SystemTime, Duration};

// Defining structs
pub struct MyApp {
    current_pane: Pane,     // Keeps track of current pane
    command_input: String,  // Command line input
    command_output: String, // Command line output
    logger: Option<Logger>, // Data logger
    status: Status,         // Device status
    settings: Settings,
    command: Sender<RPCCommand>,
    response: Receiver<RPCResponse>,
    error_message: Option<String>, // Error message
    dac_table: Option<Box<[Complex<i16>; 524288]>>, // DAC table
    if_freq: Option<Hertz>, // IF frequency
    if_attens: Option<Attens>, // Attenuations
    connection_time: Option<SystemTime>, // Connection timestamp
    sweep_start_freq: String, // Input for the starting frequency
    sweep_stop_freq: String,  // Input for the stopping frequency
    sweep_count: String,      // Input for the total number of counts in frequency list
    sweep_freqs: Vec<Hertz>,  // Generated list of frequencies
    sweep_input_atten: String, // Input attenuation (input)
    sweep_output_atten: String, // Output attenuation (input)
    sweep_dsp_scale: String,   // Input for DSP scale
    sweep_average: String,    // Input for the average value
    sweep_result: Option<String>, // Display Sweep results (non-functional!!!)
}

// Defining different panes in the gui
#[derive(PartialEq, Default)]
enum Pane {
    #[default]
    Settings,
    Command, 
    DataLogging,
    Status,
    DSPScale, 
    DACTable, 
    IFBoard, 
    Sweep, 
}

#[derive(Default)]
struct Settings {
    fft_scale: String, // Use String to handle text input
    if_freq: String, // Use String to handle IF frequency input
    if_input_atten: String, // Use String to handle IF input attenuation
    if_output_atten: String, // Use String to handle IF output attenuation
    if_freq_mode: String, // Use String to handle IF frequency input (Manual or Board)
    dsp_scale_mode: String, // Use String to handle DSP scale input (Manual or Board)
    if_atten_mode: String, // Use String to handle IF attenuation input (Manual or Board)
}

// Defining each gui pane/clickable functionality
impl App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Ok(c) = self.response.try_recv() {
            match c {
                // Handle the CaptureResult response
                RPCResponse::CaptureResult(data) => {
                    if data.is_empty() {
                        self.error_message = Some("Capture failed: No data available.".to_string());
                    } else {
                        self.sweep_result = Some(format!("Captured Data: {:?}", data));
                    }
                }
                // Update the FFT scale in the settings
                RPCResponse::FFTScale(i) => {
                    self.settings.fft_scale = i.map_or_else(|| "0".to_string(), |v| v.to_string());
                }
                // Update the DAC table
                RPCResponse::DACTable(d) => {
                    self.dac_table = d;
                }
                // Update the IF frequency
                RPCResponse::IFFreq(f) => {
                    self.if_freq = f;
                }
                // Update the attenuation
                RPCResponse::IFAttens(a) => {
                    self.if_attens = a;
                }
                // Update the connection status
                RPCResponse::Connected(time) => {
                    self.status.update("Connected");
                    self.connection_time = Some(time);
                }
                RPCResponse::Sweep(sweep) => {
                    self.sweep_result = Some(format!("{:?}", sweep));
                }
            }
        }

        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            ui.heading("Menu");

            if ui.button("Settings").clicked() {
                self.current_pane = Pane::Settings;
            }
            if ui.button("Command Line").clicked() {
                self.current_pane = Pane::Command;
            }
            if ui.button("Data Logging").clicked() {
                self.current_pane = Pane::DataLogging;
            }
            if ui.button("Status").clicked() {
                self.current_pane = Pane::Status;
            }
            if ui.button("DSP Scale Adjustment").clicked() {
                self.current_pane = Pane::DSPScale;
            }
            if ui.button("DAC Table").clicked() {
                self.current_pane = Pane::DACTable;
            }
            if ui.button("IF Board").clicked() {
                self.current_pane = Pane::IFBoard;
            }
            if ui.button("Sweep").clicked() {
                self.current_pane = Pane::Sweep;
            }
        });

        // Showing the central pane selected
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.current_pane {
                Pane::Settings => {
                    ui.heading("Settings");

                    // Display connection status and timestamp
                    if let Some(connection_time) = self.connection_time {
                        let duration = connection_time.elapsed().unwrap_or(Duration::new(0, 0));
                        ui.label(format!("Successfully connected to server"));
                        ui.label(format!("Connection duration: {:.2?}", duration));
                    } else {
                        ui.label("Not connected to server");
                    }
                }
                Pane::Command => {
                    ui.heading("Command Line");

                    // Input command line text
                    ui.horizontal(|ui| {
                        ui.label("Command:");
                        ui.text_edit_singleline(&mut self.command_input);
                        if ui.button("Run").clicked() {
                            self.command_output = run_command(&self.command_input);
                        }
                    });

                    // Show command line output
                    ui.label("Output:");
                    ui.add(
                        egui::TextEdit::multiline(&mut self.command_output)
                            .desired_width(f32::INFINITY) // Make the output box fill the pane width
                            .desired_rows(10), // Number of output rows (can adjust)
                    );
                }
                // Log Data (non-fully functional) 
                Pane::DataLogging => {
                    ui.heading("Data Logging");

                    if ui.button("Start Logging").clicked() {
                        self.logger =
                            Some(Logger::new("log.txt").expect("Failed to create log file"));
                        self.status.update("Logging started");
                    }
                    if ui.button("Stop Logging").clicked() {
                        self.logger = None;
                        self.status.update("Logging stopped");
                    }

                    if let Some(logger) = &mut self.logger {
                        logger.log("Sample data").expect("Failed to log data");
                        ui.label("Logging data...");
                    } else {
                        ui.label("Logging stopped.");
                    }
                }
                Pane::Status => {
                    ui.heading("Status");
                    ui.label(&self.status.status_message);
                }
                Pane::DSPScale => {
                    ui.heading("DSP Scale");

                    // Button to request the current DSP scale
                    if ui.button("Get DSP Scale").clicked() {
                        self.command.send(RPCCommand::GetFFTScale).unwrap();
                    }

                    // Display the current DSP scale if available
                    ui.label(format!("DSP Scale: {}", self.settings.fft_scale));

                    // Text input for adjusting scale
                    ui.horizontal(|ui| {
                        ui.label("DSP Scale:");
                        ui.text_edit_singleline(&mut self.settings.fft_scale);
                    });

                    // Display valid range and example values
                    ui.label("Valid values: 4095, 3967, 1919, 1911, 1879, 1877, 1365, 1301, 277, 273, 257, 1, 0");

                    // Button to apply scale
                    if ui.button("Apply Scale").clicked() {
                        let valid_values = vec![4095, 3967, 1919, 1911, 1879, 1877, 1365, 1301, 277, 273, 257, 1, 0];
                        if let Ok(scale_value) = self.settings.fft_scale.parse::<u16>() {
                            // Only pass the value to the worker.rs if it is within the accepted values
                            if valid_values.contains(&scale_value) {
                                if let Err(e) = set_scale(&self.command, scale_value) {
                                    self.error_message = Some(format!("Failed to set scale: {}", e));
                                } else {
                                    self.error_message = None; // Clear the error message on success
                                }
                            } else {
                                // Display an error message if the value is not valid
                                self.error_message = Some("Invalid scale value. Please enter one of the valid values.".to_string());
                            }
                        } else {
                            // Error if input is not a valid number
                            self.error_message = Some("Invalid scale value. Please enter one of the valid values.".to_string());
                        }
                    }

                    // Display possible error
                    if let Some(ref error_message) = self.error_message {
                        ui.label(error_message);
                    }
                }
                Pane::DACTable => {
                    ui.heading("DAC Table");

                    // Button to request the current DAC table
                    if ui.button("Get DAC Table").clicked() {
                        self.command.send(RPCCommand::GetDACTable).unwrap();
                    }

                    // Display the current DAC table if available (non-fully functional)
                    // Possible memory issue with large table of complex values 
                    if let Some(ref dac_table) = self.dac_table {
                        ui.label(format!("DAC Table: {:?}", &dac_table[..16]));
                    }

                    // Text input for setting DAC table
                    ui.horizontal(|ui| {
                        ui.label("DAC Table:");
                        ui.text_edit_singleline(&mut self.settings.fft_scale); // Use a different field for DAC table input
                    });

                    // Button to set the DAC table
                    if ui.button("Set DAC Table").clicked() {
                        // PArse input and set DAC table
                        let data: Box<[Complex<i16>; 524288]> = Box::new([Complex::new(0, 0); 524288]); // Replace with actual user input parsing
                        if let Err(e) = set_dac_table(&self.command, data) {
                            self.error_message = Some(format!("Failed to set DAC table: {}", e));
                        } else {
                            self.error_message = None; // Clear the error message on success
                        }
                    }

                    // Display the error message if it exists
                    if let Some(ref error_message) = self.error_message {
                        ui.label(error_message);
                    }
                }
                Pane::IFBoard => {
                    ui.heading("IF Board");

                    // Button to request the current IF frequency
                    if ui.button("Get IF Frequency").clicked() {
                        self.command.send(RPCCommand::GetIFFreq).unwrap();
                    }

                    // Display the current IF frequency if available
                    if let Some(ref if_freq) = self.if_freq {
                        ui.label(format!("Current IF Frequency: {}/{}", if_freq.numer(), if_freq.denom()));
                    }

                    // Text input for setting IF frequency
                    ui.horizontal(|ui| {
                        ui.label("IF Frequency:");
                        ui.text_edit_singleline(&mut self.settings.if_freq);
                    });

                    // Button to set the IF frequency
                    if ui.button("Set IF Frequency").clicked() {
                        // Parse user input and set the IF frequency
                        let parts: Vec<&str> = self.settings.if_freq.split('/').collect();
                        if parts.len() == 2 {
                            if let (Ok(numer), Ok(denom)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                                let freq = Hertz::new(numer, denom);
                                if let Err(e) = set_if_freq(&self.command, freq) {
                                    self.error_message = Some(format!("Failed to set IF frequency: {}", e));
                                } else {
                                    self.error_message = None; // Clear the error message on success
                                }
                            } else {
                                self.error_message = Some("Invalid IF frequency. Enter valid numerator/denominator.".to_string());
                            }
                        } else {
                            self.error_message = Some("Invalid IF frequency format. Use the format numerator/denominator.".to_string());
                        }
                    }

                    // Button to request the current IF attenuations
                    if ui.button("Get IF Attenuation").clicked() {
                        self.command.send(RPCCommand::GetIFAttens).unwrap();
                    }

                    // Display the current IF attenuations if available
                    if let Some(ref if_attens) = self.if_attens {
                        ui.label(format!("Current IF Attenuation - Input: {}, Output: {}", if_attens.input, if_attens.output));
                    }

                    // Text input for setting IF attenuations
                    ui.horizontal(|ui| {
                        ui.label("IF Input Attenuation:");
                        ui.text_edit_singleline(&mut self.settings.if_input_atten);
                    });
                    ui.horizontal(|ui| {
                        ui.label("IF Output Attenuation:");
                        ui.text_edit_singleline(&mut self.settings.if_output_atten);
                    });

                    // Button to set the IF attenuations
                    if ui.button("Set IF Attenuation").clicked() {
                        // Parse the user input and set the IF attenuations
                        if let (Ok(input), Ok(output)) = (self.settings.if_input_atten.parse::<i32>(), self.settings.if_output_atten.parse::<i32>()) {
                            let attens = Attens {
                                input: input as f32,
                                output: output as f32,
                            };
                            if let Err(e) = set_if_attens(&self.command, attens) {
                                self.error_message = Some(format!("Failed to set IF attenuation: {}", e));
                            } else {
                                self.error_message = None; // Clear the error message on success
                            }
                        } else {
                            self.error_message = Some("Invalid IF attenuations. Enter valid input and output attenuations.".to_string());
                        }
                    }

                    // Display the error message if it exists
                    if let Some(ref error_message) = self.error_message {
                        ui.label(error_message);
                    }
                }
                Pane::Sweep => {
                    ui.heading("Sweep Configuration");

                    // Frequency Settings
                    ui.group(|ui| {
                        ui.heading("Frequency Settings");

                        // Option to choose between manual input or fetching frequency from the board
                        ui.horizontal(|ui| {
                            ui.label("Initial Frequency:");
                            if ui.radio_value(&mut self.settings.if_freq_mode, "Manual".to_string(), "Manual").clicked() {
                                self.settings.if_freq_mode = "Manual".to_string();
                            }
                            if ui.radio_value(&mut self.settings.if_freq_mode, "Board".to_string(), "Board").clicked() {
                                self.settings.if_freq_mode = "Board".to_string();
                            }
                        });

                        // Handle manual input or fetching frequency from the board
                        if self.settings.if_freq_mode == "Manual" {
                            ui.horizontal(|ui| {
                                ui.label("Start Frequency (e.g., 6000000000):");
                                ui.text_edit_singleline(&mut self.sweep_start_freq);
                            });
                        } else if self.settings.if_freq_mode == "Board" {
                            if ui.button("Get Frequency from Board").clicked() {
                                self.command.send(RPCCommand::GetIFFreq).unwrap();
                            }

                            if let Some(ref if_freq) = self.if_freq {
                                ui.label(format!("Initial Frequency (from board): {}/{}", if_freq.numer(), if_freq.denom()));
                            } else {
                                ui.label("Initial Frequency not available.");
                            }
                        }

                        // Show stopping frequency and number of counts
                        if self.settings.if_freq_mode == "Manual" || self.settings.if_freq_mode == "Board" {
                            ui.horizontal(|ui| {
                                ui.label("Stopping Frequency (e.g., 6020000000):");
                                ui.text_edit_singleline(&mut self.sweep_stop_freq);
                            });

                            ui.horizontal(|ui| {
                                ui.label("Number of Frequency Values:");
                                ui.text_edit_singleline(&mut self.sweep_count);
                            });

                            // Button to generate frequencies
                            if ui.button("Generate Frequency List").clicked() {
                                let start_freq = if self.settings.if_freq_mode == "Manual" {
                                    self.sweep_start_freq.parse::<i64>().ok()
                                } else {
                                    self.if_freq.as_ref().map(|f| f.numer()).cloned()
                                };

                                if let (Some(start), Ok(stop), Ok(count)) = (
                                    start_freq,
                                    self.sweep_stop_freq.parse::<i64>(),
                                    self.sweep_count.parse::<usize>(),
                                ) {
                                    if count > 1 && start < stop {
                                        self.sweep_freqs = (0..count)
                                            .map(|i| {
                                                let freq = start + i as i64 * (stop - start) / (count as i64 - 1);
                                                Hertz::new(freq, 1)
                                            })
                                            .collect();
                                        self.error_message = None; // Clear any previous error messages
                                    } else {
                                        self.error_message = Some("Invalid input: Count must be > 1 and initial frequency < stopping frequency.".to_string());
                                    }
                                } else {
                                    self.error_message = Some("Invalid input: Enter valid numbers for initial/stopping frequency and count.".to_string());
                                }
                            }

                            // Display the generated frequencies
                            if !self.sweep_freqs.is_empty() {
                                ui.label("Generated Frequency List:");
                                for freq in &self.sweep_freqs {
                                    ui.label(format!("{}/{}", freq.numer(), freq.denom()));
                                }
                            }
                        }
                    });

                    // Power Settings
                    ui.group(|ui| {
                        ui.heading("Power Settings");

                        // Option to choose between manual input or fetching attenuations from the board
                        ui.horizontal(|ui| {
                            ui.label("Attenuations:");
                            if ui.radio_value(&mut self.settings.if_atten_mode, "Manual".to_string(), "Manual").clicked() {
                                self.settings.if_atten_mode = "Manual".to_string();
                            }
                            if ui.radio_value(&mut self.settings.if_atten_mode, "Board".to_string(), "Board").clicked() {
                                self.settings.if_atten_mode = "Board".to_string();
                            }
                        });

                        // Handle manual input or fetching attenuations from the board
                        if self.settings.if_atten_mode == "Manual" {
                            ui.horizontal(|ui| {
                                ui.label("Input Attenuation:");
                                ui.text_edit_singleline(&mut self.sweep_input_atten);
                            });

                            ui.horizontal(|ui| {
                                ui.label("Output Attenuation:");
                                ui.text_edit_singleline(&mut self.sweep_output_atten);
                            });
                        } else if self.settings.if_atten_mode == "Board" {
                            if ui.button("Get IF Attenuations from Board").clicked() {
                                self.command.send(RPCCommand::GetIFAttens).unwrap();
                            }

                            if let Some(ref if_attens) = self.if_attens {
                                ui.label(format!(
                                    "Current IF Attenuations - Input: {}, Output: {}",
                                    if_attens.input, if_attens.output
                                ));
                            } else {
                                ui.label("Attenuation not available.");
                            }
                        }

                        // Input for DSP scale
                        ui.horizontal(|ui| {
                            ui.label("DSP Scale:");
                            if ui.radio_value(&mut self.settings.dsp_scale_mode, "Manual".to_string(), "Manual").clicked() {
                                self.settings.dsp_scale_mode = "Manual".to_string();
                            }
                            if ui.radio_value(&mut self.settings.dsp_scale_mode, "Board".to_string(), "Board").clicked() {
                                self.settings.dsp_scale_mode = "Board".to_string();
                            }
                        });

                        if self.settings.dsp_scale_mode == "Manual" {
                            ui.horizontal(|ui| {
                                ui.label("Enter DSP Scale:");
                                ui.text_edit_singleline(&mut self.sweep_dsp_scale);
                            });
                        } else if self.settings.dsp_scale_mode == "Board" {
                            if ui.button("Get DSP Scale from Board").clicked() {
                                self.command.send(RPCCommand::GetFFTScale).unwrap();
                            }

                            ui.label(format!("Current DSP Scale: {}", self.settings.fft_scale));
                        }
                    });

                    // Time Average
                    ui.group(|ui| {
                        ui.heading("Time Average");

                        ui.horizontal(|ui| {
                            ui.label("Average:");
                            ui.text_edit_singleline(&mut self.sweep_average);
                        });

                        // Button to perform the sweep
                        if ui.button("Perform Sweep").clicked() {
                            let dsp_scale = if self.settings.dsp_scale_mode == "Manual" {
                                self.sweep_dsp_scale.parse::<u16>().ok()
                            } else {
                                self.settings.fft_scale.parse::<u16>().ok()
                            };

                            let input_atten = if self.settings.if_atten_mode == "Manual" {
                                self.sweep_input_atten.parse::<f32>().ok()
                            } else {
                                self.if_attens.as_ref().map(|a| a.input)
                            };

                            let output_atten = if self.settings.if_atten_mode == "Manual" {
                                self.sweep_output_atten.parse::<f32>().ok()
                            } else {
                                self.if_attens.as_ref().map(|a| a.output)
                            };

                            if let (Some(input_atten), Some(output_atten), Ok(average), Some(fft_scale)) = (
                                input_atten,
                                output_atten,
                                self.sweep_average.parse::<u64>(),
                                dsp_scale,
                            ) {
                                let settings = vec![PowerSetting {
                                    attens: Attens {
                                        input: input_atten,
                                        output: output_atten,
                                    },
                                    fft_scale,
                                }];

                                let config = SweepConfig {
                                    freqs: self.sweep_freqs.clone(),
                                    settings,
                                    average,
                                };

                                self.command.send(RPCCommand::SweepConfig(config)).unwrap();
                            } else {
                                self.error_message = Some("Invalid input values.".to_string());
                            }
                        }

                        // Button to perform a capture 
                        // Non-functional (yields error)
                        if ui.button("Capture").clicked() {
                            self.command.send(RPCCommand::PerformCapture).unwrap();
                        }
                    });

                    // Display error message
                    if let Some(ref error_message) = self.error_message {
                        ui.label(error_message);
                    }

                    // Display the sweep result
                    if let Some(ref sweep_result) = self.sweep_result {
                        ui.label(sweep_result);
                    }
                }
            }
        });
    }
}

// Function to run a command and return the output
fn run_command(command: &str) -> String {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .expect("Failed to execute command");

    if output.status.success() {
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        String::from_utf8_lossy(&output.stderr).to_string()
    }
}

// Function to set the scale value
fn set_scale(tx: &Sender<RPCCommand>, scale: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting scale to: {}", scale);
    tx.send(RPCCommand::SetFFTScale(scale))?;
    Ok(())
}

// Function to set the DAC table
fn set_dac_table(tx: &Sender<RPCCommand>, data: Box<[Complex<i16>; 524288]>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting DAC table");
    tx.send(RPCCommand::SetDACTable(data))?;
    Ok(())
}

// Function to set the IF frequency
fn set_if_freq(tx: &Sender<RPCCommand>, freq: Hertz) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting IF frequency to: {}/{}", freq.numer(), freq.denom());
    tx.send(RPCCommand::SetIFFreq(freq))?;
    Ok(())
}

// Function to set the IF attenuations
fn set_if_attens(tx: &Sender<RPCCommand>, attens: Attens) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting IF attenuations - Input: {}, Output: {}", attens.input, attens.output);
    tx.send(RPCCommand::SetIFAttens(attens))?;
    Ok(())
}


// Outputting the gui
pub fn run_gui(command: Sender<RPCCommand>, response: Receiver<RPCResponse>) {
    let native_options = NativeOptions::default();
    eframe::run_native(
        "Reading Rainbow",
        native_options,
        Box::new(|cc: &CreationContext| {
            let fonts = egui::FontDefinitions::default(); // Minimal fonts (don't delete)
            // fonts.font_data.clear(); // Uncomment to remove default fonts (you need to upload a custom font file)
            cc.egui_ctx.set_fonts(fonts);

            Ok(Box::new(MyApp {
                current_pane: Pane::Settings,
                command_input: String::new(),
                command_output: String::new(),
                logger: None,
                status: Status::new(),
                settings: Settings::default(),
                command,
                response,
                error_message: None,
                dac_table: None,
                if_freq: None,
                if_attens: None,
                connection_time: None,
                sweep_start_freq: String::new(),
                sweep_stop_freq: String::new(),
                sweep_count: String::new(),
                sweep_freqs: Vec::new(),
                sweep_input_atten: String::new(),
                sweep_output_atten: String::new(),
                sweep_dsp_scale: String::new(),
                sweep_average: String::new(),
                sweep_result: None,
            }))
        }),
    )
    .unwrap_or_else(|e| eprintln!("Failed to run native: {}", e));
}
