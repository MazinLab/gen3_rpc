// Nikki Zivkov 02/10/2025
// Logging

use std::fs::File;
use std::io::{self, Write};

pub struct Logger {
    file: File,
}

impl Logger {
    pub fn new(filename: &str) -> io::Result<Self> {
        let file = File::create(filename)?;
        Ok(Self { file })
    }

    pub fn log(&mut self, data: &str) -> io::Result<()> {
        writeln!(self.file, "{}", data)
    }
}