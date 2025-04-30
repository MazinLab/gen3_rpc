// Nikki Zivkov 02/10/2025
// Readout status

// Defining Status struct 
pub struct Status {
    pub status_message: String, // Status message will be string 
}

// Implimenting methods for Status struct
impl Status {
    pub fn new() -> Self {
        Self {
            status_message: String::from("Disconnected"), // Creating default message, will be replaced 
        }
    }

    // Method for updating status (will be filled in)
    #[allow(dead_code)]
    pub fn update(&mut self, message: &str) {
        self.status_message = message.to_string();
    }
}

// Implementing default for Status struct 
impl Default for Status {
    fn default() -> Self {
        Self::new() // Creating new default instance so we can keep rechecking status 
    }
}