pub mod udma {
    use memmap2::Mmap;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    pub struct Map {
        map: Mmap,
        sync_for_cpu: Arc<Mutex<File>>,
        sync_for_device: Arc<Mutex<File>>,
        pub phys: usize,
        pub size: usize,
    }

    impl Deref for Map {
        type Target = [u8];
        #[inline]
        fn deref(&self) -> &Self::Target {
            self.map.deref()
        }
    }

    impl AsRef<[u8]> for Map {
        #[inline]
        fn as_ref(&self) -> &[u8] {
            self.deref()
        }
    }

    impl Map {
        pub fn new(name: &str) -> Result<Self, std::io::Error> {
            let map = File::open(format!("/dev/{}", name))?;

            let mut phys = File::open(format!("/sys/class/u-dma-buf/{}/phys_addr", name))?;
            let mut phys_str = String::new();
            phys.read_to_string(&mut phys_str)?;
            let phys: usize = phys_str.parse().unwrap();

            let mut size = File::open(format!("/sys/class/u-dma-buf/{}/size", name))?;
            let mut size_str = String::new();
            size.read_to_string(&mut size_str)?;
            let size: usize = size_str.parse().unwrap();

            let sync_for_cpu = Arc::new(Mutex::new(File::open(format!(
                "/sys/class/u-dma-buf/{}/sync_for_cpu",
                name
            ))?));

            let sync_for_device = Arc::new(Mutex::new(File::open(format!(
                "/sys/class/u-dma-buf/{}/sync_for_device",
                name
            ))?));

            Ok(Map {
                map: unsafe { Mmap::map(&map)? },
                sync_for_cpu,
                sync_for_device,
                phys,
                size,
            })
        }

        pub fn sync_for_cpu(&self) -> Result<(), std::io::Error> {
            let mut cpu_file = self.sync_for_cpu.lock().unwrap();
            cpu_file.write_all(b"1")?;
            Ok(())
        }

        pub fn sync_for_device(&self) -> Result<(), std::io::Error> {
            let mut device_file = self.sync_for_device.lock().unwrap();
            device_file.write_all(b"1")?;
            Ok(())
        }
    }
}

pub mod uirq {}
