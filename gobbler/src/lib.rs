use serde::{Deserialize, Serialize};
use soa_derive::StructOfArray;
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Photon<P: Clone> {
    pub phase: P,
    pub bin: u32,
    pub cycle: u64,
}

#[derive(StructOfArray, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[soa_derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChunkSample {
    pub secs: u64,
    pub ns: u32,
    pub subns: u8,
    pub cycle: u64,
    pub read: u8,
    pub dropped: bool,
    pub fault: bool,
    pub empty: bool,
}

pub type Gen3Photon = Photon<i16>;

pub enum ParseError {
    AdvanceHeader { bin: u32 },
    ReadZeroed,
}

pub struct MultiDeque<T: Clone, I: Iterator<Item = T>, const N: usize> {
    heads: [VecDeque<T>; N],
    iter: I,
}

impl<T: Clone, I: Iterator<Item = T>, const N: usize> MultiDeque<T, I, N> {
    pub fn new(iter: I) -> Self {
        Self {
            heads: core::array::from_fn(|_| VecDeque::new()),
            iter,
        }
    }

    pub fn front(&mut self, lane: usize) -> Option<&T> {
        match self.heads[lane].front() {
            Some(_t) => (),
            None => {
                let elem = self.iter.next()?;
                for head in self.heads.iter_mut() {
                    head.push_back(elem.clone());
                }
            }
        };
        self.heads[lane].front()
    }

    pub fn pop_front(&mut self, lane: usize) -> Option<T> {
        match self.heads[lane].front() {
            Some(_t) => (),
            None => {
                let elem = self.iter.next()?;
                for head in self.heads.iter_mut() {
                    head.push_back(elem.clone());
                }
            }
        };
        self.heads[lane].pop_front()
    }

    pub(crate) fn consume(self) -> I {
        self.iter
    }
}

impl<T: Clone + PartialEq + Eq, I: Iterator<Item = T>, const N: usize> MultiDeque<T, I, N> {
    pub fn aligned(&mut self) -> bool {
        let mut a = true;
        for i in 0..self.heads.len() {
            a &= self.front(i).cloned() == self.front(0).cloned();
        }
        a
    }
}

pub struct GobbleState<const N: usize, I: Iterator<Item = ChunkSample>> {
    samples: MultiDeque<ChunkSample, I, N>,
    synced: [bool; N],
}

impl<const N: usize, I: Iterator<Item = ChunkSample>> GobbleState<N, I> {
    pub fn new(iter: I) -> Self {
        Self {
            samples: MultiDeque::new(iter),
            synced: core::array::from_fn(|_| false),
        }
    }

    pub fn consume(self) -> I {
        self.samples.consume()
    }
}

impl Gen3Photon {
    #[inline(always)]
    fn parse(photon: u64, cs: &ChunkSample) -> Result<Self, ParseError> {
        let phase = i16::from_le_bytes([photon.to_le_bytes()[0], photon.to_le_bytes()[1]]);
        let cycle = photon.wrapping_shr(16) & ((1 << 24) - 1);
        let bin = Self::parse_bin(photon);
        let read = Self::parse_read(photon);

        let cycle = cycle + cs.cycle.wrapping_shr(9);

        if read == 0 {
            Err(ParseError::ReadZeroed)
        } else if read != cs.read {
            Err(ParseError::AdvanceHeader { bin })
        } else {
            Ok(Gen3Photon { phase, cycle, bin })
        }
    }

    #[inline(always)]
    fn parse_bin(photon: u64) -> u32 {
        let bin = photon.wrapping_shr(16 + 24) & ((1 << 11) - 1);
        bin as u32
    }

    #[inline(always)]
    fn parse_read(photon: u64) -> u8 {
        let read = photon.wrapping_shr(16 + 24 + 11) & ((1 << 2) - 1);

        read as u8
    }

    pub fn gobble_single<I: Iterator<Item = ChunkSample>>(
        photon: u64,
        state: &mut GobbleState<4, I>,
    ) -> Self {
        loop {
            match Self::parse(
                photon,
                state
                    .samples
                    .front(Self::parse_bin(photon) as usize % 4)
                    .unwrap(),
            ) {
                Ok(photon) => return photon,
                Err(ParseError::AdvanceHeader { bin }) => {
                    state
                        .samples
                        .pop_front(bin as usize % 4)
                        .expect("Ran out of chunk samples before photons...");
                }
                Err(ParseError::ReadZeroed) => {
                    panic!("Lost stream sync...")
                }
            }
        }
    }

    pub fn gobble<I: Iterator<Item = ChunkSample>>(
        bytes: &mut impl std::io::BufRead,
        mut state: GobbleState<4, I>,
    ) -> (Vec<Self>, GobbleState<4, I>) {
        let buf = bytes.fill_buf().unwrap();
        let consume = 8 * (buf.len() / 8);

        // TODO: To add SIMD here, iterate over ~2048 bit sized chunks_exact segments using
        // and assuming the chunksampler is alligned, recovering with read_single if it is not
        let it = buf.chunks_exact(8);
        let ret = it
            .filter_map(|c| {
                let p = u64::from_le_bytes(c.try_into().unwrap());
                if !(state.synced[Self::parse_bin(p) as usize % 4]) & (Self::parse_read(p) == 0) {
                    core::hint::cold_path();
                    None
                } else {
                    Some(Self::gobble_single(p, &mut state))
                }
            })
            .collect();

        bytes.consume(consume);

        (ret, state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
