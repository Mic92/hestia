//! Shared by the `*_sim` examples.

#![allow(dead_code)]

pub const MIB: f64 = 1048576.0;

/// xorshift64, seeded per run so results are reproducible.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn uniform(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    pub fn normal(&mut self) -> f64 {
        let (u, v) = (self.uniform().max(1e-12), self.uniform());
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }
    pub fn lognormal(&mut self, median: f64, sigma: f64) -> f64 {
        median * (sigma * self.normal()).exp()
    }
    pub fn exponential(&mut self, rate: f64) -> f64 {
        -self.uniform().max(1e-12).ln() / rate
    }
}

/// Sizes in TB from argv, else the given defaults.
pub fn tb_args(defaults: &[f64]) -> Vec<f64> {
    let args: Vec<f64> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    if args.is_empty() {
        defaults.to_vec()
    } else {
        args
    }
}
