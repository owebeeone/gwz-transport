//! Generator v1: fixed SplitMix64 arithmetic, independent of platform or rand versions.
pub struct Random(pub u64);
impl Random {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    pub fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit as u64) as usize
    }
}
pub fn case_seed(run_seed: u64, index: usize) -> u64 {
    Random(run_seed ^ (index as u64).wrapping_mul(0xa24baed4963ee407)).next()
}
pub fn setting(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| {
        let value = value.replace('_', "");
        if let Some(hex) = value.strip_prefix("0x") {
            u64::from_str_radix(hex, 16).expect("invalid hexadecimal Monte Carlo setting")
        } else {
            value.parse().expect("invalid decimal Monte Carlo setting")
        }
    })
}
