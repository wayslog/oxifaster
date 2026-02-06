#![allow(dead_code)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, Copy)]
pub struct FuzzParams {
    pub seed: u64,
    pub steps: usize,
    // Some fuzz targets don't use a key space; keep it in the shared params for convenience.
    pub key_space: u64,
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
}

fn mix_seed(mut seed: u64, tag: &str) -> u64 {
    // A tiny stable mixer so each test gets a different deterministic stream by default.
    for (i, b) in tag.as_bytes().iter().enumerate() {
        seed ^= (*b as u64) << ((i % 8) * 8);
        seed = seed.rotate_left(13).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    seed
}

pub fn params(test_tag: &str, default_steps: usize, default_key_space: u64) -> FuzzParams {
    let base_seed = env_u64("OXIFASTER_FUZZ_SEED").unwrap_or(0xA1B2_C3D4_E5F6_0708);
    let steps = env_usize("OXIFASTER_FUZZ_STEPS").unwrap_or(default_steps);
    let key_space = env_u64("OXIFASTER_FUZZ_KEY_SPACE").unwrap_or(default_key_space);

    FuzzParams {
        seed: mix_seed(base_seed, test_tag),
        steps,
        key_space: key_space.max(1),
    }
}

pub fn strict() -> bool {
    env_flag("OXIFASTER_FUZZ_STRICT")
}

pub fn verbose() -> bool {
    env_flag("OXIFASTER_FUZZ_VERBOSE")
}

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub fn choose_key(rng: &mut StdRng, key_space: u64) -> u64 {
    rng.gen_range(0..key_space)
}

pub fn choose_value(rng: &mut StdRng) -> u64 {
    rng.gen::<u64>() ^ (rng.gen::<u64>() << 1)
}
