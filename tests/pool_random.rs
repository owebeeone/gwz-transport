//! No wire or network: adversarial scheduling of pool calls and fake resources.
#[path = "support/pool_case.rs"]
mod pool_case;
mod support;
use pool_case::{Case, Coverage};
use std::panic::{AssertUnwindSafe, catch_unwind};
use support::{case_seed, setting};
const SEED: u64 = 0x4757_5a50_2026_0919;
const CASES: usize = 2000;
const GENERATOR: &str = "gwz-transport-pool-v1";
fn run(seed: u64, run_seed: u64, index: usize) -> (u64, Coverage) {
    let mut case = Case::new(seed);
    if let Err(failure) = catch_unwind(AssertUnwindSafe(|| case.run())) {
        eprintln!(
            "generator={GENERATOR} run_seed={run_seed:#018x} case={index} case_seed={seed:#018x} step={} clock={} config={:?}\ntrace={:#?}\nReplay from gwz-transport:\nGWZ_POOL_MC_CASE_SEED={seed:#018x} cargo test --locked --test pool_random seeded_pool_lifecycles -- --exact --nocapture",
            case.step, case.clock, case.config, case.trace
        );
        std::panic::resume_unwind(failure);
    }
    (case.digest, case.coverage)
}
fn campaign(seed: u64, count: usize) -> Coverage {
    assert!(count > 0);
    eprintln!("generator={GENERATOR} run_seed={seed:#018x} cases={count}");
    let mut coverage = Coverage::default();
    for index in 0..count {
        let seed_case = case_seed(seed, index);
        let result = run(seed_case, seed, index);
        if index % 16 == 0 {
            assert_eq!(
                result,
                run(seed_case, seed, index),
                "replay mismatch: generator={GENERATOR} run_seed={seed:#018x} case={index} case_seed={seed_case:#018x}"
            );
        }
        coverage.add(&result.1);
    }
    eprintln!(
        "coverage [connect,reuse,lease,cancel,late_success,close,abort,blocked,interaction,timeout]={:?}",
        coverage.0
    );
    coverage
}
#[test]
fn seeded_pool_lifecycles() {
    if let Some(seed) = setting("GWZ_POOL_MC_CASE_SEED") {
        assert_eq!(run(seed, seed, 0), run(seed, seed, 0));
        return;
    }
    let seed = setting("GWZ_POOL_MC_SEED").unwrap_or(SEED);
    let count = setting("GWZ_POOL_MC_CASES").unwrap_or(CASES as u64) as usize;
    let coverage = campaign(seed, count);
    if seed == SEED && count >= CASES {
        assert!(
            coverage.0.iter().all(|count| *count > 100),
            "missing coverage: {coverage:?}"
        );
    }
}
#[test]
#[ignore = "long seeded pool lifecycle campaign; every failure prints a replay command"]
fn extended_pool_lifecycles() {
    let seed = setting("GWZ_POOL_MC_SEED").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    campaign(
        seed,
        setting("GWZ_POOL_MC_CASES").unwrap_or(50_000) as usize,
    );
}
