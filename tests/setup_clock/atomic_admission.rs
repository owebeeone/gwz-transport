use super::*;
#[test]
fn atomic_terminal_admission_distinguishes_equal_competing_terminal_and_expiry() {
    use gwz_transport::protocol::{Effect, ErrorCode};
    let resource = SetupCause::ResourceFailure {
        code: ErrorCode::Authentication,
        effect: Effect::None,
        setup_cause: None,
    };
    for prior in [resource, SetupCause::Cancelled] {
        let (_, _, clock) = fixture(100, 10);
        assert!(matches!(
            clock.observe().deliver(),
            Observation::Alive { .. }
        ));
        let other = clock.clone();
        let winner = std::thread::spawn(move || other.terminate(prior).deliver())
            .join()
            .unwrap();
        assert_eq!(clock.terminate_if_alive(resource).deliver(), Err(winner));
        assert_eq!(terminal(&clock), winner);
    }
    let (_, now, clock) = fixture(100, 10);
    now.store(100, Ordering::SeqCst);
    let refused = clock.terminate_if_alive(resource).deliver().unwrap_err();
    assert_eq!(refused.cause, SetupCause::NetworkAggregate);
    let (_, _, clock) = fixture(100, 10);
    let admitted = clock.terminate_if_alive(resource).deliver().unwrap();
    assert_eq!(admitted.cause, resource);
    assert_eq!(clock.terminate_if_alive(resource).deliver(), Err(admitted));
}
