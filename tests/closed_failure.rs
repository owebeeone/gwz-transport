use gwz_transport::{
    protocol::*,
    stream::{Config, Error, Side, StreamMachine},
};

fn closing() -> (StreamMachine, StreamMachine) {
    let mut config = Config::new("close-detail", 1, Side::Initiator);
    config.profile_version = 2;
    let mut peer = config.clone();
    peer.side = Side::Endpoint;
    let mut a = StreamMachine::new(config).unwrap();
    let mut b = StreamMachine::new(peer).unwrap();
    a.end_write().unwrap();
    while let Some(message) = a.next_message() {
        b.receive(message).unwrap();
    }
    b.end_write().unwrap();
    while let Some(message) = b.next_message() {
        a.receive(message).unwrap();
    }
    a.start_close().unwrap();
    while let Some(message) = a.next_message() {
        b.receive(message).unwrap();
    }
    (a, b)
}

#[test]
fn remediation_closed_failure_retains_exact_first_owned_detail_and_close_facts() {
    let (mut a, mut b) = closing();
    let failure = Failure {
        code: ErrorCode::Timeout,
        effect: Effect::None,
        setup_cause: Some(SetupFailureCause::Interaction),
        detail: Some(Box::new(FailureDetail {
            helper_budget_ms: Some(1250),
            retry_attempt: Some(RetryAttempt {
                attempt: 2,
                attempts: 3,
            }),
            ..Default::default()
        })),
        facts: None,
    };
    let facts = Facts {
        credential_offered: true,
        ..Default::default()
    };
    b.complete_close_failure(
        Disposition::Discarded,
        facts.clone(),
        failure.code,
        failure.effect,
    )
    .unwrap();
    let mut message = b.next_message().unwrap();
    message.closed.as_mut().unwrap().failure = Some(failure.clone());
    let mut late = message.clone();
    let pointer = message
        .closed
        .as_ref()
        .unwrap()
        .failure
        .as_ref()
        .unwrap()
        .detail
        .as_deref()
        .unwrap() as *const FailureDetail;
    a.receive(message).unwrap();
    assert_eq!(a.retained_failure(), Some(&failure));
    assert_eq!(
        a.retained_failure().unwrap().detail.as_deref().unwrap() as *const FailureDetail,
        pointer
    );
    assert_eq!(a.retained_failure_facts(), Some(&facts));
    assert_eq!(
        a.close_result(),
        Err(Error::PeerFailed {
            code: failure.code,
            effect: failure.effect
        })
    );
    late.closed
        .as_mut()
        .unwrap()
        .failure
        .as_mut()
        .unwrap()
        .detail
        .as_mut()
        .unwrap()
        .helper_budget_ms = Some(5);
    a.receive(late).unwrap();
    assert_eq!(a.retained_failure(), Some(&failure));
}

#[test]
fn remediation_invalid_closed_detail_is_not_retained() {
    let (mut a, mut b) = closing();
    b.complete_close_failure(
        Disposition::Discarded,
        Facts::default(),
        ErrorCode::Timeout,
        Effect::None,
    )
    .unwrap();
    let mut message = b.next_message().unwrap();
    message
        .closed
        .as_mut()
        .unwrap()
        .failure
        .as_mut()
        .unwrap()
        .detail = Some(Box::new(FailureDetail {
        helper_budget_ms: Some(-1),
        ..Default::default()
    }));
    assert_eq!(a.receive(message), Err(Error::Protocol));
    assert!(a.retained_failure().is_none());
}
