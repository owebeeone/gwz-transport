//! The client's receiver against a raw peer playing the core: binds and opens are crafted here, so what the receiver
//! admits and refuses is read off the wire, with no core-side handle in between.
use super::{
    super::{AllowedSet, Invocation, ServiceClass},
    world::{Way, World, key, prepared},
};
use gwz_transport::{
    mux::{self, Attachment, Mux},
    protocol::{
        AuthPolicy, Deadlines, Destination, Effect, Envelope, ErrorCode, GitService, Identity,
        IdentityMode, MessageKind, Open, Scheme,
    },
};
use std::sync::atomic::Ordering;

const SESSION: &str = "00000000000000000000000000000c0f.1";

/// A core that speaks the transport's initiator state machine directly.
struct Raw {
    mux: std::cell::RefCell<Mux>,
    request: String,
    session: String,
}
impl Raw {
    fn bind(w: &World, request: &str, session: &str) -> Self {
        let config = mux::Config {
            limits: crate::binding::limits(),
            max_requests: 4,
            ..Default::default()
        };
        let mut mux = Mux::initiator(session, config).unwrap();
        mux.register(request, Some(request.to_owned())).unwrap();
        mux.begin(request).unwrap();
        let raw = Self {
            mux: std::cell::RefCell::new(mux),
            request: request.to_owned(),
            session: session.to_owned(),
        };
        raw.flush(w);
        raw
    }
    /// The mux's outbound messages onto the wire.
    fn flush(&self, w: &World) {
        while let Some(message) = self.mux.borrow_mut().next_message() {
            w.carrier.inject(Way::ToClient, message);
        }
    }
    /// What the client sent since the last read, with the mux fed the bootstrap reply.
    fn read(&self, w: &World) -> Vec<Attachment> {
        let messages = w.carrier.take(Way::ToCore);
        for message in &messages {
            if message.1.session_id == self.session && message.1.kind == MessageKind::Bound {
                self.mux.borrow_mut().receive(message).unwrap();
            }
        }
        messages
    }
    fn endpoint_id(&self) -> String {
        self.mux
            .borrow()
            .binding()
            .expect("bound")
            .endpoint_id()
            .to_owned()
    }
    /// A typed `Open` on stream `n`, as a core-built one.
    fn open_frame(&self, stream: i64, open: Open) -> Attachment {
        let envelope = Envelope {
            version: 2,
            session_id: self.session.clone(),
            stream_id: stream,
            kind: MessageKind::Open,
            open: Some(open),
            ..Default::default()
        };
        (self.request.clone(), envelope)
    }
    fn open(&self, scheme: Scheme, service: GitService, host: &str, port: i64, path: &str) -> Open {
        Open {
            endpoint_id: self.endpoint_id(),
            operation_id: self.request.clone(),
            destination: Destination {
                scheme,
                host: host.into(),
                port,
                path: path.into(),
                ssh_username: (scheme == Scheme::Ssh).then(|| "git".to_owned()),
                https_username: None,
            },
            service,
            identity: Identity {
                mode: if scheme == Scheme::Ssh {
                    IdentityMode::Ambient
                } else {
                    IdentityMode::CredentialsDisabled
                },
                key_path: None,
                path_base: None,
            },
            policy: if scheme == Scheme::Ssh {
                AuthPolicy::SshAmbient
            } else {
                AuthPolicy::Anonymous
            },
            deadlines: Deadlines {
                allocation_ms: 1000,
                connect_ms: 1000,
                io_ms: 1000,
                interaction_ms: 1000,
                cleanup_ms: 1000,
            },
            receive_limits: self.mux.borrow().binding().expect("bound").limits().clone(),
        }
    }
    fn ssh_open(&self, service: GitService, path: &str) -> Open {
        self.open(Scheme::Ssh, service, "example.invalid", 22, path)
    }
    fn send(&self, w: &World, frame: Attachment) {
        w.carrier.inject(Way::ToClient, frame);
        w.settle_client();
    }
}
fn rejected(messages: &[Attachment]) -> Vec<&Envelope> {
    messages
        .iter()
        .map(|m| &m.1)
        .filter(|e| e.kind == MessageKind::BindRejected)
        .collect()
}
fn of_kind(messages: &[Attachment], kind: MessageKind) -> Vec<&Envelope> {
    messages
        .iter()
        .map(|m| &m.1)
        .filter(|e| e.kind == kind)
        .collect()
}

fn started(w: &World, request: &str, session: &str, invocation: Invocation) -> Raw {
    w.call_with(request, invocation, Some("/home/me"));
    let raw = Raw::bind(w, request, session);
    w.settle_client();
    let replies = raw.read(w);
    assert_eq!(
        of_kind(&replies, MessageKind::Bound).len(),
        1,
        "{replies:?}"
    );
    raw
}
fn invocation(class: ServiceClass, fetch: &[&str], push: &[&str]) -> Invocation {
    let mut invocation = Invocation::new(prepared("client-call", 1), class);
    invocation.fetch = AllowedSet::parse(fetch).unwrap();
    invocation.push = AllowedSet::parse(push).unwrap();
    invocation
}
/// An `open` frame on stream `n` that the receiver must answer `open_failed(invalid_request)`, `Effect::None`.
fn assert_refused(w: &World, raw: &Raw, stream: i64, open: Open) {
    raw.send(w, raw.open_frame(stream, open));
    let replies = raw.read(w);
    let failed = of_kind(&replies, MessageKind::OpenFailed);
    assert_eq!(failed.len(), 1, "{replies:?}");
    assert_eq!(failed[0].stream_id, stream);
    let failure = failed[0].open_failed.as_ref().unwrap();
    assert_eq!(
        (failure.code, failure.effect),
        (ErrorCode::InvalidRequest, Effect::None)
    );
    assert!(
        failure.facts.is_none() && failure.detail.is_none(),
        "the core learns nothing but the refusal"
    );
    assert_eq!(
        w.rig.world.connects(),
        0,
        "no connection, agent, key or helper was touched"
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 0);
}
fn assert_admitted(w: &World, raw: &Raw, stream: i64, open: Open) {
    raw.send(w, raw.open_frame(stream, open));
    w.settle_client();
    let replies = raw.read(w);
    assert_eq!(
        of_kind(&replies, MessageKind::Opened).len(),
        1,
        "{replies:?}"
    );
}

#[test]
fn a_bind_for_an_unknown_finished_or_already_bound_request_is_bind_rejected_invalid_request_with_no_effect()
 {
    let w = World::new();
    // Unknown: the client never sent this call.
    let raw = Raw::bind(&w, "ghost", SESSION);
    w.settle_client();
    let replies = raw.read(&w);
    let rejections = rejected(&replies);
    assert_eq!(rejections.len(), 1, "{replies:?}");
    let failure = rejections[0].bind_rejected.as_ref().unwrap();
    assert_eq!(
        (failure.code, failure.effect),
        (ErrorCode::InvalidRequest, Effect::None)
    );
    assert!(failure.facts.is_none());
    assert_eq!(w.receiver.bindings(), 0, "a refused bind made no state");
    assert_eq!(w.host.snapshot().bindings, 0);
    assert_eq!(w.host.snapshot().active_operations, 0);

    // Already bound: a second bind for the call under a new session.
    let _first = started(
        &w,
        "call-1",
        "00000000000000000000000000000c0f.2",
        invocation(ServiceClass::Fetch, &[], &[]),
    );
    let second = Raw::bind(&w, "call-1", "00000000000000000000000000000c0f.3");
    w.settle_client();
    let replies = second.read(&w);
    assert_eq!(rejected(&replies).len(), 1, "{replies:?}");
    assert_eq!(w.receiver.bindings(), 1, "the first binding is untouched");

    // Finished: the call ended on the client; a late bind for it finds nothing.
    w.receiver.end_call("call-1");
    let late = Raw::bind(&w, "call-1", "00000000000000000000000000000c0f.4");
    w.settle_client();
    assert_eq!(rejected(&late.read(&w)).len(), 1);
    assert_eq!(w.receiver.bindings(), 0);
}

#[test]
fn a_non_ascii_host_is_refused_and_the_refusal_names_the_host_and_the_reason() {
    // Decision of the rework brief: no IDNA dependency for 1.1.0, so a non-ASCII host is refused, and the command that
    // prints refusals (section 5.6) is told which host and why. The core is told only `invalid_request`.
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(ServiceClass::Fetch, &["ssh://example.invalid/"], &[]),
    );
    assert_refused(
        &w,
        &raw,
        1,
        raw.open(
            Scheme::Ssh,
            GitService::UploadPackExchange,
            "b\u{fc}cher.example",
            22,
            "/org/repo",
        ),
    );
    // A bidirectional override in a core-named host cannot spoof the line: it is shown as an escape.
    assert_refused(
        &w,
        &raw,
        2,
        raw.open(
            Scheme::Ssh,
            GitService::UploadPackExchange,
            "\u{202e}evil.example",
            22,
            "/org/repo",
        ),
    );
    let notes = w.receiver.take_refusals();
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert_eq!(notes[0].request, "call-1");
    assert_eq!(
        notes[0].text,
        r#"the core asked to fetch from host "b\u{fc}cher.example" (upload-pack, port 22), which this command cannot admit: the host is not ASCII, and this release maps no IDNA (UTS-46) host names"#
    );
    assert!(
        notes[1].text.contains(r#"host "\u{202e}evil.example""#),
        "{:?}",
        notes[1].text
    );
    assert!(notes.iter().all(|note| note.text.is_ascii()));
    assert!(w.receiver.take_refusals().is_empty(), "taken once");
}

#[test]
fn an_open_outside_the_allowed_set_leaves_a_note_in_the_words_of_section_5_6() {
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(
            ServiceClass::Fetch,
            &["ssh://example.invalid/org/"],
            &["ssh://example.invalid/"],
        ),
    );
    assert_refused(
        &w,
        &raw,
        1,
        raw.open(
            Scheme::Ssh,
            GitService::UploadPackExchange,
            "EVIL.invalid",
            2222,
            "/org/repo.git",
        ),
    );
    assert_refused(
        &w,
        &raw,
        2,
        raw.ssh_open(GitService::ReceivePackExchange, "/org/repo"),
    );
    let notes = w.receiver.take_refusals();
    assert_eq!(
        notes.iter().map(|n| n.text.as_str()).collect::<Vec<_>>(),
        [
            "the core asked to fetch from ssh://evil.invalid:2222/org/repo (upload-pack, port 2222), which this command does not allow",
            "the core asked to push to ssh://example.invalid:22/org/repo (receive-pack, port 22), which this command does not allow",
        ]
    );
    // The notes are bounded: a core that asks for a thousand refused hosts leaves the latest sixty-four.
    for stream in 3..1200 {
        raw.send(
            &w,
            raw.open_frame(
                stream,
                raw.open(
                    Scheme::Ssh,
                    GitService::UploadPackExchange,
                    "evil.invalid",
                    22,
                    "/org/repo",
                ),
            ),
        );
        let _ = raw.read(&w);
    }
    assert_eq!(w.receiver.take_refusals().len(), 64);
}

#[test]
fn a_bind_for_a_request_whose_class_admits_no_transport_is_rejected_and_makes_no_binding() {
    let w = World::new();
    w.call_with(
        "call-1",
        invocation(ServiceClass::NoTransport, &[], &[]),
        None,
    );
    let raw = Raw::bind(&w, "call-1", SESSION);
    w.settle_client();
    let replies = raw.read(&w);
    let rejections = rejected(&replies);
    assert_eq!(rejections.len(), 1);
    assert_eq!(
        rejections[0].bind_rejected.as_ref().unwrap().code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(w.host.snapshot().bindings, 0);
}

#[test]
fn bound_advertises_the_clients_policies_not_the_cores_offer() {
    let w = World::new();
    w.call_with(
        "call-1",
        Invocation::new(super::world::ssh_only(), ServiceClass::Fetch),
        None,
    );
    let raw = Raw::bind(&w, "call-1", SESSION);
    w.settle_client();
    let replies = raw.read(&w);
    let bound = of_kind(&replies, MessageKind::Bound);
    assert_eq!(bound.len(), 1);
    let policies = &bound[0].bound.as_ref().unwrap().policies;
    assert!(
        policies.contains(&AuthPolicy::SshAmbient) && policies.contains(&AuthPolicy::SshExplicit)
    );
    assert!(
        !policies.contains(&AuthPolicy::Gh)
            && !policies.contains(&AuthPolicy::WindowsDefault)
            && !policies.contains(&AuthPolicy::Anonymous)
    );
    assert_eq!(bound[0].bound.as_ref().unwrap().schemes, vec![Scheme::Ssh]);
}

#[test]
fn a_fetch_class_request_admits_upload_pack_for_an_allowed_tuple_only() {
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(
            ServiceClass::Fetch,
            &["ssh://example.invalid/org/"],
            &["ssh://example.invalid/"],
        ),
    );
    // receive-pack under a fetch-class request, even for a push-allowed tuple.
    assert_refused(
        &w,
        &raw,
        1,
        raw.ssh_open(GitService::ReceivePackExchange, "/org/repo"),
    );
    // outside the fetch allowance.
    assert_refused(
        &w,
        &raw,
        2,
        raw.ssh_open(GitService::UploadPackExchange, "/other/repo"),
    );
    // a core-chosen host.
    assert_refused(
        &w,
        &raw,
        3,
        raw.open(
            Scheme::Ssh,
            GitService::UploadPackExchange,
            "evil.invalid",
            22,
            "/org/repo",
        ),
    );
    // a port the allowance does not name.
    assert_refused(
        &w,
        &raw,
        4,
        raw.open(
            Scheme::Ssh,
            GitService::UploadPackExchange,
            "example.invalid",
            2222,
            "/org/repo",
        ),
    );
    // a path that escapes its prefix.
    assert_refused(
        &w,
        &raw,
        5,
        raw.ssh_open(GitService::UploadPackExchange, "/org/../other/repo"),
    );
    // and the allowed one opens.
    assert_admitted(
        &w,
        &raw,
        6,
        raw.ssh_open(GitService::UploadPackExchange, "/org/repo.git"),
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 1);
    assert_eq!(
        w.rig.world.last_spec.lock().unwrap().clone().unwrap().0,
        crate::GitService::UploadPackExchange
    );
}

#[test]
fn receive_pack_needs_a_push_class_request_and_the_push_allowance_and_a_push_allowance_is_not_a_fetch_allowance()
 {
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(
            ServiceClass::Push,
            &["ssh://example.invalid/read/"],
            &["ssh://example.invalid/write/repo"],
        ),
    );
    assert_refused(
        &w,
        &raw,
        1,
        raw.ssh_open(GitService::ReceivePackExchange, "/read/repo"),
    );
    assert_refused(
        &w,
        &raw,
        2,
        raw.ssh_open(GitService::UploadPackExchange, "/write/repo"),
    );
    assert_admitted(
        &w,
        &raw,
        3,
        raw.ssh_open(GitService::UploadPackExchange, "/read/repo"),
    );
    assert_admitted(
        &w,
        &raw,
        5,
        raw.ssh_open(GitService::ReceivePackExchange, "/write/repo"),
    );
    assert_eq!(
        w.rig.world.last_spec.lock().unwrap().clone().unwrap().0,
        crate::GitService::ReceivePackExchange
    );
}

#[test]
fn a_plan_class_request_refuses_every_open_even_for_a_push_allowed_tuple() {
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(
            ServiceClass::Plan,
            &["ssh://example.invalid/"],
            &["ssh://example.invalid/"],
        ),
    );
    assert_refused(
        &w,
        &raw,
        1,
        raw.ssh_open(GitService::ReceivePackExchange, "/org/repo"),
    );
    assert_refused(
        &w,
        &raw,
        2,
        raw.ssh_open(GitService::UploadPackExchange, "/org/repo"),
    );
    assert_eq!(
        w.rig.world.exchanges.load(Ordering::SeqCst),
        0,
        "the fixture remote is unchanged"
    );
}

#[test]
fn an_identity_is_admitted_only_when_the_invocation_named_it() {
    let w = World::new();
    let mut named = invocation(ServiceClass::Fetch, &["ssh://example.invalid/"], &[]);
    named.named_keys = vec![key("/home/me/.ssh/id_work")];
    named.path_base = Some("/home/me".into());
    let raw = started(&w, "call-1", SESSION, named);
    let explicit = |raw: &Raw, path: Option<&str>, base: Option<&str>| {
        let mut open = raw.ssh_open(GitService::UploadPackExchange, "/org/repo");
        open.identity = Identity {
            mode: IdentityMode::ExplicitKey,
            key_path: path.map(str::to_owned),
            path_base: base.map(str::to_owned),
        };
        open.policy = AuthPolicy::SshExplicit;
        open
    };
    // A core-named key path, a wrong path base, and ambient when the invocation named a key: all refused.
    assert_refused(
        &w,
        &raw,
        1,
        explicit(&raw, Some("/home/me/.ssh/id_other"), Some("/home/me")),
    );
    assert_refused(
        &w,
        &raw,
        2,
        explicit(&raw, Some("/home/me/.ssh/id_work"), Some("/elsewhere")),
    );
    assert_refused(
        &w,
        &raw,
        3,
        explicit(&raw, Some("/home/me/.ssh/id_work"), None),
    );
    assert_refused(
        &w,
        &raw,
        4,
        raw.ssh_open(GitService::UploadPackExchange, "/org/repo"),
    );
    assert_admitted(
        &w,
        &raw,
        5,
        explicit(&raw, Some("/home/me/.ssh/id_work"), Some("/home/me")),
    );
    // The proof the owner saw is the client's, not the path.
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 1);
}

#[test]
fn a_core_supplied_username_is_admitted_only_when_equal_to_the_clients_choice() {
    let w = World::new();
    let raw = started(
        &w,
        "call-1",
        SESSION,
        invocation(
            ServiceClass::Fetch,
            &[
                "ssh://deploy@example.invalid/",
                "https://tok@example.invalid/",
            ],
            &[],
        ),
    );
    let ssh = |raw: &Raw, user: &str| {
        let mut open = raw.ssh_open(GitService::UploadPackExchange, "/org/repo");
        open.destination.ssh_username = Some(user.to_owned());
        open
    };
    let https = |raw: &Raw, user: Option<&str>| {
        let mut open = raw.open(
            Scheme::Https,
            GitService::UploadPackAdvertisement,
            "example.invalid",
            443,
            "/org/repo",
        );
        open.destination.https_username = user.map(str::to_owned);
        open
    };
    // The allowance names `deploy`: the core's `root` and `git` are not the client's choice.
    assert_refused(&w, &raw, 1, ssh(&raw, "root"));
    assert_refused(&w, &raw, 2, ssh(&raw, "git"));
    assert_refused(&w, &raw, 3, https(&raw, Some("other")));
    // An SSH `Open` always carries a username on the wire (the codec refuses one without), so "absent" is HTTPS only.
    assert_admitted(&w, &raw, 4, ssh(&raw, "deploy"));
    assert_admitted(&w, &raw, 5, https(&raw, Some("tok")));
    assert_admitted(&w, &raw, 6, https(&raw, None));
}

#[test]
fn an_open_that_breaks_the_transports_rules_faults_that_binding_only() {
    let w = World::new();
    let ok = started(
        &w,
        "call-ok",
        "00000000000000000000000000000c0f.1",
        invocation(
            ServiceClass::Fetch,
            &["ssh://example.invalid/", "https://example.invalid/"],
            &[],
        ),
    );
    let bad = started(
        &w,
        "call-bad",
        "00000000000000000000000000000c0f.2",
        invocation(
            ServiceClass::Fetch,
            &["ssh://example.invalid/", "https://example.invalid/"],
            &[],
        ),
    );
    // An SSH advertisement: no row in the table (a fault, not an admission refusal).
    bad.send(
        &w,
        bad.open_frame(
            1,
            bad.ssh_open(GitService::UploadPackAdvertisement, "/org/repo"),
        ),
    );
    let replies = bad.read(&w);
    let failed = of_kind(&replies, MessageKind::OpenFailed);
    assert_eq!(failed.len(), 1, "{replies:?}");
    assert_eq!(
        failed[0].open_failed.as_ref().unwrap().code,
        ErrorCode::Protocol
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 0);
    assert_eq!(
        w.receiver.bindings(),
        1,
        "the faulted binding is gone; the other stays"
    );
    // The other binding carries on.
    assert_admitted(
        &w,
        &ok,
        1,
        ok.ssh_open(GitService::UploadPackExchange, "/org/repo"),
    );
    // An open whose policy is outside the set the client advertised is a fault of its binding too.
    let other = started(
        &w,
        "call-3",
        "00000000000000000000000000000c0f.3",
        Invocation::new(super::world::ssh_only(), ServiceClass::Fetch),
    );
    let mut foreign = other.open(
        Scheme::Https,
        GitService::UploadPackExchange,
        "example.invalid",
        443,
        "/org/repo",
    );
    foreign.policy = AuthPolicy::WindowsDefault;
    other.send(&w, other.open_frame(1, foreign));
    let failed = of_kind(&other.read(&w), MessageKind::OpenFailed).len();
    assert_eq!(failed, 1);
    assert_eq!(w.receiver.bindings(), 1);
}

#[test]
fn check_identity_for_a_core_named_key_path_is_refused_and_a_named_one_is_not_performed_here() {
    let w = World::new();
    let mut named = invocation(ServiceClass::Fetch, &["ssh://example.invalid/"], &[]);
    named.named_keys = vec![key("/home/me/.ssh/id_work")];
    named.path_base = Some("/home/me".into());
    let raw = started(&w, "call-1", SESSION, named);
    let check = |raw: &Raw, stream: i64, path: &str, base: &str| -> Attachment {
        let envelope = Envelope {
            version: 2,
            session_id: raw.session.clone(),
            stream_id: stream,
            kind: MessageKind::CheckIdentity,
            check_identity: Some(gwz_transport::protocol::CheckIdentity {
                endpoint_id: raw.endpoint_id(),
                operation_id: raw.request.clone(),
                identity: Identity {
                    mode: IdentityMode::ExplicitKey,
                    key_path: Some(path.to_owned()),
                    path_base: Some(base.to_owned()),
                },
                timeout_ms: 1000,
            }),
            ..Default::default()
        };
        (raw.request.clone(), envelope)
    };
    raw.send(&w, check(&raw, 1, "/home/me/.ssh/id_other", "/home/me"));
    let replies = raw.read(&w);
    let failed = of_kind(&replies, MessageKind::IdentityCheckFailed);
    assert_eq!(failed.len(), 1, "{replies:?}");
    let failure = failed[0].identity_check_failed.as_ref().unwrap();
    assert_eq!(
        (failure.code, failure.effect),
        (ErrorCode::InvalidRequest, Effect::None)
    );
    // A named key is admitted; the check itself is the SSH endpoint's work (step 2.3), so it answers unavailable.
    raw.send(&w, check(&raw, 2, "/home/me/.ssh/id_work", "/home/me"));
    let replies = raw.read(&w);
    let failed = of_kind(&replies, MessageKind::IdentityCheckFailed);
    assert_eq!(
        failed[0].identity_check_failed.as_ref().unwrap().code,
        ErrorCode::Unavailable
    );
    assert_eq!(w.rig.world.connects(), 0);
}

#[test]
fn a_frame_for_a_binding_that_does_not_exist_is_discarded_and_makes_no_state() {
    let w = World::new();
    let live = started(
        &w,
        "call-1",
        SESSION,
        invocation(ServiceClass::Fetch, &["ssh://example.invalid/"], &[]),
    );
    let stray = Envelope {
        version: 2,
        session_id: "00000000000000000000000000000c0f.99".into(),
        stream_id: 1,
        kind: MessageKind::Data,
        data: Some(gwz_transport::protocol::Data {
            offset: 0,
            payload: vec![1],
        }),
        ..Default::default()
    };
    let discarded = w.receiver.discarded();
    w.carrier.inject(Way::ToClient, ("call-1".into(), stray));
    w.settle_client();
    assert_eq!(w.receiver.discarded(), discarded + 1);
    assert_eq!(w.receiver.bindings(), 1);
    assert!(
        live.read(&w).is_empty(),
        "no reply to a frame that belongs to nothing"
    );
    assert_admitted(
        &w,
        &live,
        1,
        live.ssh_open(GitService::UploadPackExchange, "/org/repo"),
    );
}

#[test]
fn the_receiver_keeps_its_own_record_of_bound_calls_whatever_the_invocation_source_yields() {
    let w = World::new();
    w.calls.persistent.store(true, Ordering::SeqCst);
    let _first = started(
        &w,
        "call-1",
        SESSION,
        invocation(ServiceClass::Fetch, &[], &[]),
    );
    let second = Raw::bind(&w, "call-1", "00000000000000000000000000000c0f.3");
    w.settle_client();
    let replies = second.read(&w);
    let rejections = rejected(&replies);
    assert_eq!(rejections.len(), 1, "{replies:?}");
    assert_eq!(
        rejections[0].bind_rejected.as_ref().unwrap().code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(w.receiver.bindings(), 1);
}
