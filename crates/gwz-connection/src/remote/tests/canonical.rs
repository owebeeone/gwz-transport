//! Section 5.2's canonical form and section 5.3's class rule, as pure functions.
use super::super::{
    Allowance, AllowedSet, CanonError, Canonical, Refusal, ServiceClass, admit_service,
};
use gwz_transport::protocol::{Destination, GitService, Scheme};

fn destination(scheme: Scheme, host: &str, port: i64, path: &str) -> Destination {
    Destination {
        scheme,
        host: host.into(),
        port,
        path: path.into(),
        ssh_username: None,
        https_username: None,
    }
}
fn ssh(path: &str) -> Result<Canonical, CanonError> {
    Canonical::from_destination(&destination(Scheme::Ssh, "git.example", 22, path))
}
fn https(path: &str) -> Result<Canonical, CanonError> {
    Canonical::from_destination(&destination(Scheme::Https, "git.example", 443, path))
}
fn allowed(allowance: &str, tuple: &Result<Canonical, CanonError>) -> bool {
    Allowance::parse(allowance)
        .unwrap()
        .matches(tuple.as_ref().unwrap())
}

#[test]
fn dot_and_dotdot_and_empty_segments_are_refused() {
    for path in [
        "/a/./b", "/a/../b", "/a//b", "/../a", "/a/.", "/a/..", "//a",
    ] {
        assert_eq!(ssh(path), Err(CanonError::Path), "{path}");
        assert_eq!(https(path), Err(CanonError::Path), "{path}");
    }
    assert_eq!(ssh("/"), Err(CanonError::Path));
}

#[test]
fn percent_encoded_separators_and_dots_are_refused_and_other_escapes_compare_uppercase_undecoded() {
    for path in [
        "/a%2Fb",
        "/a%2fb",
        "/a%5Cb",
        "/a%2Eb",
        "/a/%2e%2e/b",
        "/a%00b",
        "/a%2",
    ] {
        assert_eq!(ssh(path), Err(CanonError::Path), "{path}");
    }
    assert_eq!(ssh("/a%7ec"), ssh("/a%7Ec"));
    assert_ne!(ssh("/a%7Ec"), ssh("/a~c"), "an escape is never decoded");
    assert_eq!(ssh("/a\\b"), Err(CanonError::Path));
    assert_eq!(ssh("/a\u{7}b"), Err(CanonError::Path));
}

#[test]
fn tilde_marks_home_relative_and_tilde_user_is_refused() {
    assert!(allowed("ssh://git.example/~/repo", &ssh("~/repo")));
    assert!(allowed("ssh://git.example/~/repo", &ssh("/~/repo")));
    assert!(
        !allowed("ssh://git.example/repo", &ssh("~/repo")),
        "home-relative is not absolute"
    );
    assert!(!allowed("ssh://git.example/~/repo", &ssh("/repo")));
    assert_eq!(ssh("~alice/repo"), Err(CanonError::Home));
    assert_eq!(ssh("~"), Err(CanonError::Home));
    // For HTTPS a tilde is an ordinary character.
    assert!(allowed(
        "https://git.example/~alice/repo",
        &https("/~alice/repo")
    ));
}

#[test]
fn scp_relative_is_home_relative_except_on_gwz_known_hosts() {
    // `host:owner/repo` is `~/owner/repo` and matches only a home-relative allowance.
    assert!(allowed("git.example:owner/repo", &ssh("owner/repo")));
    assert!(allowed(
        "ssh://git.example/~/owner/repo",
        &ssh("owner/repo")
    ));
    assert!(!allowed("ssh://git.example/owner/repo", &ssh("owner/repo")));
    // On a known host the SCP operand is the absolute path, one repository with the `ssh://` form.
    let known =
        Canonical::from_destination(&destination(Scheme::Ssh, "github.com", 22, "owner/repo"));
    assert!(allowed("git@github.com:owner/repo", &known));
    assert!(allowed("ssh://git@github.com/owner/repo", &known));
    let other =
        Canonical::from_destination(&destination(Scheme::Ssh, "git.example", 22, "owner/repo"));
    assert!(!allowed("ssh://git@git.example/owner/repo", &other));
}

#[test]
fn one_trailing_dot_git_and_one_trailing_slash_are_removed() {
    assert_eq!(ssh("/a/repo.git"), ssh("/a/repo"));
    assert_eq!(ssh("/a/repo/"), ssh("/a/repo"));
    assert_eq!(ssh("/a/repo.git/"), ssh("/a/repo"));
    assert_ne!(
        ssh("/a/repo.git.git"),
        ssh("/a/repo"),
        "only one .git comes off"
    );
    assert!(allowed("ssh://git.example/a/repo.git", &ssh("/a/repo")));
    assert!(allowed("https://git.example/a/repo", &https("/a/repo.git")));
    assert_eq!(ssh("/a/.git"), Err(CanonError::Path));
}

#[test]
fn case_is_compared_exactly_in_the_path_and_not_in_the_host() {
    assert!(allowed(
        "https://GIT.Example/Owner/Repo",
        &https("/Owner/Repo")
    ));
    assert!(!allowed(
        "https://git.example/owner/repo",
        &https("/Owner/Repo")
    ));
    assert!(!allowed(
        "ssh://git.example/owner/repo",
        &ssh("/Owner/Repo")
    ));
    let upper = Canonical::from_destination(&destination(Scheme::Https, "GIT.EXAMPLE.", 443, "/a"));
    assert!(
        allowed("https://git.example/a", &upper),
        "host lowercased, trailing dot removed"
    );
}

#[test]
fn exact_allowances_match_one_repository_and_prefix_allowances_need_a_further_segment() {
    assert!(allowed("ssh://git.example/org/repo", &ssh("/org/repo")));
    assert!(!allowed(
        "ssh://git.example/org/repo",
        &ssh("/org/repo/sub")
    ));
    assert!(!allowed("ssh://git.example/org/repo", &ssh("/org")));
    assert!(allowed("ssh://git.example/org/", &ssh("/org/repo")));
    assert!(allowed("ssh://git.example/org/", &ssh("/org/a/b")));
    assert!(
        !allowed("ssh://git.example/org/", &ssh("/org")),
        "a prefix needs one more segment"
    );
    assert!(!allowed("ssh://git.example/org/", &ssh("/other/repo")));
    assert!(
        !allowed("ssh://git.example/org/", &ssh("/organ/repo")),
        "a segment, not a string prefix"
    );
    // `ssh://host/` is a host-only grant: every path on that host and port.
    assert!(allowed("ssh://git.example/", &ssh("/anything/at/all")));
    assert!(allowed("ssh://git.example/", &ssh("~/home")));
    assert!(allowed("https://git.example/", &https("/x")));
    assert!(!allowed(
        "https://git.example/",
        &Canonical::from_destination(&destination(Scheme::Https, "other.example", 443, "/x"))
    ));
}

#[test]
fn the_port_must_match_and_defaults_by_scheme() {
    assert!(allowed("ssh://git.example/r", &ssh("/r")));
    assert!(allowed("ssh://git.example:22/r", &ssh("/r")));
    assert!(
        !allowed("ssh://git.example:2222/r", &ssh("/r")),
        "no prompt grants a non-default port"
    );
    let odd = Canonical::from_destination(&destination(Scheme::Ssh, "git.example", 2222, "/r"));
    assert!(!allowed("ssh://git.example/r", &odd));
    assert!(allowed("ssh://git.example:2222/r", &odd));
    assert!(allowed("https://git.example/r", &https("/r")));
    assert!(!allowed("https://git.example:8443/r", &https("/r")));
    assert!(
        Canonical::from_destination(&destination(Scheme::Ssh, "git.example", 0, "/r")).is_err()
    );
    assert!(
        Canonical::from_destination(&destination(Scheme::Ssh, "git.example", 70000, "/r")).is_err()
    );
}

#[test]
fn schemes_and_the_scheme_must_match() {
    for scheme in ["ssh", "git+ssh", "ssh+git"] {
        assert!(
            allowed(&format!("{scheme}://git.example/r"), &ssh("/r")),
            "{scheme}"
        );
    }
    assert_eq!(
        Allowance::parse("http://git.example/r"),
        Err(CanonError::Scheme)
    );
    assert_eq!(
        Allowance::parse("ftp://git.example/r"),
        Err(CanonError::Scheme)
    );
    assert!(!allowed("https://git.example/r", &ssh("/r")));
    assert!(!allowed("ssh://git.example/r", &https("/r")));
}

#[test]
fn hosts_are_ascii_ldh_ipv4_canonical_and_ipv6_in_rfc5952() {
    let host = |h: &str| Canonical::from_destination(&destination(Scheme::Ssh, h, 22, "/r"));
    assert!(host("git.example").is_ok());
    for bad in [
        "",
        "-a.example",
        "a-.example",
        "a..example",
        "a_b.example",
        "ex ample",
        "a.example..",
        "fe80::1%eth0",
    ] {
        assert_eq!(host(bad), Err(CanonError::Host), "{bad:?}");
    }
    // A non-ASCII host is refused in a category of its own: this release maps no IDNA (UTS-46) host names, so there is
    // no punycode form to print (the refusal names the host and the reason, `receiver_rows`).
    assert_eq!(host("g\u{e9}t.example"), Err(CanonError::NonAsciiHost));
    assert_eq!(host("\u{4f8b}.invalid"), Err(CanonError::NonAsciiHost));
    assert!(host("127.0.0.1").is_ok());
    for bad in [
        "127.1",
        "0x7f.0.0.1",
        "127.000.000.001",
        "1.2.3",
        "999.1.1.1",
        "1.2.3.4.5",
    ] {
        assert_eq!(host(bad), Err(CanonError::Host), "{bad:?}");
    }
    assert_eq!(host("[2001:DB8:0:0:0:0:0:1]"), host("2001:db8::1"));
    assert!(allowed(
        "ssh://[2001:db8::1]/r",
        &host("2001:0db8:0000:0000:0000:0000:0000:0001")
    ));
    assert!(allowed("ssh://[2001:db8::1]:22/r", &host("[2001:db8::1]")));
}

#[test]
fn allowance_syntax_errors_are_refused() {
    for text in [
        "",
        "ssh://",
        "ssh://host",
        "ssh://host?x=1/r",
        "ssh://host/r#frag",
        "ssh://:22/r",
        "ssh://host:0/r",
        "ssh://host:99999/r",
        "ssh://@host/r",
        "ssh://host/a/../b",
    ] {
        assert!(Allowance::parse(text).is_err(), "{text:?}");
    }
    assert_eq!(
        Allowance::parse("ssh://git@git.example/r").unwrap().user(),
        Some("git")
    );
    assert_eq!(Allowance::parse("git.example:r").unwrap().user(), None);
}

#[test]
fn a_fetch_class_request_admits_upload_pack_only() {
    let tuple = ssh("/org/repo").unwrap();
    let fetch = AllowedSet::parse(&["ssh://git.example/org/"]).unwrap();
    let push = AllowedSet::parse(&["ssh://git.example/org/"]).unwrap();
    for service in [
        GitService::UploadPackAdvertisement,
        GitService::UploadPackExchange,
    ] {
        assert!(admit_service(ServiceClass::Fetch, service, &tuple, &fetch, &push).is_ok());
    }
    for service in [
        GitService::ReceivePackAdvertisement,
        GitService::ReceivePackExchange,
    ] {
        assert_eq!(
            admit_service(ServiceClass::Fetch, service, &tuple, &fetch, &push),
            Err(Refusal::ClassForbids),
            "even a push-allowed tuple"
        );
    }
}

#[test]
fn receive_pack_needs_the_push_allowance_and_a_push_class_and_does_not_imply_fetch() {
    let tuple = ssh("/org/repo").unwrap();
    let fetch = AllowedSet::parse(&["ssh://git.example/org/"]).unwrap();
    let push = AllowedSet::parse(&["ssh://git.example/org/repo"]).unwrap();
    let none = AllowedSet::default();
    assert!(
        admit_service(
            ServiceClass::Push,
            GitService::ReceivePackExchange,
            &tuple,
            &fetch,
            &push
        )
        .is_ok()
    );
    assert_eq!(
        admit_service(
            ServiceClass::Push,
            GitService::ReceivePackExchange,
            &tuple,
            &fetch,
            &none
        ),
        Err(Refusal::PushNotAllowed),
        "the fetch allowance does not grant a push"
    );
    assert_eq!(
        admit_service(
            ServiceClass::Push,
            GitService::UploadPackExchange,
            &tuple,
            &none,
            &push
        ),
        Err(Refusal::NotAllowed),
        "a push allowance does not imply the fetch allowance"
    );
    assert!(
        admit_service(
            ServiceClass::Push,
            GitService::UploadPackAdvertisement,
            &tuple,
            &fetch,
            &push
        )
        .is_ok()
    );
}

#[test]
fn a_plan_class_request_and_a_class_with_no_transport_admit_no_open_even_for_an_allowed_tuple() {
    let tuple = ssh("/org/repo").unwrap();
    let set = AllowedSet::parse(&["ssh://git.example/"]).unwrap();
    for service in [
        GitService::UploadPackExchange,
        GitService::ReceivePackExchange,
        GitService::ReceivePackAdvertisement,
    ] {
        assert_eq!(
            admit_service(ServiceClass::Plan, service, &tuple, &set, &set),
            Err(Refusal::Plan)
        );
        assert_eq!(
            admit_service(ServiceClass::NoTransport, service, &tuple, &set, &set),
            Err(Refusal::NoTransport)
        );
    }
}

#[test]
fn the_tuple_prints_in_canonical_form_without_control_characters() {
    let tuple = Canonical::from_destination(&destination(
        Scheme::Ssh,
        "GIT.example",
        2222,
        "/org/repo.git",
    ))
    .unwrap();
    assert_eq!(tuple.to_string(), "ssh://git.example:2222/org/repo");
    let home = ssh("~/org/repo").unwrap();
    assert_eq!(home.to_string(), "ssh://git.example:22/~/org/repo");
    assert_eq!(
        Canonical::from_destination(&destination(Scheme::Ssh, "[::1]", 22, "/r"))
            .unwrap()
            .to_string(),
        "ssh://[::1]:22/r"
    );
}
