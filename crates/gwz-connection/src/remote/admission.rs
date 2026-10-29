//! The client's admission of a core-named destination (GwzSplitCommandChannel section 5.2 and 5.3): the tuple's
//! canonical form, the allowance it is matched against, and the service-class rule.
//!
//! Everything here is a pure function of its inputs: no file, no network, no configuration source. The allowed sets
//! (section 5.6) are supplied by the composition; this step implements the match.
//!
//! Known limit (reported at step 2.2c): the host is canonicalized as ASCII LDH only (lowercased, one trailing dot
//! removed); a non-ASCII host is refused rather than mapped through UTS-46, because that mapping needs the `idna`
//! crate and `gwz-connection`'s manifest takes no dependency beyond `gwz-transport` (contract section 3).
use gwz_transport::protocol::{Destination, GitService, Scheme};
use std::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr},
};

/// The hosts on which gwz already treats `git@HOST:o/r` and `ssh://git@HOST/o/r` as one repository
/// (`git/url_scheme.rs:372`): an SCP-relative operand on one of them is absolute.
const KNOWN_HOSTS: [&str; 3] = ["github.com", "gitlab.com", "bitbucket.org"];
const MAX_PATH_BYTES: usize = 16 * 1024;

/// What a request is allowed to do over the carrier, by the client's own request type (section 4.5), never the core's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceClass {
    /// Upload-pack only, against the fetch allowance.
    Fetch,
    /// Upload-pack against the fetch allowance; receive-pack against the separate push allowance.
    Push,
    /// A push-class request type with `dry_run`: `bind` and `check_identity` only, every `open` refused.
    Plan,
    /// A request type whose class admits no `bind` at all.
    NoTransport,
}

/// Why a destination or an allowance is not in canonical form (section 5.2). The refusal names a category, never the
/// offending text, so a log line carries no core-chosen string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonError {
    Scheme,
    Host,
    /// A host with a non-ASCII character. The mapping to punycode (IDNA, UTS-46) needs a dependency this crate does not
    /// take in this release, so such a host is refused, not mapped.
    NonAsciiHost,
    Port,
    /// A `.` or `..` segment, an empty segment, `\`, NUL, a control character, or a percent-encoded `/`, `\`, `.` or
    /// NUL.
    Path,
    /// `~user`, a bare `~`, or a home-relative form on a scheme that has none.
    Home,
    /// An allowance that is not `scheme://[user@]host[:port]/path[/]` or the SCP form.
    Syntax,
}
impl fmt::Display for CanonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not a canonical tuple: {self:?}")
    }
}
impl std::error::Error for CanonError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Base {
    Absolute,
    Home,
}

/// A canonical destination: scheme, host, numeric port and path (section 5.2). Equal tuples compare equal whatever the
/// spelling that produced them.
#[derive(Clone, Debug, PartialEq)]
pub struct Canonical {
    scheme: Scheme,
    host: String,
    port: u16,
    base: Base,
    segments: Vec<String>,
}
impl Canonical {
    /// Canonicalizes an `Open`'s destination: the operand actually sent, never decoded.
    pub fn from_destination(destination: &Destination) -> Result<Self, CanonError> {
        let port = u16::try_from(destination.port)
            .ok()
            .filter(|port| *port != 0)
            .ok_or(CanonError::Port)?;
        let host = canonical_host(&destination.host)?;
        let (base, segments) = canonical_path(&destination.path, destination.scheme, &host, false)?;
        let (base, segments) = match (base, segments) {
            (Some(base), segments) if !segments.is_empty() => (base, segments),
            _ => return Err(CanonError::Path),
        };
        Ok(Self {
            scheme: destination.scheme,
            host,
            port,
            base,
            segments,
        })
    }
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}
impl fmt::Display for Canonical {
    /// The tuple as the refusal text prints it: control characters cannot occur (they were refused) and the host is
    /// punycode or ASCII only.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = match self.scheme {
            Scheme::Ssh => "ssh",
            Scheme::Https => "https",
        };
        let home = if self.base == Base::Home { "~/" } else { "" };
        write!(
            f,
            "{scheme}://{}:{}/{home}{}",
            bracketed(&self.host),
            self.port,
            self.segments.join("/")
        )
    }
}

/// One entry of an allowed set: an exact repository, a prefix, or a whole host (section 5.2 "Match").
#[derive(Clone, Debug, PartialEq)]
pub struct Allowance {
    scheme: Scheme,
    user: Option<String>,
    host: String,
    port: u16,
    /// `None` only for a host-only grant, which matches any path on the host and port.
    base: Option<Base>,
    segments: Vec<String>,
    prefix: bool,
}
impl Allowance {
    /// Parses `scheme://[user@]host[:port]/path` (exact), `…/path/` (prefix), or the SCP form `[user@]host:path[/]`.
    pub fn parse(text: &str) -> Result<Self, CanonError> {
        if text.is_empty() || text.len() > MAX_PATH_BYTES || text.contains(['?', '#']) {
            return Err(CanonError::Syntax);
        }
        let (scheme, authority, path) = match text.split_once("://") {
            Some((scheme, rest)) => {
                let scheme = match scheme.to_ascii_lowercase().as_str() {
                    "ssh" | "git+ssh" | "ssh+git" => Scheme::Ssh,
                    "https" => Scheme::Https,
                    _ => return Err(CanonError::Scheme),
                };
                let slash = rest.find('/').ok_or(CanonError::Syntax)?;
                (scheme, &rest[..slash], &rest[slash..])
            }
            None => {
                let (authority, path) = scp_split(text)?;
                (Scheme::Ssh, authority, path)
            }
        };
        let (user, host, port) = split_authority(authority, scheme)?;
        let host = canonical_host(&host)?;
        let prefix = path.ends_with('/');
        let (base, segments) = canonical_path(path, scheme, &host, true)?;
        if base.is_some() && segments.is_empty() && !prefix {
            return Err(CanonError::Path);
        }
        Ok(Self {
            scheme,
            user,
            host,
            port,
            base,
            segments,
            prefix,
        })
    }
    /// The `user@` this allowance names, which is the username the client uses for it (section 5.4).
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }
    /// Whether `tuple` is inside this allowance. Scheme, host and port always compare; the port must match.
    pub fn matches(&self, tuple: &Canonical) -> bool {
        if self.scheme != tuple.scheme || self.host != tuple.host || self.port != tuple.port {
            return false;
        }
        let Some(base) = self.base else {
            return true;
        };
        if base != tuple.base {
            return false;
        }
        if self.prefix {
            tuple.segments.len() > self.segments.len()
                && tuple.segments[..self.segments.len()] == self.segments[..]
        } else {
            tuple.segments == self.segments
        }
    }
}

/// The allowances one invocation supplies (a fake set here; section 5.6 names the sources).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AllowedSet {
    entries: Vec<Allowance>,
}
impl AllowedSet {
    pub fn new(entries: Vec<Allowance>) -> Self {
        Self { entries }
    }
    pub fn parse(texts: &[&str]) -> Result<Self, CanonError> {
        texts
            .iter()
            .map(|text| Allowance::parse(text))
            .collect::<Result<Vec<_>, _>>()
            .map(Self::new)
    }
    /// The first allowance that admits `tuple`.
    pub fn admitting(&self, tuple: &Canonical) -> Option<&Allowance> {
        self.entries.iter().find(|entry| entry.matches(tuple))
    }
}

/// Why an `open` was refused by section 5.3; the receiver answers every one the same way (`open_failed`,
/// `invalid_request`, `Effect::None`, no facts), so the core never learns which.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A plan-class request admits no `open`, even for a push-allowed tuple.
    Plan,
    /// The request's class admits no transport.
    NoTransport,
    /// Receive-pack under a fetch-class request.
    ClassForbids,
    /// Not in the fetch allowance.
    NotAllowed,
    /// Receive-pack for a tuple outside the push allowance.
    PushNotAllowed,
}

/// Section 5.3, returning the allowance that admitted the tuple (it names the username, section 5.4): the class is the client's own, fetch class admits upload-pack only, and receive-pack needs the push
/// allowance under a push-class request. A push allowance does not imply a fetch allowance.
pub fn admit_service<'a>(
    class: ServiceClass,
    service: GitService,
    tuple: &Canonical,
    fetch: &'a AllowedSet,
    push: &'a AllowedSet,
) -> Result<&'a Allowance, Refusal> {
    let receive_pack = matches!(
        service,
        GitService::ReceivePackAdvertisement | GitService::ReceivePackExchange
    );
    match class {
        ServiceClass::Plan => Err(Refusal::Plan),
        ServiceClass::NoTransport => Err(Refusal::NoTransport),
        ServiceClass::Fetch if receive_pack => Err(Refusal::ClassForbids),
        ServiceClass::Push if receive_pack => push.admitting(tuple).ok_or(Refusal::PushNotAllowed),
        ServiceClass::Fetch | ServiceClass::Push => {
            fetch.admitting(tuple).ok_or(Refusal::NotAllowed)
        }
    }
}

/// The text of a refused `open` for the command that prints it (section 5.6, without the leading `gwz: refused:` and
/// the flag that would allow it, which are the command's): what the core asked for, in words that carry nothing a
/// terminal would act on.
fn asked(service: GitService) -> (&'static str, &'static str) {
    match service {
        GitService::UploadPackAdvertisement | GitService::UploadPackExchange => {
            ("fetch from", "upload-pack")
        }
        GitService::ReceivePackAdvertisement | GitService::ReceivePackExchange => {
            ("push to", "receive-pack")
        }
    }
}
/// A destination that does not canonicalize. The one refusal that names the host as sent is the non-ASCII host, with
/// the reason, because there is no canonical (punycode) form to print: its characters outside ASCII graphic are shown
/// as `\u{..}` escapes.
pub(super) fn tuple_refusal_text(
    destination: &Destination,
    service: GitService,
    error: CanonError,
) -> String {
    let (verb, word) = asked(service);
    let port = destination.port;
    match error {
        CanonError::NonAsciiHost => format!(
            "the core asked to {verb} host \"{}\" ({word}, port {port}), which this command cannot admit: the host is \
             not ASCII, and this release maps no IDNA (UTS-46) host names",
            printable(&destination.host)
        ),
        error => format!(
            "the core asked to {verb} a destination that is not in canonical form ({error:?}), which this command \
             cannot admit"
        ),
    }
}
/// A canonical tuple outside what the request's class and allowed sets admit.
pub(super) fn class_refusal_text(tuple: &Canonical, service: GitService) -> String {
    let (verb, word) = asked(service);
    format!(
        "the core asked to {verb} {tuple} ({word}, port {}), which this command does not allow",
        tuple.port()
    )
}
/// `text` for a terminal: ASCII graphic characters as they are and every other character as a `\u{..}` escape (a
/// bidirectional override or a control character cannot spoof the line), at most 253 characters of it (the longest
/// host name).
fn printable(text: &str) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for c in text.chars().take(253) {
        if c.is_ascii_graphic() {
            out.push(c);
        } else {
            let _ = write!(out, "\\u{{{:x}}}", u32::from(c));
        }
    }
    out
}

fn bracketed(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

/// `[user@]host:path` for the SCP form: the first `:` before any `/`, outside an IPv6 bracket.
fn scp_split(text: &str) -> Result<(&str, &str), CanonError> {
    let search_from = if let Some(close) = text.find(']') {
        if !text.starts_with('[') && !text.contains("@[") {
            return Err(CanonError::Syntax);
        }
        close
    } else {
        0
    };
    let colon = text[search_from..]
        .find(':')
        .map(|at| at + search_from)
        .ok_or(CanonError::Syntax)?;
    if text[..colon].contains('/') {
        return Err(CanonError::Syntax);
    }
    Ok((&text[..colon], &text[colon + 1..]))
}

type Authority = (Option<String>, String, u16);
fn split_authority(authority: &str, scheme: Scheme) -> Result<Authority, CanonError> {
    let default = match scheme {
        Scheme::Ssh => 22,
        Scheme::Https => 443,
    };
    let (user, hostport) = match authority.rsplit_once('@') {
        Some((user, rest)) => {
            if user.is_empty()
                || user.contains(['/', '\\', '@'])
                || user.chars().any(char::is_control)
            {
                return Err(CanonError::Syntax);
            }
            (Some(user.to_owned()), rest)
        }
        None => (None, authority),
    };
    let (host, port) = if let Some(rest) = hostport.strip_prefix('[') {
        let close = rest.find(']').ok_or(CanonError::Host)?;
        let host = format!("[{}]", &rest[..close]);
        match &rest[close + 1..] {
            "" => (host, default),
            tail => (
                host,
                parse_port(tail.strip_prefix(':').ok_or(CanonError::Host)?)?,
            ),
        }
    } else {
        match hostport.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => (host.to_owned(), parse_port(port)?),
            Some(_) => return Err(CanonError::Host),
            None => (hostport.to_owned(), default),
        }
    };
    Ok((user, host, port))
}
fn parse_port(text: &str) -> Result<u16, CanonError> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) || text.len() > 5 {
        return Err(CanonError::Port);
    }
    text.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or(CanonError::Port)
}

/// ASCII LDH host, lowercased, no trailing dot; IPv4 only in canonical dotted decimal; IPv6 in RFC 5952 form with no
/// zone. Non-ASCII, empty and non-LDH labels are refused.
fn canonical_host(host: &str) -> Result<String, CanonError> {
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return v6(inner);
    }
    if host.contains(':') {
        return v6(host);
    }
    let host = host.strip_suffix('.').unwrap_or(host);
    if !host.is_ascii() {
        return Err(CanonError::NonAsciiHost);
    }
    if host.is_empty() || host.len() > 253 {
        return Err(CanonError::Host);
    }
    let lower = host.to_ascii_lowercase();
    let labels: Vec<&str> = lower.split('.').collect();
    for label in &labels {
        let bytes = label.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 63
            || bytes[0] == b'-'
            || bytes[bytes.len() - 1] == b'-'
            || !bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        {
            return Err(CanonError::Host);
        }
    }
    // A name whose last label is all digits is an IPv4 spelling: only the canonical dotted quad is one.
    let last_numeric = labels
        .last()
        .is_some_and(|label| label.bytes().all(|b| b.is_ascii_digit()));
    if last_numeric {
        let quad: Ipv4Addr = lower.parse().map_err(|_| CanonError::Host)?;
        if quad.to_string() != lower {
            return Err(CanonError::Host);
        }
    }
    Ok(lower)
}
fn v6(text: &str) -> Result<String, CanonError> {
    if text.contains('%') {
        return Err(CanonError::Host);
    }
    let address: Ipv6Addr = text.parse().map_err(|_| CanonError::Host)?;
    Ok(address.to_string())
}

/// The path's base and segments. `allowance` accepts the host-only grant (`/`, `~/`) as zero segments, and a path whose
/// base is `None` is that host-only grant on any base.
fn canonical_path(
    raw: &str,
    scheme: Scheme,
    host: &str,
    allowance: bool,
) -> Result<(Option<Base>, Vec<String>), CanonError> {
    if raw.len() > MAX_PATH_BYTES {
        return Err(CanonError::Path);
    }
    let raw = percent_normalized(raw)?;
    let (base, rest) = match scheme {
        Scheme::Https => match raw.strip_prefix('/') {
            Some(rest) => (Base::Absolute, rest.to_owned()),
            None => return Err(CanonError::Path),
        },
        Scheme::Ssh => {
            if let Some(rest) = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("/~/")) {
                (Base::Home, rest.to_owned())
            } else if raw.starts_with('~') || raw.starts_with("/~") {
                return Err(CanonError::Home);
            } else if let Some(rest) = raw.strip_prefix('/') {
                (Base::Absolute, rest.to_owned())
            } else if KNOWN_HOSTS.contains(&host) {
                (Base::Absolute, raw.clone())
            } else {
                (Base::Home, raw.clone())
            }
        }
    };
    let rest = rest.strip_suffix('/').unwrap_or(&rest);
    if rest.is_empty() {
        // `host:/`, `ssh://host/`, `https://host/`: a host-only grant on any base. `~/` alone is a home-wide grant.
        let host_only = allowance && (raw == "/" || raw == "~/" || raw == "/~/");
        return if host_only {
            Ok((if raw == "/" { None } else { Some(base) }, Vec::new()))
        } else {
            Err(CanonError::Path)
        };
    }
    let mut segments = Vec::new();
    for segment in rest.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(CanonError::Path);
        }
        segments.push(segment.to_owned());
    }
    if let Some(last) = segments.last_mut()
        && let Some(stem) = last.strip_suffix(".git")
    {
        if stem.is_empty() {
            return Err(CanonError::Path);
        }
        *last = stem.to_owned();
    }
    Ok((Some(base), segments))
}

/// Refuses `\`, NUL, control characters and the percent escapes of `/`, `\`, `.` and NUL; leaves other escapes
/// undecoded with their hex digits in uppercase.
fn percent_normalized(raw: &str) -> Result<String, CanonError> {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte == b'%' {
            let hex = raw.get(at + 1..at + 3).ok_or(CanonError::Path)?;
            let value = u8::from_str_radix(hex, 16).map_err(|_| CanonError::Path)?;
            if hex.bytes().any(|b| !b.is_ascii_hexdigit())
                || matches!(value, 0x00 | 0x2E | 0x2F | 0x5C)
            {
                return Err(CanonError::Path);
            }
            out.push('%');
            out.push_str(&hex.to_ascii_uppercase());
            at += 3;
            continue;
        }
        if byte == b'\\' || byte < 0x20 || byte == 0x7F {
            return Err(CanonError::Path);
        }
        let ch = raw[at..].chars().next().ok_or(CanonError::Path)?;
        out.push(ch);
        at += ch.len_utf8();
    }
    Ok(out)
}
