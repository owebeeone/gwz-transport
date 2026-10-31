//! What the endpoint instance's rows need to reach (movement from `instance.rs`).

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use super::Instance;
        use crate::AuthScope;

        impl Instance {
            /// Makes every connection the instance holds now one set up under `scope`, as if another operation's
            /// context had set it up: what a test needs to offer an opening only connections that do not fit.
            pub(crate) fn rescope_for_test(&self, scope: AuthScope) {
                for setup in self.lock().connections.values_mut() {
                    setup.scope = Some(scope);
                }
            }
            /// Leases of another scope this instance has refused.
            pub(crate) fn misfits_for_test(&self) -> u64 {
                self.lock().misfits
            }
            /// Pool requests that are still live.
            pub(crate) fn outstanding_for_test(&self) -> usize {
                self.lock().pool.outstanding_requests()
            }
        }
    }
}
