//! The remote module against fakes only: a pair of in-memory lane ends, the crate's fake factory and executor, no
//! file, socket or thread (step 2.2c).
mod cancel_rows;
mod canonical;
mod carried_rows;
mod core_rows;
mod flow;
mod receiver_rows;
mod shared_rows;
mod state_rows;
mod world;
