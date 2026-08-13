//! Database layer: account/database discovery, live-reader connection
//! management, and SQLCipher opening (through the offset VFS).

pub mod decrypt;
pub mod live;
pub mod scan;
pub mod vfs;
