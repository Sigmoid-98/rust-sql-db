pub mod keycode;

use serde::{Deserialize, Serialize};

pub trait Key<'de>: Serialize + Deserialize<'de> {
    fn decode(bytes: &'de [u8]) -> Result<Self> {
        key
    }
}