use crate::error::{DiceError, Result};

pub const DEFAULT_ZSTD_LEVEL: i32 = 1;

pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| DiceError::msg(format!("zstd compress: {e}")))
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| DiceError::msg(format!("zstd decompress: {e}")))
}
