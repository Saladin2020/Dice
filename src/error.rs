use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiceError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = std::result::Result<T, DiceError>;

impl DiceError {
    pub fn msg(m: impl Into<String>) -> Self {
        Self::Msg(m.into())
    }
}
