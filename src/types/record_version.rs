use crate::TitoError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TitoRecordVersion(u64);

impl TitoRecordVersion {
    pub(crate) fn from_transaction(version: u64) -> Result<Self, TitoError> {
        if version == 0 {
            return Err(TitoError::InvalidInput(
                "Record writes require a positive engine transaction version".into(),
            ));
        }
        Ok(Self(version))
    }
}

impl std::fmt::Display for TitoRecordVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "tito:v1:{:016x}", self.0)
    }
}

impl From<TitoRecordVersion> for String {
    fn from(value: TitoRecordVersion) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for TitoRecordVersion {
    type Error = TitoError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl std::str::FromStr for TitoRecordVersion {
    type Err = TitoError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || TitoError::InvalidInput("Invalid opaque Tito record version".into());
        let digits = value.strip_prefix("tito:v1:").ok_or_else(invalid)?;
        if digits.len() != 16
            || !digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid());
        }
        Self::from_transaction(u64::from_str_radix(digits, 16).map_err(|_| invalid())?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TitoVersioned<T> {
    pub value: T,
    pub version: TitoRecordVersion,
}
