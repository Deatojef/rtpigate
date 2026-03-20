use std::fmt;

/// Central error type for the rtpigate application.
#[derive(Debug)]
pub enum RtpigateError {
    /// Network errors: socket operations, TCP connect, DNS resolution
    Network(String),

    /// IO errors: file read/write, telemetry persistence
    Io(std::io::Error),

    /// Parse errors: AX.25 frame, UTF-8, TOML, APRS packet decoding
    Parse(String),

    /// Configuration errors: missing or invalid config fields
    Config(String),

    /// Validation errors: coordinate ranges, packet flags, data integrity
    Validation(String),
}

impl fmt::Display for RtpigateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RtpigateError::Network(msg) => write!(f, "Network error: {}", msg),
            RtpigateError::Io(err) => write!(f, "IO error: {}", err),
            RtpigateError::Parse(msg) => write!(f, "Parse error: {}", msg),
            RtpigateError::Config(msg) => write!(f, "Config error: {}", msg),
            RtpigateError::Validation(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for RtpigateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RtpigateError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RtpigateError {
    fn from(err: std::io::Error) -> Self {
        RtpigateError::Io(err)
    }
}

impl From<toml::de::Error> for RtpigateError {
    fn from(err: toml::de::Error) -> Self {
        RtpigateError::Parse(err.to_string())
    }
}

impl From<std::num::ParseIntError> for RtpigateError {
    fn from(err: std::num::ParseIntError) -> Self {
        RtpigateError::Parse(err.to_string())
    }
}

// Allow Send + Sync for use across async task boundaries
unsafe impl Send for RtpigateError {}
unsafe impl Sync for RtpigateError {}
