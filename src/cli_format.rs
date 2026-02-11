use clap::ValueEnum;

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub(crate) enum OutputFormat {
    #[default]
    Text,
    Json,
    Csv,
    Tsv,
}

impl OutputFormat {
    /// Returns the delimiter for CSV/TSV formats
    pub(crate) fn delimiter(&self) -> Option<&'static str> {
        match self {
            Self::Csv => Some(","),
            Self::Tsv => Some("\t"),
            _ => None,
        }
    }

    /// Check if this is a delimited format (CSV or TSV)
    #[allow(dead_code)]
    pub(crate) fn is_delimited(&self) -> bool {
        matches!(self, Self::Csv | Self::Tsv)
    }
}

/// Parse a header in key=value format
pub(crate) fn parse_header(s: &str) -> std::result::Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid header format '{}'. Expected key=value", s));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Escape a field for CSV output (handles commas, quotes, newlines)
pub(crate) fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Escape a field for TSV output (replaces tabs and newlines)
pub(crate) fn tsv_escape(field: &str) -> String {
    field
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Format fields as a delimited row (CSV or TSV)
pub(crate) fn format_delimited_row(format: OutputFormat, fields: &[&str]) -> String {
    let escaped: Vec<String> = match format {
        OutputFormat::Csv => fields.iter().map(|f| csv_escape(f)).collect(),
        OutputFormat::Tsv => fields.iter().map(|f| tsv_escape(f)).collect(),
        _ => fields.iter().map(|s| s.to_string()).collect(),
    };
    let delimiter = format.delimiter().unwrap_or(",");
    escaped.join(delimiter)
}
