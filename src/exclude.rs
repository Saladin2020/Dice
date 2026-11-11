//! Path exclude rules for nested folder walks.

use std::path::Path;

/// Default directories/files skipped during recursive put/convert.
pub const DEFAULT_EXCLUDES: &[&str] = &[
    ".git",
    ".svn",
    ".hg",
    "node_modules",
    "target",
    "dist",
    ".dice-store",
    ".DS_Store",
    "Thumbs.db",
    "__pycache__",
    ".idea",
    ".vscode",
];

#[derive(Debug, Clone, Default)]
pub struct ExcludeSet {
    /// Exact path segment or file name matches (case-insensitive on Windows).
    names: Vec<String>,
}

impl ExcludeSet {
    pub fn from_defaults() -> Self {
        Self::from_list(DEFAULT_EXCLUDES.iter().map(|s| (*s).to_string()))
    }

    pub fn from_list(items: impl IntoIterator<Item = String>) -> Self {
        let mut names: Vec<String> = items
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        names.sort();
        names.dedup();
        Self { names }
    }

    pub fn parse_csv(raw: &str) -> Self {
        let items = raw
            .split([',', ';', '\n'])
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self::from_list(items)
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn as_csv(&self) -> String {
        self.names.join(",")
    }

    /// True if this path component or full relative path should be skipped.
    pub fn matches_name(&self, name: &str) -> bool {
        self.names.iter().any(|n| eq_ignore_platform(n, name))
    }

    pub fn matches_path(&self, path: &Path) -> bool {
        for comp in path.components() {
            if let std::path::Component::Normal(s) = comp {
                if self.matches_name(&s.to_string_lossy()) {
                    return true;
                }
            }
        }
        false
    }
}

fn eq_ignore_platform(a: &str, b: &str) -> bool {
    #[cfg(windows)]
    {
        a.eq_ignore_ascii_case(b)
    }
    #[cfg(not(windows))]
    {
        a == b
    }
}
