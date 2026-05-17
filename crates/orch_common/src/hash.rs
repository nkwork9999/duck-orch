// Tiny FNV-1a 64-bit hash. Used by Phase 13 Asset registration to compute
// `code_version` (a stable fingerprint of the task SQL) without dragging in
// an external `sha2`/`fnv` crate. Not cryptographic — we only care about
// change detection.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Hash bytes with FNV-1a 64-bit.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Compute the canonical `code_version` (sql hash) for a task SQL body.
/// Whitespace at both ends is trimmed so trivial reformatting doesn't bump
/// the version; internal whitespace is preserved.
pub fn sql_code_version(sql: &str) -> String {
    let h = fnv1a_64(sql.trim().as_bytes());
    format!("fnv1a64:{:016x}", h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_known_vector() {
        // FNV-1a of empty input is the offset basis.
        assert_eq!(fnv1a_64(b""), FNV_OFFSET);
    }

    #[test]
    fn code_version_stable_across_outer_whitespace() {
        let a = sql_code_version("SELECT 1");
        let b = sql_code_version("\n  SELECT 1  \n");
        assert_eq!(a, b);
    }

    #[test]
    fn code_version_changes_with_body() {
        assert_ne!(sql_code_version("SELECT 1"), sql_code_version("SELECT 2"));
    }

    #[test]
    fn code_version_format() {
        let v = sql_code_version("SELECT 1");
        assert!(v.starts_with("fnv1a64:"), "got: {}", v);
        assert_eq!(v.len(), "fnv1a64:".len() + 16);
    }
}
