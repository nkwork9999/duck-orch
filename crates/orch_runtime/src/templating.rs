// Jinja-style {{ key }} placeholder substitution.

use std::collections::HashMap;

pub fn substitute(sql: &str, vars: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(close) = find_subseq(&bytes[i + 2..], b"}}") {
                let end = i + 2 + close;
                let key = std::str::from_utf8(&bytes[i + 2..end]).unwrap_or("").trim();
                if let Some(v) = vars.get(key) {
                    if v.starts_with('\'') || v.parse::<f64>().is_ok() {
                        out.push_str(v);
                    } else {
                        out.push('\'');
                        out.push_str(&v.replace('\'', "''"));
                        out.push('\'');
                    }
                } else {
                    out.push_str(&sql[i..end + 2]);
                }
                i = end + 2;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn find_subseq(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn replaces_known_var() {
        let mut v = HashMap::new();
        v.insert("last_processed_at".to_string(), "2026-01-01 00:00:00".to_string());
        let r = substitute("SELECT * FROM x WHERE t > {{ last_processed_at }}", &v);
        assert_eq!(r, "SELECT * FROM x WHERE t > '2026-01-01 00:00:00'");
    }

    #[test]
    fn passthrough_unknown() {
        let v = HashMap::new();
        let r = substitute("SELECT {{ unknown }}", &v);
        assert_eq!(r, "SELECT {{ unknown }}");
    }
}
