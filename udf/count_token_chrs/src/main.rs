//! Executor-based equivalent of count_tokens_rs.
//! Configure RowBinary, send_chunk_header=true, (String, String) -> UInt32.
//! models.json is staged from count_tokens_rs next to this executable.

use std::collections::HashMap;
use std::io::{self, BufReader};

use std::path::PathBuf;
use std::process;

use clickhouse::{udf, Row};
use serde::Deserialize;
use tiktoken_rs::{cl100k_base, o200k_base, CoreBPE};

/// models.json ships next to the binary in the zip. It lets you map new model
/// names to an encoding without recompiling.
#[derive(Deserialize)]
struct ModelRule {
    prefix: String,
    encoding: String,
}

/// Data files are deployed next to the binary, but the sandbox does not start
/// the process in that directory (the working directory is `/`, the bundle is
/// under `/scripts`), so resolve paths relative to the executable, never to cwd.
fn data_file(name: &str) -> PathBuf {
    let exe = std::env::current_exe()
        .ok()
        .or_else(|| std::env::args().next().map(PathBuf::from));
    match exe.as_deref().and_then(|p| p.parent()) {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(name),
        _ => PathBuf::from(name),
    }
}

struct Tokenizers {
    rules: Vec<ModelRule>,
    encoders: HashMap<&'static str, CoreBPE>,
}

impl Tokenizers {
    fn load() -> Self {
        let path = data_file("models.json");
        // Fail loudly: the zip always contains models.json, so a missing file
        // means the deployment is wrong, not that there are no rules.
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| die(&format!("cannot read {}: {e}", path.display())));
        let mut rules: Vec<ModelRule> = serde_json::from_str(&raw)
            .unwrap_or_else(|e| die(&format!("bad {}: {e}", path.display())));
        rules.sort_by_key(|r| std::cmp::Reverse(r.prefix.len())); // longest prefix wins
        Self {
            rules,
            encoders: HashMap::new(),
        }
    }

    fn encoding_for(&self, model: &str) -> &'static str {
        if let Some(r) = self.rules.iter().find(|r| model.starts_with(&r.prefix)) {
            return match r.encoding.as_str() {
                "cl100k_base" => "cl100k_base",
                _ => "o200k_base",
            };
        }
        // Models we don't recognise get o200k_base. That is an approximation for
        // non-OpenAI tokenizers; see the blog post for how we handle those.
        "o200k_base"
    }

    fn count(&mut self, model: &str, text: &str) -> u32 {
        let name = self.encoding_for(model);
        let bpe = self.encoders.entry(name).or_insert_with(|| {
            let r = if name == "cl100k_base" {
                cl100k_base()
            } else {
                o200k_base()
            };
            r.unwrap_or_else(|e| die(&format!("loading {name}: {e}")))
        });
        bpe.encode_ordinary(text).len() as u32
    }
}

fn die(msg: &str) -> ! {
    eprintln!("count_token_chrs: {msg}"); // stderr is surfaced in the ClickHouse error
    process::exit(1)
}

#[derive(Row, Deserialize)]
struct Input<'a> {
    #[serde(borrow, with = "serde_bytes")]
    model: &'a [u8],
    #[serde(borrow, with = "serde_bytes")]
    text: &'a [u8],
}

fn main() {
    let mut tokenizers = Tokenizers::load();
    let stdin = io::stdin();
    let stdout = io::stdout();
    let input = BufReader::with_capacity(1 << 20, stdin.lock());
    if let Err(error) = udf::Executor::new(input, stdout.lock()).run::<Input<'_>, u32, _>(|row| {
        Ok(tokenizers.count(
            &String::from_utf8_lossy(row.model),
            &String::from_utf8_lossy(row.text),
        ))
    }) {
        die(&error.to_string());
    }
}
