//! Parses a samply profile (Firefox Processed Profile JSON) and prints the
//! top N functions by inclusive time, filtered to this crate's symbols.
//!
//! Inclusive time means a function is counted for every sample where it
//! appears anywhere in the call stack -- not just at the leaf. This way
//! `write_record` gets credit for all the I/O, encoding and formatting
//! it calls into.
//!
//! Supports samply's `--unstable-presymbolicate` sidecar (`.syms.json`)
//! for resolving addresses to function names, as well as pre-symbolicated
//! profiles (format v60+).
//!
//! Usage:
//!     `profile_report --input <profile.json|profile.json.gz> [--top <N>]`

use clap::Parser;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(about = "Parse a samply profile and report top functions by inclusive/self time")]
struct Cli {
    /// Path to the samply profile (.json or .json.gz)
    #[arg(long, short)]
    input: PathBuf,

    /// Number of top functions to display
    #[arg(long, short, default_value_t = 10)]
    top: usize,
}

fn main() {
    let cli = Cli::parse();

    let json = read_profile(&cli.input);
    let profile: Value = serde_json::from_str(&json).expect("Failed to parse profile JSON");
    let symbol_map = load_sidecar_symbols(&cli.input);

    print_top_functions(&profile, &symbol_map, cli.top);
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

fn read_profile(path: &Path) -> String {
    let file = File::open(path).expect("Failed to open profile file");
    let mut reader = BufReader::new(file);

    let is_gzip = path.extension().is_some_and(|ext| ext == "gz") || {
        let mut magic = [0u8; 2];
        if reader.read_exact(&mut magic).is_ok() {
            let file = File::open(path).expect("Failed to reopen profile file");
            reader = BufReader::new(file);
            magic == [0x1f, 0x8b]
        } else {
            false
        }
    };

    let mut json = String::new();
    if is_gzip {
        GzDecoder::new(reader)
            .read_to_string(&mut json)
            .expect("Failed to decompress gzip profile");
    } else {
        reader
            .read_to_string(&mut json)
            .expect("Failed to read profile file");
    }
    json
}

// ---------------------------------------------------------------------------
// Sidecar symbol resolution (.syms.json)
// ---------------------------------------------------------------------------

/// An address range [rva, rva + size) mapped to a function name.
struct SymbolEntry {
    rva: u64,
    size: u64,
    name: String,
}

/// Load the `.syms.json` sidecar file if it exists, building a flat list of
/// address-to-name mappings sorted by RVA for binary search.
fn load_sidecar_symbols(profile_path: &Path) -> Vec<SymbolEntry> {
    let sidecar_path = sidecar_path_for(profile_path);
    let Some(sidecar_path) = sidecar_path else {
        return Vec::new();
    };

    let Ok(file) = File::open(&sidecar_path) else {
        return Vec::new();
    };
    let Ok(syms): Result<Value, _> = serde_json::from_reader(BufReader::new(file)) else {
        return Vec::new();
    };

    let Some(string_table) = syms["string_table"].as_array() else {
        return Vec::new();
    };
    let Some(data) = syms["data"].as_array() else {
        return Vec::new();
    };

    let mut entries = Vec::new();
    for lib in data {
        let Some(symbol_table) = lib["symbol_table"].as_array() else {
            continue;
        };
        for sym in symbol_table {
            let Some(rva) = sym["rva"].as_u64() else {
                continue;
            };
            let size = sym["size"].as_u64().unwrap_or(0);
            let Some(name_idx) = sym["symbol"].as_u64() else {
                continue;
            };
            let Some(name_idx) = usize::try_from(name_idx).ok() else {
                continue;
            };
            let Some(name) = string_table.get(name_idx).and_then(|v| v.as_str()) else {
                continue;
            };
            entries.push(SymbolEntry {
                rva,
                size,
                name: name.to_owned(),
            });
        }
    }

    entries.sort_by_key(|e| e.rva);
    entries
}

/// Derive the sidecar path: `foo.json.gz` -> `foo.json.syms.json`.
fn sidecar_path_for(profile_path: &Path) -> Option<PathBuf> {
    let s = profile_path.to_str()?;
    let base = s
        .strip_suffix(".json.gz")
        .or_else(|| s.strip_suffix(".json"))?;
    let sidecar = format!("{base}.json.syms.json");
    let path = PathBuf::from(sidecar);
    if path.exists() { Some(path) } else { None }
}

/// Resolve a hex address string like `"0x4c8c2"` to a function name using
/// the sidecar symbol map. Returns `None` if the address is not in any range.
fn resolve_address<'a>(name: &str, symbol_map: &'a [SymbolEntry]) -> Option<&'a str> {
    let addr = u64::from_str_radix(name.strip_prefix("0x")?, 16).ok()?;

    // Binary search: find the last entry whose rva <= addr.
    let idx = symbol_map.partition_point(|e| e.rva <= addr);
    if idx == 0 {
        return None;
    }
    let entry = &symbol_map[idx - 1];
    if addr < entry.rva + entry.size {
        Some(&entry.name)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Profile tables and stack walking
// ---------------------------------------------------------------------------

/// Lookup tables needed to resolve a stack sample to a function name.
/// In format v49 (samply 0.13.x) these live per-thread; in format v60+
/// they are under the top-level `shared` key.
struct Tables<'a> {
    string_array: &'a [Value],
    func_name_indices: &'a [Value],
    frame_func_indices: &'a [Value],
    stack_frame_indices: &'a [Value],
    stack_prefix: &'a [Value],
}

impl<'a> Tables<'a> {
    /// Resolve a stack index to the function name at that frame.
    fn resolve_frame(&self, stack_idx: u64) -> Option<&'a str> {
        let stack_idx = usize::try_from(stack_idx).ok()?;
        let frame_idx = self.stack_frame_indices.get(stack_idx)?.as_u64()?;
        let frame_idx = usize::try_from(frame_idx).ok()?;
        let func_idx = self.frame_func_indices.get(frame_idx)?.as_u64()?;
        let func_idx = usize::try_from(func_idx).ok()?;
        let name_idx = self.func_name_indices.get(func_idx)?.as_u64()?;
        let name_idx = usize::try_from(name_idx).ok()?;
        self.string_array.get(name_idx)?.as_str()
    }

    /// Walk the full stack from leaf to root, collecting every function name.
    /// Each function is yielded at most once per sample (deduped within a
    /// single stack walk) so recursive functions are not double-counted.
    fn walk_stack(&self, leaf_stack_idx: u64) -> Vec<&'a str> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut current = Some(leaf_stack_idx);

        while let Some(idx) = current {
            if let Some(name) = self.resolve_frame(idx)
                && seen.insert(name)
            {
                result.push(name);
            }
            current = usize::try_from(idx)
                .ok()
                .and_then(|i| self.stack_prefix.get(i))
                .and_then(Value::as_u64);
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Report generation
// ---------------------------------------------------------------------------

fn print_top_functions(profile: &Value, symbol_map: &[SymbolEntry], top_n: usize) {
    let threads = profile["threads"]
        .as_array()
        .expect("missing threads array");

    let shared_tables = try_shared_tables(profile);

    // Inclusive: counted for every sample where the function appears anywhere
    // in the stack. Self: counted only when the function is the leaf frame.
    let mut inclusive_counts: HashMap<String, u64> = HashMap::new();
    let mut self_counts: HashMap<String, u64> = HashMap::new();
    let mut total_samples: u64 = 0;

    for thread in threads {
        let Some(stacks) = thread["samples"]["stack"].as_array() else {
            continue;
        };
        let weights = thread["samples"]["weight"].as_array();

        let per_thread_tables = if shared_tables.is_none() {
            Some(tables_from(thread))
        } else {
            None
        };
        let tables = shared_tables
            .as_ref()
            .or(per_thread_tables.as_ref())
            .unwrap();

        for (i, stack_val) in stacks.iter().enumerate() {
            let Some(stack_idx) = stack_val.as_u64() else {
                continue;
            };
            let weight = weights
                .and_then(|w| w.get(i))
                .and_then(Value::as_u64)
                .unwrap_or(1);

            let stack = tables.walk_stack(stack_idx);

            // The first entry from walk_stack is the leaf frame.
            if let Some(&leaf_name) = stack.first() {
                let resolved = resolve_address(leaf_name, symbol_map).unwrap_or(leaf_name);
                *self_counts.entry(resolved.to_owned()).or_default() += weight;
            }

            for raw_name in stack {
                let resolved = resolve_address(raw_name, symbol_map).unwrap_or(raw_name);
                *inclusive_counts.entry(resolved.to_owned()).or_default() += weight;
            }
            total_samples += weight;
        }
    }

    if total_samples == 0 {
        println!("No samples found in profile.");
        return;
    }

    let mut own_functions: Vec<(&str, u64)> = inclusive_counts
        .iter()
        .filter(|(name, _)| is_own_code(name))
        .map(|(name, count)| (name.as_str(), *count))
        .collect();
    own_functions.sort_by(|a, b| b.1.cmp(&a.1));

    let sampling_interval_ms = 1.0;
    #[allow(clippy::cast_precision_loss)]
    let display: Vec<_> = own_functions
        .iter()
        .take(top_n)
        .map(|(name, incl)| {
            let short_name = shorten(name);
            let self_count = self_counts.get(*name).copied().unwrap_or(0);
            let incl_pct = (*incl as f64 / total_samples as f64) * 100.0;
            let self_pct = (self_count as f64 / total_samples as f64) * 100.0;
            let incl_ms = *incl as f64 * sampling_interval_ms;
            (short_name, incl_pct, self_pct, incl_ms)
        })
        .collect();

    let header = "Function";
    let name_width = display
        .iter()
        .map(|(name, _, _, _)| name.len())
        .max()
        .unwrap_or(0)
        .max(header.len());
    let total_width = name_width + 27;

    println!();
    println!(
        "{:<name_width$} {:>8} {:>8} {:>8}",
        header, "Incl %", "Self %", "~Incl ms",
    );
    println!("{}", "-".repeat(total_width));

    for (short_name, incl_pct, self_pct, incl_ms) in display {
        println!("{short_name:<name_width$} {incl_pct:>7.1}% {self_pct:>7.1}% {incl_ms:>7.0} ms");
    }

    println!("{}", "-".repeat(total_width));
    println!("  ({total_samples} total samples)");
}

/// Try to build tables from the top-level `shared` key (format v60+).
fn try_shared_tables(profile: &Value) -> Option<Tables<'_>> {
    let shared = profile.get("shared")?;
    Some(tables_from(shared))
}

/// Build tables from a JSON object that contains stringArray, funcTable, etc.
/// Works for both a per-thread object (v49) and the shared object (v60+).
fn tables_from(source: &Value) -> Tables<'_> {
    Tables {
        string_array: source["stringArray"]
            .as_array()
            .expect("missing stringArray"),
        func_name_indices: source["funcTable"]["name"]
            .as_array()
            .expect("missing funcTable.name"),
        frame_func_indices: source["frameTable"]["func"]
            .as_array()
            .expect("missing frameTable.func"),
        stack_frame_indices: source["stackTable"]["frame"]
            .as_array()
            .expect("missing stackTable.frame"),
        stack_prefix: source["stackTable"]["prefix"]
            .as_array()
            .expect("missing stackTable.prefix"),
    }
}

/// Returns true if the function name looks like it belongs to this crate.
fn is_own_code(name: &str) -> bool {
    let patterns = ["dta::", "profile::"];
    patterns.iter().any(|p| name.contains(p))
}

/// Shorten a fully qualified Rust symbol to something readable.
fn shorten(name: &str) -> &str {
    if let Some(rest) = name.strip_prefix("dta::") {
        rest
    } else if let Some(rest) = name.strip_prefix("profile::") {
        rest
    } else {
        name
    }
}
