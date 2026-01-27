use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use oxifaster::address::Address;
use oxifaster::checkpoint::binary_format::{CIndexMetadata, CLogMetadata};
use oxifaster::codec::multi_hash::hash_faster_compat_u64;
use oxifaster::index::IndexHashBucketEntry;
use uuid::Uuid;

const ENTRIES_PER_BUCKET: usize = 7;
const BUCKET_BYTES: usize = (ENTRIES_PER_BUCKET + 1) * 8;
const DEFAULT_ENTRY_COUNT: usize = 12;
const DEFAULT_ADDRESS_BASE: u64 = 256;
const DEFAULT_LOG_FINAL_ADDRESS: u64 = 1024;

#[derive(Debug, Clone)]
struct ManifestEntry {
    key: u64,
    address: u64,
}

#[derive(Debug, Clone)]
struct Manifest {
    version: u32,
    table_size: u64,
    log_begin_address: u64,
    checkpoint_start_address: u64,
    entries: Vec<ManifestEntry>,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    entries: [u64; ENTRIES_PER_BUCKET],
    overflow: u64,
}

impl Bucket {
    fn empty() -> Self {
        Self {
            entries: [0u64; ENTRIES_PER_BUCKET],
            overflow: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct IndexData {
    table: Vec<Bucket>,
    overflow: Vec<Bucket>,
}

#[derive(Parser, Debug)]
#[command(name = "interop_index_tool")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Generate {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 16)]
        table_size: u64,
        #[arg(long)]
        entries: Option<String>,
    },
    Verify {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        token: Option<String>,
    },
    Update {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        entries: String,
        #[arg(long)]
        token: Option<String>,
    },
}

fn hash_key(key: u64) -> u64 {
    hash_faster_compat_u64(key)
}

fn bucket_index(hash: u64, table_size: u64) -> u64 {
    hash & (table_size - 1)
}

fn tag_from_hash(hash: u64) -> u16 {
    let mask = (1u64 << IndexHashBucketEntry::TAG_BITS) - 1;
    ((hash >> 48) & mask) as u16
}

fn make_entry_control(address: u64, tag: u16) -> u64 {
    IndexHashBucketEntry::new(Address::from_control(address), tag, false).control()
}

fn parse_entries_arg(arg: &str) -> Vec<ManifestEntry> {
    arg.split(',')
        .filter_map(|item| {
            let mut parts = item.split('=');
            let key = parts.next()?.trim().parse::<u64>().ok()?;
            let address = parts.next()?.trim().parse::<u64>().ok()?;
            Some(ManifestEntry { key, address })
        })
        .collect()
}

fn build_default_entries(table_size: u64) -> Vec<ManifestEntry> {
    let mut entries = Vec::new();
    let mut used = HashSet::new();
    let mut key = 1u64;
    while entries.len() < DEFAULT_ENTRY_COUNT {
        let hash = hash_key(key);
        let bucket = bucket_index(hash, table_size);
        let tag = tag_from_hash(hash);
        if bucket != 0 {
            key += 1;
            continue;
        }
        let tag_key = (bucket << 16) | tag as u64;
        if used.insert(tag_key) {
            let address = DEFAULT_ADDRESS_BASE + key * 64;
            entries.push(ManifestEntry { key, address });
        }
        key += 1;
    }
    entries
}

fn read_manifest(path: &Path) -> io::Result<Manifest> {
    let content = fs::read_to_string(path)?;
    let mut manifest = Manifest {
        version: 1,
        table_size: 16,
        log_begin_address: 0,
        checkpoint_start_address: 128,
        entries: Vec::new(),
    };
    let mut in_entries = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed == "entries:" {
            in_entries = true;
            continue;
        }
        if in_entries {
            let mut parts = trimmed.split(',');
            let key = parts
                .next()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(0);
            let address = parts
                .next()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(0);
            if key != 0 || address != 0 {
                manifest.entries.push(ManifestEntry { key, address });
            }
            continue;
        }
        if let Some((k, v)) = trimmed.split_once('=') {
            let key = k.trim();
            let value = v.trim();
            match key {
                "version" => manifest.version = value.parse().unwrap_or(1),
                "table_size" => manifest.table_size = value.parse().unwrap_or(16),
                "log_begin_address" => manifest.log_begin_address = value.parse().unwrap_or(0),
                "checkpoint_start_address" => {
                    manifest.checkpoint_start_address = value.parse().unwrap_or(128)
                }
                _ => {}
            }
        }
    }
    Ok(manifest)
}

fn write_manifest(path: &Path, manifest: &Manifest) -> io::Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(out, "version={}", manifest.version)?;
    writeln!(out, "table_size={}", manifest.table_size)?;
    writeln!(out, "log_begin_address={}", manifest.log_begin_address)?;
    writeln!(
        out,
        "checkpoint_start_address={}",
        manifest.checkpoint_start_address
    )?;
    writeln!(out, "entries:")?;
    for entry in &manifest.entries {
        writeln!(out, "{},{}", entry.key, entry.address)?;
    }
    Ok(())
}

fn insert_into_bucket(bucket: &mut Bucket, control: u64, overflow: &mut Vec<Bucket>) -> bool {
    let mut current_overflow: Option<usize> = None;
    loop {
        let next = if let Some(idx) = current_overflow {
            let current_bucket = &mut overflow[idx];
            for entry in current_bucket.entries.iter_mut() {
                if *entry == 0 {
                    *entry = control;
                    return true;
                }
            }
            current_bucket.overflow
        } else {
            for entry in bucket.entries.iter_mut() {
                if *entry == 0 {
                    *entry = control;
                    return true;
                }
            }
            bucket.overflow
        };

        if next == 0 {
            overflow.push(Bucket::empty());
            let new_idx = overflow.len() as u64;
            match current_overflow {
                None => bucket.overflow = new_idx,
                Some(idx) => overflow[idx].overflow = new_idx,
            }
            current_overflow = Some((new_idx - 1) as usize);
        } else {
            let idx = (next - 1) as usize;
            if idx >= overflow.len() {
                return false;
            }
            current_overflow = Some(idx);
        }
    }
}

fn build_index(manifest: &Manifest) -> Result<IndexData, String> {
    if !manifest.table_size.is_power_of_two() {
        return Err("table_size must be power of two".to_string());
    }
    let mut data = IndexData {
        table: vec![Bucket::empty(); manifest.table_size as usize],
        overflow: Vec::new(),
    };

    let mut used = HashSet::new();
    for entry in &manifest.entries {
        let hash = hash_key(entry.key);
        let bucket = bucket_index(hash, manifest.table_size);
        let tag = tag_from_hash(hash);
        let tag_key = (bucket << 16) | tag as u64;
        if !used.insert(tag_key) {
            return Err(format!("duplicate tag for key {}", entry.key));
        }
        let control = make_entry_control(entry.address, tag);
        let bucket_idx = bucket as usize;
        if !insert_into_bucket(&mut data.table[bucket_idx], control, &mut data.overflow) {
            return Err(format!("failed to insert key {}", entry.key));
        }
    }

    Ok(data)
}

fn lookup(data: &IndexData, table_size: u64, key: u64) -> Option<u64> {
    if table_size == 0 {
        return None;
    }
    let hash = hash_key(key);
    let bucket_idx = bucket_index(hash, table_size) as usize;
    let tag = tag_from_hash(hash);
    let mut bucket = data.table.get(bucket_idx)?;

    loop {
        for &control in &bucket.entries {
            if control == 0 {
                continue;
            }
            let entry = IndexHashBucketEntry::from_control(control);
            if entry.tag() == tag {
                return Some(entry.address().control());
            }
        }
        let next = bucket.overflow;
        if next == 0 {
            return None;
        }
        let idx = (next - 1) as usize;
        bucket = data.overflow.get(idx)?;
    }
}

fn write_buckets(path: &Path, buckets: &[Bucket]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    for bucket in buckets {
        for entry in bucket.entries {
            writer.write_all(&entry.to_le_bytes())?;
        }
        writer.write_all(&bucket.overflow.to_le_bytes())?;
    }
    Ok(())
}

fn read_buckets(path: &Path, count: u64) -> io::Result<Vec<Bucket>> {
    let mut buckets = Vec::with_capacity(count as usize);
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = [0u8; BUCKET_BYTES];
    for _ in 0..count {
        reader.read_exact(&mut buf)?;
        let mut entries = [0u64; ENTRIES_PER_BUCKET];
        for (i, entry) in entries.iter_mut().enumerate() {
            let start = i * 8;
            *entry = u64::from_le_bytes(buf[start..start + 8].try_into().unwrap());
        }
        let overflow = u64::from_le_bytes(buf[ENTRIES_PER_BUCKET * 8..].try_into().unwrap());
        buckets.push(Bucket { entries, overflow });
    }
    Ok(buckets)
}

fn write_checkpoint(base_dir: &Path, manifest: &Manifest) -> io::Result<()> {
    fs::create_dir_all(base_dir)?;
    let token = Uuid::new_v4().to_string().to_uppercase();
    let index_dir = base_dir.join("index-checkpoints").join(&token);
    let log_dir = base_dir.join("cpr-checkpoints").join(&token);
    fs::create_dir_all(&index_dir)?;
    fs::create_dir_all(&log_dir)?;

    let data = build_index(manifest).map_err(io::Error::other)?;

    let mut index_meta = CIndexMetadata::new();
    index_meta.version = manifest.version;
    index_meta.table_size = manifest.table_size;
    index_meta.num_ht_bytes = manifest.table_size * BUCKET_BYTES as u64;
    index_meta.num_ofb_bytes = data.overflow.len() as u64 * BUCKET_BYTES as u64;
    index_meta.ofb_count = data.overflow.len() as u64;
    index_meta.log_begin_address = manifest.log_begin_address;
    index_meta.checkpoint_start_address = manifest.checkpoint_start_address;

    let info_path = index_dir.join("info.dat");
    let mut info = BufWriter::new(File::create(info_path)?);
    info.write_all(&index_meta.serialize())?;

    write_buckets(&index_dir.join("ht.dat"), &data.table)?;
    write_buckets(&index_dir.join("ofb.dat"), &data.overflow)?;

    write_manifest(&index_dir.join("manifest.txt"), manifest)?;

    let mut log_meta = CLogMetadata::new();
    log_meta.use_snapshot_file = true;
    log_meta.version = manifest.version;
    log_meta.num_threads = 1;
    log_meta.flushed_address = 0;
    log_meta.final_address = DEFAULT_LOG_FINAL_ADDRESS;
    log_meta.monotonic_serial_nums[0] = 1;
    log_meta.guids[0] = Uuid::new_v4().as_u128();

    let mut log_info = BufWriter::new(File::create(log_dir.join("info.dat"))?);
    log_info.write_all(&log_meta.serialize())?;
    File::create(log_dir.join("snapshot.dat"))?;

    let mut token_file = BufWriter::new(File::create(base_dir.join("token.txt"))?);
    writeln!(token_file, "{}", token)?;

    println!("Token: {}", token);
    println!("Index dir: {}", index_dir.display());
    println!("Log dir: {}", log_dir.display());
    Ok(())
}

fn read_token(base_dir: &Path, token: &Option<String>) -> io::Result<String> {
    if let Some(value) = token {
        return Ok(value.clone());
    }
    let content = fs::read_to_string(base_dir.join("token.txt"))?;
    Ok(content
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string())
}

fn read_index_data(index_dir: &Path, metadata: &CIndexMetadata) -> io::Result<IndexData> {
    let table = read_buckets(&index_dir.join("ht.dat"), metadata.table_size)?;
    let overflow = read_buckets(&index_dir.join("ofb.dat"), metadata.ofb_count)?;
    Ok(IndexData { table, overflow })
}

fn verify_checkpoint(base_dir: &Path, token: &Option<String>) -> io::Result<()> {
    let token_str = read_token(base_dir, token)?;
    let index_dir = base_dir.join("index-checkpoints").join(&token_str);
    let log_dir = base_dir.join("cpr-checkpoints").join(&token_str);

    let manifest = read_manifest(&index_dir.join("manifest.txt"))?;
    let info_bytes = fs::read(index_dir.join("info.dat"))?;
    let index_meta = CIndexMetadata::deserialize(&info_bytes)?;
    let data = read_index_data(&index_dir, &index_meta)?;

    let mut ok = 0usize;
    for entry in &manifest.entries {
        match lookup(&data, manifest.table_size, entry.key) {
            Some(address) if address == entry.address => ok += 1,
            _ => {
                return Err(io::Error::other(format!(
                    "lookup failed for key {}",
                    entry.key
                )))
            }
        }
    }

    let log_bytes = fs::read(log_dir.join("info.dat"))?;
    let log_meta = CLogMetadata::deserialize(&log_bytes)?;
    if log_meta.version != manifest.version {
        return Err(io::Error::other("log metadata version mismatch"));
    }

    println!("Verified entries: {}", ok);
    println!("Index dir: {}", index_dir.display());
    Ok(())
}

fn update_checkpoint(
    base_dir: &Path,
    out_dir: &Path,
    token: &Option<String>,
    new_entries: Vec<ManifestEntry>,
) -> io::Result<()> {
    let token_str = read_token(base_dir, token)?;
    let index_dir = base_dir.join("index-checkpoints").join(&token_str);
    let mut manifest = read_manifest(&index_dir.join("manifest.txt"))?;
    manifest.entries.extend(new_entries);
    write_checkpoint(out_dir, &manifest)
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Generate {
            out,
            table_size,
            entries,
        } => {
            let mut manifest = Manifest {
                version: 1,
                table_size,
                log_begin_address: 0,
                checkpoint_start_address: 128,
                entries: Vec::new(),
            };
            manifest.entries = if let Some(arg) = entries {
                parse_entries_arg(&arg)
            } else {
                build_default_entries(table_size)
            };
            write_checkpoint(&out, &manifest)
        }
        Command::Verify { dir, token } => verify_checkpoint(&dir, &token),
        Command::Update {
            dir,
            out,
            entries,
            token,
        } => {
            let new_entries = parse_entries_arg(&entries);
            update_checkpoint(&dir, &out, &token, new_entries)
        }
    }
}
