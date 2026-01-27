// 兼容性验证工具
//
// 用于验证 oxifaster 与 C++ FASTER 数据文件的兼容性

use clap::Parser;
use oxifaster::checkpoint::binary_format::{CIndexMetadata, CLogMetadata};
use oxifaster::format::{FormatDetector, FormatType};
use oxifaster::index::HashBucket;
use oxifaster::varlen::SpanByteView;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[clap(
    name = "verify_compatibility",
    about = "验证 oxifaster 与 C++ FASTER 数据文件的兼容性"
)]
struct Args {
    /// Checkpoint 索引元数据文件
    #[clap(long)]
    index_metadata: Option<PathBuf>,

    /// Checkpoint 日志元数据文件
    #[clap(long)]
    log_metadata: Option<PathBuf>,

    /// Log 文件
    #[clap(long)]
    log_file: Option<PathBuf>,

    /// Index 文件 (index.dat)
    #[clap(long)]
    index_file: Option<PathBuf>,

    /// C++ index checkpoint 目录 (包含 info.dat/ht.dat/ofb.dat)
    #[clap(long)]
    cpp_index_dir: Option<PathBuf>,

    /// 详细输出
    #[clap(short, long)]
    verbose: bool,

    /// 严格模式（发现任何问题立即退出）
    #[clap(short, long)]
    strict: bool,
}

#[derive(Debug)]
struct VerificationResult {
    component: String,
    status: Status,
    message: String,
}

#[derive(Debug, PartialEq)]
enum Status {
    Ok,
    Warning,
    Error,
}

impl VerificationResult {
    fn ok(component: &str, message: &str) -> Self {
        Self {
            component: component.to_string(),
            status: Status::Ok,
            message: message.to_string(),
        }
    }

    fn warning(component: &str, message: &str) -> Self {
        Self {
            component: component.to_string(),
            status: Status::Warning,
            message: message.to_string(),
        }
    }

    fn error(component: &str, message: &str) -> Self {
        Self {
            component: component.to_string(),
            status: Status::Error,
            message: message.to_string(),
        }
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    println!("oxifaster 兼容性验证工具");
    println!("========================\n");

    let mut results = Vec::new();
    let mut has_errors = false;

    // 验证索引元数据
    if let Some(path) = &args.index_metadata {
        match verify_index_metadata(path, args.verbose) {
            Ok(result) => {
                if result.status == Status::Error {
                    has_errors = true;
                }
                results.push(result);
            }
            Err(e) => {
                has_errors = true;
                results.push(VerificationResult::error(
                    "IndexMetadata",
                    &format!("读取失败: {}", e),
                ));
            }
        }

        if args.strict && has_errors {
            print_results(&results);
            std::process::exit(1);
        }
    }

    // 验证日志元数据
    if let Some(path) = &args.log_metadata {
        match verify_log_metadata(path, args.verbose) {
            Ok(result) => {
                if result.status == Status::Error {
                    has_errors = true;
                }
                results.push(result);
            }
            Err(e) => {
                has_errors = true;
                results.push(VerificationResult::error(
                    "LogMetadata",
                    &format!("读取失败: {}", e),
                ));
            }
        }

        if args.strict && has_errors {
            print_results(&results);
            std::process::exit(1);
        }
    }

    // 验证 Log 文件
    if let Some(path) = &args.log_file {
        match verify_log_file(path, args.verbose) {
            Ok(result) => {
                if result.status == Status::Error {
                    has_errors = true;
                }
                results.push(result);
            }
            Err(e) => {
                has_errors = true;
                results.push(VerificationResult::error(
                    "LogFile",
                    &format!("读取失败: {}", e),
                ));
            }
        }
    }

    // 验证 Index 文件
    if let Some(path) = &args.index_file {
        match verify_index_file(path, args.verbose) {
            Ok(result) => {
                if result.status == Status::Error {
                    has_errors = true;
                }
                results.push(result);
            }
            Err(e) => {
                has_errors = true;
                results.push(VerificationResult::error(
                    "IndexFile",
                    &format!("读取失败: {}", e),
                ));
            }
        }
    }

    // 验证 C++ Index checkpoint 目录
    if let Some(path) = &args.cpp_index_dir {
        match verify_cpp_index_dir(path, args.verbose) {
            Ok(result) => {
                if result.status == Status::Error {
                    has_errors = true;
                }
                results.push(result);
            }
            Err(e) => {
                has_errors = true;
                results.push(VerificationResult::error(
                    "CppIndex",
                    &format!("读取失败: {}", e),
                ));
            }
        }
    }

    // 打印结果
    print_results(&results);

    // 汇总
    println!("\n汇总:");
    let ok_count = results.iter().filter(|r| r.status == Status::Ok).count();
    let warning_count = results
        .iter()
        .filter(|r| r.status == Status::Warning)
        .count();
    let error_count = results.iter().filter(|r| r.status == Status::Error).count();

    println!("  通过: {}", ok_count);
    println!("  警告: {}", warning_count);
    println!("  错误: {}", error_count);

    if has_errors {
        println!("\n验证失败：发现兼容性问题");
        std::process::exit(1);
    } else if warning_count > 0 {
        println!("\n验证完成：存在警告");
        std::process::exit(0);
    } else {
        println!("\n验证完成：所有检查通过");
        std::process::exit(0);
    }
}

fn verify_index_metadata(path: &PathBuf, verbose: bool) -> io::Result<VerificationResult> {
    println!("验证索引元数据: {}", path.display());

    let mut file = File::open(path)?;

    // 检测格式
    let format_result = FormatDetector::detect(&mut file)?;

    if verbose {
        println!("  格式类型: {}", format_result.format_type);
        if let Some(version) = format_result.version {
            println!("  版本: {}", version);
        }
        println!("  详细信息: {}", format_result.info);
    }

    // 读取完整数据
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    // 尝试解析为 C 二进制格式
    match CIndexMetadata::deserialize(&buf) {
        Ok(metadata) => {
            // 验证合理性
            let mut warnings = Vec::new();

            if metadata.version == 0 {
                warnings.push("版本号为 0，可能是未初始化的数据");
            }

            if metadata.table_size == 0 {
                warnings.push("表大小为 0，可能是未初始化的数据");
            }

            if !metadata.table_size.is_power_of_two() {
                warnings.push("表大小不是 2 的幂次，可能导致性能问题");
            }

            if verbose {
                println!("  版本: {}", metadata.version);
                println!("  表大小: {}", metadata.table_size);
                println!("  哈希表字节数: {}", metadata.num_ht_bytes);
                println!("  溢出桶字节数: {}", metadata.num_ofb_bytes);
                println!("  溢出桶数量: {}", metadata.ofb_count);
                println!("  日志起始地址: 0x{:x}", metadata.log_begin_address);
                println!(
                    "  Checkpoint 起始地址: 0x{:x}",
                    metadata.checkpoint_start_address
                );
            }

            if warnings.is_empty() {
                Ok(VerificationResult::ok(
                    "IndexMetadata",
                    "C 二进制格式，所有检查通过",
                ))
            } else {
                Ok(VerificationResult::warning(
                    "IndexMetadata",
                    &format!("C 二进制格式，警告: {}", warnings.join("; ")),
                ))
            }
        }
        Err(e) => {
            // 可能是 JSON 格式
            if format_result.format_type == FormatType::Json {
                Ok(VerificationResult::ok(
                    "IndexMetadata",
                    "JSON 格式（oxifaster 原生格式）",
                ))
            } else {
                Ok(VerificationResult::error(
                    "IndexMetadata",
                    &format!("解析失败: {}", e),
                ))
            }
        }
    }
}

fn verify_log_metadata(path: &PathBuf, verbose: bool) -> io::Result<VerificationResult> {
    println!("验证日志元数据: {}", path.display());

    let mut file = File::open(path)?;

    // 检测格式
    let format_result = FormatDetector::detect(&mut file)?;

    if verbose {
        println!("  格式类型: {}", format_result.format_type);
        if let Some(version) = format_result.version {
            println!("  版本: {}", version);
        }
        println!("  详细信息: {}", format_result.info);
    }

    // 读取完整数据
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    // 尝试解析为 C 二进制格式
    match CLogMetadata::deserialize(&buf) {
        Ok(metadata) => {
            // 验证合理性
            let mut warnings = Vec::new();

            if metadata.version == 0 {
                warnings.push("版本号为 0，可能是未初始化的数据");
            }

            if metadata.num_threads == 0 {
                warnings.push("线程数为 0，可能是未初始化的数据");
            }

            if metadata.num_threads > 1024 {
                warnings.push("线程数 > 1024，可能是异常值");
            }

            if metadata.flushed_address > metadata.final_address {
                warnings.push("flushed_address > final_address，数据可能不一致");
            }

            if verbose {
                println!("  版本: {}", metadata.version);
                println!("  使用快照文件: {}", metadata.use_snapshot_file);
                println!("  线程数: {}", metadata.num_threads);
                println!("  已刷新地址: 0x{:x}", metadata.flushed_address);
                println!("  最终地址: 0x{:x}", metadata.final_address);

                // 显示会话信息（前10个）
                println!("  会话信息 (前10个):");
                for i in 0..10.min(metadata.num_threads as usize) {
                    if metadata.monotonic_serial_nums[i] != 0 || metadata.guids[i] != 0 {
                        println!(
                            "    会话 {}: serial_num={}, guid=0x{:x}",
                            i, metadata.monotonic_serial_nums[i], metadata.guids[i]
                        );
                    }
                }
            }

            if warnings.is_empty() {
                Ok(VerificationResult::ok(
                    "LogMetadata",
                    "C 二进制格式，所有检查通过",
                ))
            } else {
                Ok(VerificationResult::warning(
                    "LogMetadata",
                    &format!("C 二进制格式，警告: {}", warnings.join("; ")),
                ))
            }
        }
        Err(e) => {
            // 可能是 JSON 格式
            if format_result.format_type == FormatType::Json {
                Ok(VerificationResult::ok(
                    "LogMetadata",
                    "JSON 格式（oxifaster 原生格式）",
                ))
            } else {
                Ok(VerificationResult::error(
                    "LogMetadata",
                    &format!("解析失败: {}", e),
                ))
            }
        }
    }
}

fn verify_log_file(path: &PathBuf, verbose: bool) -> io::Result<VerificationResult> {
    println!("验证 Log 文件: {}", path.display());

    let mut file = File::open(path)?;

    // 检测格式
    let format_result = FormatDetector::detect(&mut file)?;

    if verbose {
        println!("  格式类型: {}", format_result.format_type);
        if let Some(version) = format_result.version {
            println!("  版本: {}", version);
        }
        println!("  详细信息: {}", format_result.info);
    }

    // 读取数据并尝试解析记录
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    // 跳过格式头（如果有）
    let data_start = if format_result.has_header {
        24 // UniversalFormatHeader::SIZE
    } else {
        0
    };

    if buf.len() <= data_start {
        return Ok(VerificationResult::error("LogFile", "文件太小，没有数据"));
    }

    // 尝试解析 SpanByte 记录
    let mut offset = data_start;
    let mut record_count = 0;
    let mut parse_errors = 0;
    const MAX_RECORDS_TO_CHECK: usize = 100;

    while offset < buf.len() && record_count < MAX_RECORDS_TO_CHECK {
        if offset + 4 > buf.len() {
            break; // 不足以读取头部
        }

        match SpanByteView::parse(&buf[offset..]) {
            Some(view) => {
                if verbose && record_count < 10 {
                    println!(
                        "  记录 {}: 长度={}, 有元数据={}",
                        record_count,
                        view.payload().len(),
                        view.metadata().is_some()
                    );
                }
                // SpanByteView::total_size is an associated function, not a method
                offset += SpanByteView::total_size(view.payload().len(), view.metadata().is_some());
                record_count += 1;
            }
            None => {
                parse_errors += 1;
                offset += 4; // 跳过可能损坏的头部
            }
        }
    }

    if verbose {
        println!("  解析的记录数: {}", record_count);
        println!("  解析错误数: {}", parse_errors);
    }

    if parse_errors == 0 && record_count > 0 {
        Ok(VerificationResult::ok(
            "LogFile",
            &format!("成功解析 {} 条记录", record_count),
        ))
    } else if parse_errors > 0 && record_count > 0 {
        Ok(VerificationResult::warning(
            "LogFile",
            &format!(
                "解析了 {} 条记录，但有 {} 个错误",
                record_count, parse_errors
            ),
        ))
    } else {
        Ok(VerificationResult::error("LogFile", "无法解析任何记录"))
    }
}

fn verify_index_file(path: &PathBuf, verbose: bool) -> io::Result<VerificationResult> {
    println!("验证 Index 文件: {}", path.display());

    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mut header = [0u8; 16];
    reader.read_exact(&mut header)?;
    let table_size = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let overflow_count = u64::from_le_bytes(header[8..16].try_into().unwrap());

    let bucket_bytes = (HashBucket::NUM_ENTRIES as u64 + 1) * 8;
    let expected_len = 16 + (table_size + overflow_count) * bucket_bytes;

    if verbose {
        println!("  table_size: {}", table_size);
        println!("  overflow_bucket_count: {}", overflow_count);
        println!("  bucket_bytes: {}", bucket_bytes);
        println!("  file_len: {}", file_len);
        println!("  expected_len: {}", expected_len);
    }

    if file_len != expected_len {
        return Ok(VerificationResult::error(
            "IndexFile",
            &format!(
                "文件长度不匹配: expected {}, got {}",
                expected_len, file_len
            ),
        ));
    }

    let bucket_count = table_size + overflow_count;
    let sample_count = bucket_count.min(10) as usize;
    let mut non_zero_entries = 0usize;
    let mut total_entries = 0usize;

    let mut buf = vec![0u8; bucket_bytes as usize];
    for _ in 0..sample_count {
        reader.read_exact(&mut buf)?;
        for i in 0..(HashBucket::NUM_ENTRIES + 1) {
            let start = i * 8;
            let control =
                u64::from_le_bytes(buf[start..start + 8].try_into().expect("slice length is 8"));
            total_entries += 1;
            if control != 0 {
                non_zero_entries += 1;
            }
        }
    }

    if verbose {
        println!(
            "  sample buckets: {}, non-zero entries: {} / {}",
            sample_count, non_zero_entries, total_entries
        );
    }

    if bucket_count == 0 {
        return Ok(VerificationResult::warning(
            "IndexFile",
            "table_size 和 overflow_bucket_count 均为 0",
        ));
    }

    if non_zero_entries == 0 {
        return Ok(VerificationResult::warning(
            "IndexFile",
            "样本 bucket 全部为空",
        ));
    }

    Ok(VerificationResult::ok(
        "IndexFile",
        "Index 文件结构检查通过",
    ))
}

fn verify_cpp_index_dir(path: &Path, verbose: bool) -> io::Result<VerificationResult> {
    println!("验证 C++ Index 目录: {}", path.display());

    let info_path = path.join("info.dat");
    let ht_path = path.join("ht.dat");
    let ofb_path = path.join("ofb.dat");

    let info_bytes = std::fs::read(&info_path)?;
    let metadata = CIndexMetadata::deserialize(&info_bytes)?;

    let bucket_bytes = (HashBucket::NUM_ENTRIES as u64 + 1) * 8;
    let expected_ht = metadata.table_size * bucket_bytes;
    let expected_ofb = metadata.ofb_count * bucket_bytes;

    let ht_len = std::fs::metadata(&ht_path)?.len();
    let ofb_len = if ofb_path.exists() {
        std::fs::metadata(&ofb_path)?.len()
    } else {
        0
    };

    if verbose {
        println!("  table_size: {}", metadata.table_size);
        println!("  ofb_count: {}", metadata.ofb_count);
        println!("  bucket_bytes: {}", bucket_bytes);
        println!("  ht.dat: {} (expected {})", ht_len, expected_ht);
        println!("  ofb.dat: {} (expected {})", ofb_len, expected_ofb);
    }

    if ht_len != expected_ht {
        return Ok(VerificationResult::error(
            "CppIndex",
            &format!(
                "ht.dat 长度不匹配: expected {}, got {}",
                expected_ht, ht_len
            ),
        ));
    }

    if expected_ofb == 0 {
        if ofb_len != 0 {
            return Ok(VerificationResult::warning(
                "CppIndex",
                "ofb_count 为 0 但 ofb.dat 非空",
            ));
        }
    } else if ofb_len != expected_ofb {
        return Ok(VerificationResult::error(
            "CppIndex",
            &format!(
                "ofb.dat 长度不匹配: expected {}, got {}",
                expected_ofb, ofb_len
            ),
        ));
    }

    let mut ht_reader = BufReader::new(File::open(&ht_path)?);
    let sample_count = metadata.table_size.min(10) as usize;
    let mut buf = vec![0u8; bucket_bytes as usize];
    let mut non_zero_entries = 0usize;
    let mut total_entries = 0usize;

    for _ in 0..sample_count {
        ht_reader.read_exact(&mut buf)?;
        for i in 0..(HashBucket::NUM_ENTRIES + 1) {
            let start = i * 8;
            let control =
                u64::from_le_bytes(buf[start..start + 8].try_into().expect("slice length is 8"));
            total_entries += 1;
            if control != 0 {
                non_zero_entries += 1;
            }
        }
    }

    if verbose {
        println!(
            "  ht.dat sample buckets: {}, non-zero entries: {} / {}",
            sample_count, non_zero_entries, total_entries
        );
    }

    if metadata.ofb_count > 0 && ofb_len > 0 {
        let mut ofb_reader = BufReader::new(File::open(&ofb_path)?);
        let ofb_sample = metadata.ofb_count.min(5) as usize;
        let mut ofb_non_zero = 0usize;
        let mut ofb_total = 0usize;
        for _ in 0..ofb_sample {
            ofb_reader.read_exact(&mut buf)?;
            for i in 0..(HashBucket::NUM_ENTRIES + 1) {
                let start = i * 8;
                let control = u64::from_le_bytes(
                    buf[start..start + 8].try_into().expect("slice length is 8"),
                );
                ofb_total += 1;
                if control != 0 {
                    ofb_non_zero += 1;
                }
            }
        }

        if verbose {
            println!(
                "  ofb.dat sample buckets: {}, non-zero entries: {} / {}",
                ofb_sample, ofb_non_zero, ofb_total
            );
        }
    }

    Ok(VerificationResult::ok(
        "CppIndex",
        "C++ index 文件结构检查通过",
    ))
}

fn print_results(results: &[VerificationResult]) {
    println!("\n验证结果:");
    println!("----------");

    for result in results {
        let status_str = match result.status {
            Status::Ok => "✓ 通过",
            Status::Warning => "⚠ 警告",
            Status::Error => "✗ 错误",
        };

        println!(
            "{:20} {} - {}",
            result.component, status_str, result.message
        );
    }
}
