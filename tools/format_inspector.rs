// 格式检查工具 - 用于解析和显示 FASTER 二进制文件的详细结构
//
// 用法:
//   cargo run --bin format_inspector -- <file_path> [--format checkpoint|log|index]

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FormatType {
    Checkpoint,
    Log,
    Index,
    Auto,
}

#[derive(Parser, Debug)]
#[command(name = "format_inspector")]
#[command(about = "FASTER 二进制格式检查工具", long_about = None)]
struct Args {
    /// 要检查的文件路径
    #[arg(value_name = "FILE")]
    file_path: PathBuf,

    /// 文件格式类型
    #[arg(short, long, value_enum, default_value = "auto")]
    format: FormatType,

    /// 显示详细信息
    #[arg(short, long)]
    verbose: bool,

    /// 以十六进制显示原始字节
    #[arg(short = 'x', long)]
    hex_dump: bool,

    /// 十六进制显示的最大字节数
    #[arg(long, default_value = "256")]
    hex_limit: usize,
}

struct FileInspector {
    file: File,
    verbose: bool,
    hex_dump: bool,
    hex_limit: usize,
}

impl FileInspector {
    fn new(path: &PathBuf, verbose: bool, hex_dump: bool, hex_limit: usize) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            file,
            verbose,
            hex_dump,
            hex_limit,
        })
    }

    fn read_bytes(&mut self, count: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; count];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_u32_le(&mut self) -> io::Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64_le(&mut self) -> io::Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> io::Result<bool> {
        let byte = self.read_bytes(1)?;
        Ok(byte[0] != 0)
    }

    fn detect_format(&mut self) -> io::Result<FormatType> {
        // 读取前8字节来检测格式
        let magic = self.read_bytes(8)?;
        self.file.seek(SeekFrom::Start(0))?;

        // 检查是否是 OXFLOG1 魔数
        if &magic == b"OXFLOG1\0" {
            return Ok(FormatType::Log);
        }

        // 尝试作为 JSON 读取 (checkpoint 元数据)
        let mut content = String::new();
        self.file.seek(SeekFrom::Start(0))?;
        if self.file.read_to_string(&mut content).is_ok() && content.trim_start().starts_with('{') {
            self.file.seek(SeekFrom::Start(0))?;
            return Ok(FormatType::Checkpoint);
        }
        self.file.seek(SeekFrom::Start(0))?;

        // 默认尝试作为 C 风格二进制 checkpoint
        Ok(FormatType::Checkpoint)
    }

    fn inspect_checkpoint_json(&mut self) -> io::Result<()> {
        let mut content = String::new();
        self.file.read_to_string(&mut content)?;

        println!("=== Checkpoint 元数据 (JSON 格式) ===\n");

        match serde_json::from_str::<serde_json::Value>(&content) {
            Ok(json) => {
                println!("{}", serde_json::to_string_pretty(&json)?);

                if self.verbose {
                    println!("\n--- 详细信息 ---");
                    println!("文件大小: {} 字节", content.len());
                    println!("JSON 格式: oxifaster 原生格式");
                }
            }
            Err(e) => {
                println!("JSON 解析错误: {}", e);
                println!("\n原始内容:");
                println!("{}", content);
            }
        }

        Ok(())
    }

    fn inspect_checkpoint_c_binary(&mut self) -> io::Result<()> {
        println!("=== Checkpoint 元数据 (C 二进制格式) ===\n");

        // 尝试解析 IndexMetadata (56 字节)
        println!("--- IndexMetadata (56 字节) ---");
        let version = self.read_u32_le()?;
        let table_size = self.read_u64_le()?;
        let num_ht_bytes = self.read_u64_le()?;
        let num_ofb_bytes = self.read_u64_le()?;
        let ofb_count = self.read_u64_le()?;
        let log_begin_address = self.read_u64_le()?;
        let checkpoint_start_address = self.read_u64_le()?;

        println!("version: {}", version);
        println!("table_size: {}", table_size);
        println!("num_ht_bytes: {}", num_ht_bytes);
        println!("num_ofb_bytes: {}", num_ofb_bytes);
        println!("ofb_count: 0x{:016x}", ofb_count);
        println!("log_begin_address: 0x{:016x}", log_begin_address);
        println!(
            "checkpoint_start_address: 0x{:016x}",
            checkpoint_start_address
        );

        // 尝试解析 LogMetadata (2336 字节)
        println!("\n--- LogMetadata (2336 字节) ---");
        let use_snapshot_file = self.read_bool()?;
        let _padding1 = self.read_bytes(3)?; // padding
        let log_version = self.read_u32_le()?;
        let num_threads = self.read_u32_le()?;
        let _padding2 = self.read_bytes(4)?; // padding
        let flushed_address = self.read_u64_le()?;
        let final_address = self.read_u64_le()?;

        println!("use_snapshot_file: {}", use_snapshot_file);
        println!("version: {}", log_version);
        println!("num_threads: {}", num_threads);
        println!("flushed_address: 0x{:016x}", flushed_address);
        println!("final_address: 0x{:016x}", final_address);

        // 读取 monotonic_serial_nums (96 个 u64)
        println!("\nmonotonic_serial_nums (96 个条目):");
        for i in 0..96 {
            let serial_num = self.read_u64_le()?;
            if self.verbose || serial_num != 0 {
                println!("  [{}]: {}", i, serial_num);
            }
        }

        // 读取 GUIDs (96 个 u128)
        println!("\nguids (96 个条目):");
        for i in 0..96 {
            let guid_low = self.read_u64_le()?;
            let guid_high = self.read_u64_le()?;
            let guid = ((guid_high as u128) << 64) | (guid_low as u128);
            if self.verbose || guid != 0 {
                println!("  [{}]: {:032x}", i, guid);
            }
        }

        Ok(())
    }

    fn inspect_checkpoint(&mut self) -> io::Result<()> {
        // 首先尝试检测是 JSON 还是二进制格式
        let mut buf = vec![0u8; 64];
        self.file.read_exact(&mut buf)?;
        self.file.seek(SeekFrom::Start(0))?;

        // 检查是否以 '{' 开头 (JSON)
        if buf[0] == b'{' {
            self.inspect_checkpoint_json()
        } else {
            self.inspect_checkpoint_c_binary()
        }
    }

    fn inspect_log(&mut self) -> io::Result<()> {
        println!("=== Log 格式 ===\n");

        // 读取魔数
        let magic = self.read_bytes(8)?;
        println!("魔数: {:?}", String::from_utf8_lossy(&magic));

        if &magic == b"OXFLOG1\0" {
            println!("格式: oxifaster Log (版本 1)");
        } else {
            println!("警告: 未知的魔数，可能不是有效的 OXFLOG1 格式");
        }

        if self.hex_dump {
            println!("\n--- 十六进制转储 (前 {} 字节) ---", self.hex_limit);
            self.file.seek(SeekFrom::Start(0))?;
            let bytes = self.read_bytes(self.hex_limit.min(4096))?;
            hex_dump(&bytes);
        }

        Ok(())
    }

    fn inspect_index(&mut self) -> io::Result<()> {
        println!("=== 索引格式 ===\n");

        // 读取文件大小
        let file_size = self.file.metadata()?.len();
        println!("文件大小: {} 字节", file_size);

        // Hash bucket 是 64 字节对齐
        let num_buckets = file_size / 64;
        println!("预计桶数量: {}", num_buckets);

        if self.hex_dump {
            println!("\n--- 十六进制转储 (前 {} 字节) ---", self.hex_limit);
            self.file.seek(SeekFrom::Start(0))?;
            let bytes = self.read_bytes(self.hex_limit.min(4096))?;
            hex_dump(&bytes);
        }

        Ok(())
    }

    fn inspect(&mut self, format: FormatType) -> io::Result<()> {
        let detected_format = match format {
            FormatType::Auto => self.detect_format()?,
            _ => format,
        };

        match detected_format {
            FormatType::Checkpoint => self.inspect_checkpoint(),
            FormatType::Log => self.inspect_log(),
            FormatType::Index => self.inspect_index(),
            FormatType::Auto => unreachable!(),
        }
    }
}

fn hex_dump(bytes: &[u8]) {
    for (i, chunk) in bytes.chunks(16).enumerate() {
        print!("{:08x}  ", i * 16);

        // 十六进制部分
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }

        // 填充空白
        for _ in chunk.len()..16 {
            print!("   ");
        }

        // ASCII 部分
        print!(" |");
        for byte in chunk {
            let c = if byte.is_ascii_graphic() || *byte == b' ' {
                *byte as char
            } else {
                '.'
            };
            print!("{}", c);
        }
        println!("|");
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    println!("FASTER 格式检查工具");
    println!("文件: {}\n", args.file_path.display());

    let mut inspector =
        FileInspector::new(&args.file_path, args.verbose, args.hex_dump, args.hex_limit)?;
    inspector.inspect(args.format)?;

    Ok(())
}
