// C++ index interoperability demo tool.
//
// Generates and verifies C++-style index checkpoint files (info.dat/ht.dat/ofb.dat)
// plus a minimal log metadata file for compatibility checks.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <memory>

#include "core/checkpoint_state.h"
#include "core/utility.h"
#include "index/hash_bucket.h"

using namespace FASTER::core;
using namespace FASTER::index;

namespace {

struct ManifestEntry {
  uint64_t key;
  uint64_t address;
};

struct Manifest {
  uint32_t version{1};
  uint64_t table_size{16};
  uint64_t log_begin_address{0};
  uint64_t checkpoint_start_address{128};
  std::vector<ManifestEntry> entries;
};

struct IndexData {
  std::vector<std::unique_ptr<HotLogIndexHashBucket>> table;
  std::vector<std::unique_ptr<HotLogIndexHashBucket>> overflow;
};

constexpr uint64_t kEntryInvalid = HashBucketEntry::kInvalidEntry;
constexpr uint64_t kOverflowInvalid = HashBucketOverflowEntry::kInvalidEntry;
constexpr uint32_t kDefaultEntryCount = 12;
constexpr uint64_t kDefaultAddressBase = 256;
constexpr uint64_t kDefaultLogFinalAddress = 1024;
constexpr uint16_t kTagBits = HotLogIndexBucketEntryDef::kTagBits;
constexpr uint64_t kTagMask = (1ULL << kTagBits) - 1;

uint64_t HashKey(uint64_t key) {
  return FasterHashHelper<uint64_t>::compute(key);
}

uint64_t BucketIndex(uint64_t hash, uint64_t table_size) {
  return hash & (table_size - 1);
}

uint16_t TagFromHash(uint64_t hash) {
  return static_cast<uint16_t>((hash >> 48) & kTagMask);
}

uint64_t MakeEntryControl(uint64_t address, uint16_t tag) {
  HotLogIndexHashBucketEntry entry(Address{address}, tag, false, 0);
  HashBucketEntry base(entry);
  return base.control_;
}

bool IsPowerOfTwo(uint64_t value) {
  return value != 0 && (value & (value - 1)) == 0;
}

std::string Trim(const std::string& input) {
  const auto start = input.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    return "";
  }
  const auto end = input.find_last_not_of(" \t\r\n");
  return input.substr(start, end - start + 1);
}

bool ParseKeyValue(const std::string& line, std::string& key, std::string& value) {
  auto pos = line.find('=');
  if (pos == std::string::npos) {
    return false;
  }
  key = Trim(line.substr(0, pos));
  value = Trim(line.substr(pos + 1));
  return !key.empty() && !value.empty();
}

Manifest ReadManifest(const std::filesystem::path& path) {
  Manifest manifest;
  std::ifstream in(path);
  if (!in.is_open()) {
    throw std::runtime_error("Failed to open manifest: " + path.string());
  }
  std::string line;
  bool in_entries = false;
  while (std::getline(in, line)) {
    line = Trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    if (line == "entries:") {
      in_entries = true;
      continue;
    }
    if (in_entries) {
      auto comma = line.find(',');
      if (comma == std::string::npos) {
        continue;
      }
      uint64_t key = std::stoull(Trim(line.substr(0, comma)));
      uint64_t address = std::stoull(Trim(line.substr(comma + 1)));
      manifest.entries.push_back({key, address});
      continue;
    }
    std::string k;
    std::string v;
    if (!ParseKeyValue(line, k, v)) {
      continue;
    }
    if (k == "version") {
      manifest.version = static_cast<uint32_t>(std::stoul(v));
    } else if (k == "table_size") {
      manifest.table_size = std::stoull(v);
    } else if (k == "log_begin_address") {
      manifest.log_begin_address = std::stoull(v);
    } else if (k == "checkpoint_start_address") {
      manifest.checkpoint_start_address = std::stoull(v);
    }
  }
  return manifest;
}

void WriteManifest(const std::filesystem::path& path, const Manifest& manifest) {
  std::ofstream out(path);
  if (!out.is_open()) {
    throw std::runtime_error("Failed to write manifest: " + path.string());
  }
  out << "version=" << manifest.version << "\n";
  out << "table_size=" << manifest.table_size << "\n";
  out << "log_begin_address=" << manifest.log_begin_address << "\n";
  out << "checkpoint_start_address=" << manifest.checkpoint_start_address << "\n";
  out << "entries:" << "\n";
  for (const auto& entry : manifest.entries) {
    out << entry.key << "," << entry.address << "\n";
  }
}

std::vector<ManifestEntry> ParseEntriesArg(const std::string& arg) {
  std::vector<ManifestEntry> entries;
  if (arg.empty()) {
    return entries;
  }
  std::stringstream ss(arg);
  std::string item;
  while (std::getline(ss, item, ',')) {
    auto pos = item.find('=');
    if (pos == std::string::npos) {
      continue;
    }
    uint64_t key = std::stoull(Trim(item.substr(0, pos)));
    uint64_t address = std::stoull(Trim(item.substr(pos + 1)));
    entries.push_back({key, address});
  }
  return entries;
}

std::vector<ManifestEntry> BuildDefaultEntries(uint64_t table_size) {
  std::vector<ManifestEntry> entries;
  std::unordered_set<uint64_t> used;
  uint64_t key = 1;
  while (entries.size() < kDefaultEntryCount) {
    uint64_t hash = HashKey(key);
    uint64_t bucket = BucketIndex(hash, table_size);
    uint16_t tag = TagFromHash(hash);
    if (bucket != 0) {
      ++key;
      continue;
    }
    uint64_t tag_key = (bucket << 16) | tag;
    if (used.insert(tag_key).second) {
      uint64_t address = kDefaultAddressBase + key * 64;
      entries.push_back({key, address});
    }
    ++key;
  }
  return entries;
}

void ResetBucket(HotLogIndexHashBucket& bucket) {
  for (uint32_t i = 0; i < HotLogIndexHashBucket::kNumEntries; ++i) {
    bucket.entries[i].store(HashBucketEntry{kEntryInvalid});
  }
  bucket.overflow_entry.store(HashBucketOverflowEntry{kOverflowInvalid});
}

bool InsertIntoBucket(HotLogIndexHashBucket& bucket,
                      uint64_t control,
                      std::vector<std::unique_ptr<HotLogIndexHashBucket>>& overflow) {
  for (uint32_t i = 0; i < HotLogIndexHashBucket::kNumEntries; ++i) {
    auto entry = bucket.entries[i].load();
    if (entry.control_ == kEntryInvalid) {
      bucket.entries[i].store(HashBucketEntry{control});
      return true;
    }
  }

  auto overflow_entry = bucket.overflow_entry.load();
  uint64_t next = overflow_entry.address().control();
  if (next == kOverflowInvalid) {
    overflow.push_back(std::make_unique<HotLogIndexHashBucket>());
    ResetBucket(*overflow.back());
    next = overflow.size();
    bucket.overflow_entry.store(HashBucketOverflowEntry{FixedPageAddress{next}});
  }
  if (next == 0 || next > overflow.size()) {
    return false;
  }
  return InsertIntoBucket(*overflow[next - 1], control, overflow);
}

IndexData BuildIndex(const Manifest& manifest) {
  IndexData data;
  data.table.reserve(manifest.table_size);
  for (uint64_t i = 0; i < manifest.table_size; ++i) {
    auto bucket = std::make_unique<HotLogIndexHashBucket>();
    ResetBucket(*bucket);
    data.table.push_back(std::move(bucket));
  }

  std::unordered_set<uint64_t> used;
  for (const auto& entry : manifest.entries) {
    uint64_t hash = HashKey(entry.key);
    uint64_t bucket = BucketIndex(hash, manifest.table_size);
    uint16_t tag = TagFromHash(hash);
    uint64_t tag_key = (bucket << 16) | tag;
    if (!used.insert(tag_key).second) {
      throw std::runtime_error("Duplicate tag in bucket for key: " + std::to_string(entry.key));
    }
    uint64_t control = MakeEntryControl(entry.address, tag);
    if (!InsertIntoBucket(*data.table[bucket], control, data.overflow)) {
      throw std::runtime_error("Failed to insert entry for key: " + std::to_string(entry.key));
    }
  }
  return data;
}

bool Lookup(const IndexData& data, uint64_t table_size, uint64_t key, uint64_t& out_address) {
  if (table_size == 0) {
    return false;
  }
  uint64_t hash = HashKey(key);
  uint64_t bucket_index = BucketIndex(hash, table_size);
  uint16_t tag = TagFromHash(hash);

  const HotLogIndexHashBucket* bucket = data.table[bucket_index].get();
  while (bucket != nullptr) {
    for (uint32_t i = 0; i < HotLogIndexHashBucket::kNumEntries; ++i) {
      HashBucketEntry entry = bucket->entries[i].load();
      if (entry.control_ == kEntryInvalid) {
        continue;
      }
      HotLogIndexHashBucketEntry index_entry(entry.control_);
      if (index_entry.tag() == tag) {
        out_address = index_entry.address().control();
        return true;
      }
    }

    auto overflow_entry = bucket->overflow_entry.load();
    uint64_t next = overflow_entry.address().control();
    if (next == kOverflowInvalid) {
      return false;
    }
    if (next == 0 || next > data.overflow.size()) {
      return false;
    }
    bucket = data.overflow[next - 1].get();
  }
  return false;
}

void WriteBuckets(const std::filesystem::path& path,
                  const std::vector<std::unique_ptr<HotLogIndexHashBucket>>& buckets) {
  std::ofstream out(path, std::ios::binary);
  if (!out.is_open()) {
    throw std::runtime_error("Failed to open file: " + path.string());
  }
  for (const auto& bucket : buckets) {
    out.write(reinterpret_cast<const char*>(bucket.get()),
              static_cast<std::streamsize>(sizeof(HotLogIndexHashBucket)));
  }
}

std::vector<std::unique_ptr<HotLogIndexHashBucket>> ReadBuckets(
    const std::filesystem::path& path,
    uint64_t count) {
  std::vector<std::unique_ptr<HotLogIndexHashBucket>> buckets;
  if (count == 0) {
    return buckets;
  }
  buckets.reserve(count);
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    throw std::runtime_error("Failed to open file: " + path.string());
  }
  for (uint64_t i = 0; i < count; ++i) {
    auto bucket = std::make_unique<HotLogIndexHashBucket>();
    in.read(reinterpret_cast<char*>(bucket.get()),
            static_cast<std::streamsize>(sizeof(HotLogIndexHashBucket)));
    buckets.push_back(std::move(bucket));
  }
  return buckets;
}

void WriteCheckpoint(const std::filesystem::path& base_dir, const Manifest& manifest) {
  if (!IsPowerOfTwo(manifest.table_size)) {
    throw std::runtime_error("table_size must be power of two");
  }

  Guid token = Guid::Create();
  std::string token_str = token.ToString();

  auto index_dir = base_dir / "index-checkpoints" / token_str;
  auto log_dir = base_dir / "cpr-checkpoints" / token_str;
  std::filesystem::create_directories(index_dir);
  std::filesystem::create_directories(log_dir);

  IndexData data = BuildIndex(manifest);

  IndexMetadata index_meta;
  index_meta.version = manifest.version;
  index_meta.table_size = manifest.table_size;
  index_meta.num_ht_bytes = manifest.table_size * sizeof(HotLogIndexHashBucket);
  index_meta.num_ofb_bytes = data.overflow.size() * sizeof(HotLogIndexHashBucket);
  index_meta.ofb_count = FixedPageAddress{data.overflow.size()};
  index_meta.log_begin_address = Address{manifest.log_begin_address};
  index_meta.checkpoint_start_address = Address{manifest.checkpoint_start_address};

  {
    std::ofstream info(index_dir / "info.dat", std::ios::binary);
    info.write(reinterpret_cast<const char*>(&index_meta), sizeof(index_meta));
  }

  WriteBuckets(index_dir / "ht.dat", data.table);
  WriteBuckets(index_dir / "ofb.dat", data.overflow);

  Manifest manifest_copy = manifest;
  WriteManifest(index_dir / "manifest.txt", manifest_copy);

  LogMetadata log_meta;
  log_meta.use_snapshot_file = true;
  log_meta.version = manifest.version;
  log_meta.num_threads.store(1);
  log_meta.flushed_address = Address{0};
  log_meta.final_address = Address{kDefaultLogFinalAddress};
  log_meta.monotonic_serial_nums[0] = 1;
  log_meta.guids[0] = Guid::Create();

  {
    std::ofstream log_info_file(log_dir / "info.dat", std::ios::binary);
    log_info_file.write(reinterpret_cast<const char*>(&log_meta), sizeof(log_meta));
  }
  {
    std::ofstream snapshot(log_dir / "snapshot.dat", std::ios::binary);
    snapshot.write("", 0);
  }

  std::ofstream token_file(base_dir / "token.txt");
  token_file << token_str << "\n";

  std::cout << "Token: " << token_str << "\n";
  std::cout << "Index dir: " << index_dir << "\n";
  std::cout << "Log dir: " << log_dir << "\n";
}

std::string ReadToken(const std::filesystem::path& base_dir, const std::string& token) {
  if (!token.empty()) {
    return token;
  }
  std::ifstream in(base_dir / "token.txt");
  if (!in.is_open()) {
    throw std::runtime_error("Missing token.txt in " + base_dir.string());
  }
  std::string value;
  std::getline(in, value);
  return Trim(value);
}

IndexData ReadIndexData(const std::filesystem::path& index_dir, const IndexMetadata& metadata) {
  IndexData data;
  data.table = ReadBuckets(index_dir / "ht.dat", metadata.table_size);
  data.overflow = ReadBuckets(index_dir / "ofb.dat", metadata.ofb_count.control());
  return data;
}

void VerifyCheckpoint(const std::filesystem::path& base_dir, const std::string& token) {
  std::string token_str = ReadToken(base_dir, token);
  auto index_dir = base_dir / "index-checkpoints" / token_str;
  auto log_dir = base_dir / "cpr-checkpoints" / token_str;

  Manifest manifest = ReadManifest(index_dir / "manifest.txt");

  IndexMetadata index_meta;
  {
    std::ifstream info(index_dir / "info.dat", std::ios::binary);
    if (!info.is_open()) {
      throw std::runtime_error("Missing info.dat in " + index_dir.string());
    }
    info.read(reinterpret_cast<char*>(&index_meta), sizeof(index_meta));
  }

  IndexData data = ReadIndexData(index_dir, index_meta);

  size_t ok = 0;
  for (const auto& entry : manifest.entries) {
    uint64_t address = 0;
    if (Lookup(data, manifest.table_size, entry.key, address) && address == entry.address) {
      ++ok;
    } else {
      throw std::runtime_error("Lookup failed for key: " + std::to_string(entry.key));
    }
  }

  std::ifstream log_info_file(log_dir / "info.dat", std::ios::binary);
  if (!log_info_file.is_open()) {
    throw std::runtime_error("Missing log info.dat in " + log_dir.string());
  }

  LogMetadata log_meta;
  log_info_file.read(reinterpret_cast<char*>(&log_meta), sizeof(log_meta));
  if (log_meta.version != manifest.version) {
    throw std::runtime_error("Log metadata version mismatch");
  }

  std::cout << "Verified entries: " << ok << "\n";
  std::cout << "Index dir: " << index_dir << "\n";
}

void UpdateCheckpoint(const std::filesystem::path& base_dir,
                      const std::filesystem::path& out_dir,
                      const std::string& token,
                      const std::vector<ManifestEntry>& new_entries) {
  std::string token_str = ReadToken(base_dir, token);
  auto index_dir = base_dir / "index-checkpoints" / token_str;
  Manifest manifest = ReadManifest(index_dir / "manifest.txt");

  for (const auto& entry : new_entries) {
    manifest.entries.push_back(entry);
  }

  WriteCheckpoint(out_dir, manifest);
}

std::string GetArgValue(int argc, char** argv, const std::string& flag) {
  for (int i = 0; i < argc - 1; ++i) {
    if (flag == argv[i]) {
      return argv[i + 1];
    }
  }
  return "";
}

void PrintUsage() {
  std::cout << "Usage:\n"
            << "  faster_cpp_demo generate --out <dir> [--table-size N] [--entries k=a,...]\n"
            << "  faster_cpp_demo verify --dir <dir> [--token <token>]\n"
            << "  faster_cpp_demo update --dir <dir> --out <dir> --entries k=a,... [--token <token>]\n";
}

} // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  std::string command = argv[1];
  try {
    if (command == "generate") {
      std::string out = GetArgValue(argc, argv, "--out");
      if (out.empty()) {
        throw std::runtime_error("--out is required");
      }
      Manifest manifest;
      std::string table_size_arg = GetArgValue(argc, argv, "--table-size");
      if (!table_size_arg.empty()) {
        manifest.table_size = std::stoull(table_size_arg);
      }
      std::string entries_arg = GetArgValue(argc, argv, "--entries");
      auto entries = ParseEntriesArg(entries_arg);
      if (entries.empty()) {
        entries = BuildDefaultEntries(manifest.table_size);
      }
      manifest.entries = entries;
      WriteCheckpoint(out, manifest);
      return 0;
    }

    if (command == "verify") {
      std::string dir = GetArgValue(argc, argv, "--dir");
      if (dir.empty()) {
        throw std::runtime_error("--dir is required");
      }
      std::string token = GetArgValue(argc, argv, "--token");
      VerifyCheckpoint(dir, token);
      return 0;
    }

    if (command == "update") {
      std::string dir = GetArgValue(argc, argv, "--dir");
      std::string out = GetArgValue(argc, argv, "--out");
      std::string entries_arg = GetArgValue(argc, argv, "--entries");
      if (dir.empty() || out.empty() || entries_arg.empty()) {
        throw std::runtime_error("--dir, --out and --entries are required");
      }
      auto entries = ParseEntriesArg(entries_arg);
      std::string token = GetArgValue(argc, argv, "--token");
      UpdateCheckpoint(dir, out, token, entries);
      return 0;
    }

    PrintUsage();
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }
}
