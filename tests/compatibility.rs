// 兼容性测试模块
//
// 测试 oxifaster 与 C++ FASTER 的二进制兼容性

#[cfg(test)]
#[path = "compatibility/test_record_format.rs"]
mod test_record_format;

#[cfg(test)]
#[path = "compatibility/test_checkpoint_format.rs"]
mod test_checkpoint_format;

#[cfg(test)]
#[path = "compatibility/test_hash_index.rs"]
mod test_hash_index;

#[cfg(test)]
#[path = "compatibility/test_hash.rs"]
mod test_hash;

#[cfg(test)]
#[path = "compatibility/test_format_detection.rs"]
mod test_format_detection;

#[cfg(test)]
#[path = "compatibility/test_span_byte.rs"]
mod test_span_byte;

#[cfg(test)]
#[path = "compatibility/test_f2.rs"]
mod test_f2;

#[cfg(test)]
#[path = "compatibility/test_roundtrip.rs"]
mod test_roundtrip;
