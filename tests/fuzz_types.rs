mod fuzz_util;

use oxifaster::codec::{Bincode, PersistValue, Utf8, ValueCodec};
use oxifaster::varlen::{SpanByte, SpanByteBuilder};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Small {
    a: u32,
    b: u64,
}

#[test]
fn fuzz_types_smoke_spanbyte_roundtrip() {
    let p = fuzz_util::params("types_spanbyte_smoke", 5_000, 1);
    let mut rng = fuzz_util::rng(p.seed);

    for _ in 0..p.steps {
        let len = rng.gen_range(0usize..=4096);
        let mut payload = vec![0u8; len];
        rng.fill(&mut payload[..]);

        let mut span = SpanByte::from_vec(payload.clone());
        if rng.gen_bool(0.5) {
            span.set_metadata(rng.r#gen::<u64>());
            assert!(span.has_metadata());
        }

        let mut buf = vec![0u8; span.total_size()];
        let written = span.serialize_to(&mut buf);
        assert_eq!(written, buf.len());

        let restored = SpanByte::deserialize_from(&buf).expect("deserialize");
        assert_eq!(restored.as_slice(), payload.as_slice());
        assert_eq!(restored.metadata(), span.metadata());
    }

    // Builder path
    let built = SpanByteBuilder::new()
        .append(b"hello")
        .push(b' ')
        .append(b"world")
        .metadata(123)
        .build();
    assert_eq!(built.to_string_lossy(), "hello world");
    assert_eq!(built.metadata(), Some(123));
}

#[test]
#[ignore]
fn fuzz_types_stress_spanbyte_large_payloads() {
    let p = fuzz_util::params("types_spanbyte_stress", 50_000, 1);
    let mut rng = fuzz_util::rng(p.seed);

    for _ in 0..p.steps {
        let len = rng.gen_range(0usize..=64 * 1024);
        let mut payload = vec![0u8; len];
        rng.fill(&mut payload[..]);

        let mut span = SpanByte::from_vec(payload.clone());
        if rng.gen_ratio(1, 4) {
            span.set_metadata(rng.r#gen::<u64>());
        }

        let mut buf = vec![0u8; span.total_size()];
        span.serialize_to(&mut buf);
        let restored = SpanByte::deserialize_from(&buf).expect("deserialize");
        assert_eq!(restored.as_slice(), payload.as_slice());
    }
}

#[test]
fn fuzz_types_smoke_codecs_bincode_utf8() {
    let p = fuzz_util::params("types_codecs_smoke", 10_000, 1);
    let mut rng = fuzz_util::rng(p.seed);

    for _ in 0..p.steps {
        let s = format!("user:{}:{}", rng.r#gen::<u32>(), rng.r#gen::<u16>());
        let u = Utf8::from(s.as_str());
        assert_eq!(u.0, s);

        let x = Small {
            a: rng.r#gen(),
            b: rng.r#gen(),
        };
        let wrapped = Bincode(x.clone());
        // Round-trip through the codec API.
        let len = <Bincode<Small> as PersistValue>::Codec::encoded_len(&wrapped).expect("len");
        let mut enc = vec![0u8; len];
        <Bincode<Small> as PersistValue>::Codec::encode_into(&wrapped, &mut enc).expect("encode");
        let dec = <Bincode<Small> as PersistValue>::Codec::decode(&enc).expect("decode");
        assert_eq!(dec.0, x);
    }
}
