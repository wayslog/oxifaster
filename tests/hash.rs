use oxifaster::codec::hash64;

#[test]
fn test_hash64_is_deterministic() {
    let h1 = hash64(b"abc");
    let h2 = hash64(b"abc");
    let h3 = hash64(b"abcd");

    assert_eq!(h1, h2);
    assert_ne!(h1, h3);
    assert_ne!(h1, 0);
}
