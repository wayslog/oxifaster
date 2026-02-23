// GUID byte-order tests.
//
// Validate Rust uuid byte encoding behavior and compatibility expectations.

use uuid::Uuid;

#[test]
fn test_uuid_byteorder() {
    // Use a fixed UUID to keep the test deterministic.
    // Canonical format: "67e55044-10b1-426f-9247-bb680e5fe0c8"
    let uuid_str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    let uuid = Uuid::parse_str(uuid_str).unwrap();

    println!("UUID: {}", uuid);

    // Method 1: as_u128() - network byte order (big-endian)
    let as_u128 = uuid.as_u128();
    let u128_le_bytes = as_u128.to_le_bytes();
    println!("as_u128(): 0x{:032x}", as_u128);
    println!("as_u128().to_le_bytes(): {:02x?}", &u128_le_bytes);

    // Method 2: as_bytes() - RFC 4122 byte order (big-endian)
    let as_bytes = uuid.as_bytes();
    println!("as_bytes(): {:02x?}", as_bytes);

    // Method 3: to_bytes_le() - little-endian byte order
    let to_bytes_le = uuid.to_bytes_le();
    println!("to_bytes_le(): {:02x?}", &to_bytes_le);

    // Inspect UUID fields.
    let fields = uuid.as_fields();
    println!("\nUUID fields:");
    println!(
        "  d1 (u32): 0x{:08x} = {:?}",
        fields.0,
        fields.0.to_le_bytes()
    );
    println!(
        "  d2 (u16): 0x{:04x} = {:?}",
        fields.1,
        fields.1.to_le_bytes()
    );
    println!(
        "  d3 (u16): 0x{:04x} = {:?}",
        fields.2,
        fields.2.to_le_bytes()
    );
    println!("  d4 (u8[8]): {:02x?}", fields.3);

    // Simulate Windows GUID layout (mixed endian fields).
    let mut windows_guid = [0u8; 16];
    windows_guid[0..4].copy_from_slice(&fields.0.to_le_bytes()); // d1 little-endian
    windows_guid[4..6].copy_from_slice(&fields.1.to_le_bytes()); // d2 little-endian
    windows_guid[6..8].copy_from_slice(&fields.2.to_le_bytes()); // d3 little-endian
    windows_guid[8..16].copy_from_slice(fields.3); // d4 unchanged
    println!("\nWindows GUID layout: {:02x?}", &windows_guid);

    // Compare different encodings.
    println!("\nComparison:");
    println!(
        "  as_bytes() == Windows GUID: {}",
        as_bytes == &windows_guid
    );
    println!(
        "  to_bytes_le() == Windows GUID: {}",
        to_bytes_le == windows_guid
    );
    println!(
        "  as_u128().to_le_bytes() == Windows GUID: {}",
        u128_le_bytes == windows_guid
    );
}

#[test]
fn test_uuid_roundtrip_with_c_compat() {
    // Verify round-trip conversions with a deterministic UUID.
    let original_uuid = Uuid::parse_str("3f2504e0-4f89-41d3-9a0c-0305e82c3301").unwrap();
    println!("Original UUID: {}", original_uuid);

    // Incorrect method: as_u128() + to_le_bytes()
    let wrong_bytes = original_uuid.as_u128().to_le_bytes();

    // Correct method 1: to_bytes_le()
    let correct_bytes_le = original_uuid.to_bytes_le();

    // Correct method 2: as_bytes() (RFC 4122 compatible)
    let correct_bytes_rfc = original_uuid.as_bytes();

    println!("\nSerialization:");
    println!("  Wrong (as_u128 + to_le): {:02x?}", &wrong_bytes);
    println!("  Correct (to_bytes_le): {:02x?}", &correct_bytes_le);
    println!("  Correct (as_bytes, RFC): {:02x?}", correct_bytes_rfc);

    // Deserialization checks.
    println!("\nDeserialization:");

    // Deserialize from as_u128().to_le_bytes().
    let from_wrong = u128::from_le_bytes(wrong_bytes);
    let uuid_from_wrong = Uuid::from_u128(from_wrong);
    println!("  From wrong bytes: {}", uuid_from_wrong);

    // Deserialize from to_bytes_le().
    let uuid_from_le = Uuid::from_bytes_le(correct_bytes_le);
    println!("  From le bytes: {}", uuid_from_le);

    // Deserialize from as_bytes().
    let uuid_from_rfc = Uuid::from_bytes(*correct_bytes_rfc);
    println!("  From RFC bytes: {}", uuid_from_rfc);

    // Validate round-trip.
    assert_eq!(uuid_from_le, original_uuid, "to_bytes_le roundtrip failed");
    assert_eq!(uuid_from_rfc, original_uuid, "as_bytes roundtrip failed");

    // The incorrect method may produce a different UUID.
    if uuid_from_wrong != original_uuid {
        println!("\nWARNING: as_u128() roundtrip produces different UUID!");
        println!("  Original:  {}", original_uuid);
        println!("  Roundtrip: {}", uuid_from_wrong);
    }
}
