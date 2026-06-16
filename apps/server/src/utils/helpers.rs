pub fn generate_id() -> i64 {
    // rand::rng().gen_range(1_000_000_000..i64::MAX)
    let min_val: u64 = 1_000_000_000;
    let max_val: u64 = i64::MAX as u64;
    let rng = rand::random_range(min_val..=max_val) as i64;
    return rng;
}
