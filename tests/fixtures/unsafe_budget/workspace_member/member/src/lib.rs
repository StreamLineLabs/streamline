pub fn member_unsafe_is_counted() {
    unsafe { std::ptr::read_volatile(&0_u8) };
}
