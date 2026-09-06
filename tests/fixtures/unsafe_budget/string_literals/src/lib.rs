pub fn literal_delimiters_do_not_change_lexing() {
    let normal = "topics/* // SAFETY:"; unsafe { std::ptr::read_volatile(&normal as *const _) };
    let raw = r#"// still a string with /* and */"#;
    let byte = b"/* not a comment */";
    let raw_byte = br##"// not a comment either"##;
    let slash = '/';
    let quote = '\'';
    let lifetime: &'static str = normal;
    let keyword = "unsafe { hidden in a string }";

    let _ = (raw, byte, raw_byte, slash, quote, lifetime, keyword);
}
