#[test]
fn test() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/ok.rs"); // should compile
    t.compile_fail("tests/ui/fail.rs"); // should fail
}
