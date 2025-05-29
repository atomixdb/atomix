// put the struct in its own module
mod inner {
    #[pub_fields::pub_fields]
    pub struct Bar {
        a: i32,
    }
}

// crate-root (a sibling module) tries to construct it
fn main() {
    let _b = inner::Bar { a: 0 };
}
