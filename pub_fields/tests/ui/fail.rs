// put the struct in its own module
mod inner {
    #[cfg_attr(test, pub_fields::pub_fields)]
    pub struct Bar {
        a: i32, // private field if cfg(test)=false
    }
}

// crate-root (a sibling module) tries to construct it
fn main() {
    let _b = inner::Bar { a: 0 };
    // ^^^^^  error[E0451]: field `a` of struct `inner::Bar` is private
}
