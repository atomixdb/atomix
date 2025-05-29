// pub_fields/src/lib.rs
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, ItemStruct};

#[proc_macro_attribute]
pub fn pub_fields(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item);

    match item {
        syn::Item::Struct(mut s) => {
            // struct becomes `pub`
            s.vis = parse_quote!(pub);

            // every field becomes `pub`
            for f in &mut s.fields {
                f.vis = parse_quote!(pub);
            }

            quote!(#s).into()
        }
        syn::Item::Enum(mut e) => {
            // enum becomes `pub`
            e.vis = parse_quote!(pub);

            quote!(#e).into()
        }
        _ => {
            // Return original item if not struct or enum
            quote!(#item).into()
        }
    }
}
