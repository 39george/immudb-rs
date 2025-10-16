use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, Ident,
    LitStr, Path,
};

/// Build named SQL parameters for immudb queries.
///
/// Types that implement `ToParams` can be passed directly into
/// `SqlClient::exec` and `SqlClient::query` as the `params` argument:
///
/// ```ignore
/// #[derive(ToParams)]
/// struct InsertUser { id: Uuid, name: String }
///
/// let ins = InsertUser { id, name: "alice".into() };
/// client.exec("INSERT INTO users(id, name) VALUES (@id, @name)", &ins).await?;
/// ```
///
/// See `to_params_derive` for field-level attributes:
/// - `#[sql(rename = "...")]`
/// - `#[sql(skip)]`
/// - `#[sql(skip_if_none)]`
#[proc_macro_derive(ToParams, attributes(sql))]
pub fn derive_to_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // ==== 1) Путь к крейту (по умолчанию ::immudb_rs), можно переопределить #[sql(crate="::mycrate")]
    let mut crate_path: Path =
        syn::parse_str("::immudb_rs").expect("crate path");

    for attr in &input.attrs {
        if attr.path().is_ident("sql") {
            let res = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("crate") {
                    let lit: LitStr = meta.value()?.parse()?;
                    let p: Path =
                        syn::parse_str(&lit.value()).map_err(|e| {
                            meta.error(format!("invalid crate path: {e}"))
                        })?;
                    crate_path = p;
                    Ok(())
                } else {
                    // игнорируем незнакомые флаги на типе
                    Ok(())
                }
            });
            if let Err(e) = res {
                return e.to_compile_error().into();
            }
        }
    }

    // ==== 2) Поддерживаем только структуры с именованными полями
    let (fields_named, where_clause) = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(n) => (n, input.generics.where_clause.clone()),
            _ => {
                return syn::Error::new(
                    s.fields.span(),
                    "ToParams supports only structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new(
                input.span(),
                "ToParams can be derived only for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut bind_stmts = Vec::new();

    for f in &fields_named.named {
        let field_ident: &Ident = match &f.ident {
            Some(id) => id,
            None => {
                return syn::Error::new(f.span(), "named fields expected")
                    .to_compile_error()
                    .into();
            }
        };

        // Значения по умолчанию для атрибутов поля
        let mut skip = false;
        let mut rename: Option<String> = None;
        let mut skip_if_none = false;

        for attr in &f.attrs {
            if attr.path().is_ident("sql") {
                let res = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("skip") {
                        skip = true;
                        Ok(())
                    } else if meta.path.is_ident("skip_if_none") {
                        skip_if_none = true;
                        Ok(())
                    } else if meta.path.is_ident("rename") {
                        let lit: LitStr = meta.value()?.parse()?;
                        rename = Some(lit.value());
                        Ok(())
                    } else {
                        // незнакомые поля игнорируем, но можно и ругаться:
                        // Err(meta.error("unsupported attribute"))
                        Ok(())
                    }
                });
                if let Err(e) = res {
                    return e.to_compile_error().into();
                }
            }
        }

        if skip {
            continue;
        }

        let param_name = rename.unwrap_or_else(|| field_ident.to_string());

        // Если стоит #[sql(skip_if_none)] и тип поля Option<T> — генерим if let Some(...)
        let is_option = is_option_type(&f.ty);

        if skip_if_none && is_option {
            bind_stmts.push(quote! {
                if let Some(v) = &self.#field_ident {
                    p = p.bind(#param_name, v.clone());
                }
            });
        } else {
            // обычный случай — просто clone() (Params::bind сейчас требует owned значения)
            bind_stmts.push(quote! {
                p = p.bind(#param_name, self.#field_ident.clone());
            });
        }
    }

    let ty = &input.ident;
    let (impl_generics, ty_generics, wc) = input.generics.split_for_impl();
    let wc = where_clause.as_ref().map(|w| w as &dyn ToTokens);

    let expanded = quote! {
        impl #impl_generics #crate_path::sql::ToParams for #ty #ty_generics #wc {
            fn to_params(&self) -> #crate_path::sql::Params {
                let mut p = #crate_path::sql::Params::new();
                #(#bind_stmts)*
                p
            }
        }
    };

    TokenStream::from(expanded)
}

// Простая проверка: Option<T>?
fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(tp) = ty {
        if tp.qself.is_none() {
            if let Some(seg) = tp.path.segments.last() {
                return seg.ident == "Option";
            }
        }
    }
    false
}
