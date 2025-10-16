use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::borrow::Cow;
use std::pin::Pin;
use time::{OffsetDateTime, UtcOffset};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status};
use uuid::Uuid;

use crate::Result;
use crate::client::ImmuDB;
use crate::error::Error;
use crate::interceptor::SessionInterceptor;
use crate::protocol::schema::{
    NamedParam, SqlExecRequest, SqlExecResult, SqlQueryRequest, SqlValue,
    immu_service_client::ImmuServiceClient, sql_value,
};
use crate::schema::{NewTxRequest, NewTxResponse, TxMode};

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ToParams {
    fn to_params(&self) -> crate::sql::Params;
}

impl<T: ToParams + ?Sized> From<&T> for Params {
    fn from(t: &T) -> Self {
        t.to_params()
    }
}

/// Request params (@name -> SqlValue)
#[derive(Debug, Clone)]
pub enum SqlArg<'a> {
    Null,
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    Ts(i64),
}

#[macro_export]
macro_rules! impl_from_for_sqlarg {
    ($t:ty, $body:expr) => {
        impl From<$t> for $crate::sql::SqlArg<'_> {
            fn from(v: $t) -> Self {
                ($body)(v)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_from_for_sqlarg_borrowed {
    ($lt:lifetime, $t:ty, $body:expr) => {
        impl<$lt> From<$t> for $crate::sql::SqlArg<$lt> {
            fn from(v: $t) -> Self {
                ($body)(v)
            }
        }
    };
}

impl_from_for_sqlarg!(i64, |n| SqlArg::I64(n));
impl_from_for_sqlarg!(i32, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(i16, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(i8, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(bool, |b| SqlArg::Bool(b));
impl_from_for_sqlarg!(f64, |f| SqlArg::F64(f));
impl_from_for_sqlarg!(String, |s| SqlArg::Str(Cow::Owned(s)));
impl_from_for_sqlarg!(Vec<u8>, |b| SqlArg::Bytes(Cow::Owned(b)));
impl_from_for_sqlarg!(Uuid, |u: Uuid| SqlArg::Bytes(Cow::Owned(
    u.as_bytes().to_vec()
)));
impl_from_for_sqlarg!(time::OffsetDateTime, |dt: OffsetDateTime| {
    let dt_utc = dt.to_offset(time::UtcOffset::UTC);
    let micros = dt_utc.unix_timestamp_nanos() / 1_000;
    SqlArg::Ts(micros as i64)
});
impl_from_for_sqlarg!(u8, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(u16, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(u32, |n| SqlArg::I64(n as i64));
impl_from_for_sqlarg!(u64, |n| SqlArg::I64(n as i64));

impl_from_for_sqlarg_borrowed!('a, &'a str,  |s| SqlArg::Str(Cow::Borrowed(s)));
impl_from_for_sqlarg_borrowed!('a, &'a [u8], |b| SqlArg::Bytes(Cow::Borrowed(b)));

fn arg_to_sql_value(a: SqlArg<'_>) -> SqlValue {
    let v = match a {
        SqlArg::Null => sql_value::Value::Null(0),
        SqlArg::I64(n) => sql_value::Value::N(n),
        SqlArg::F64(f) => sql_value::Value::F(f),
        SqlArg::Bool(b) => sql_value::Value::B(b),
        SqlArg::Str(s) => sql_value::Value::S(s.into_owned()),
        SqlArg::Bytes(b) => sql_value::Value::Bs(b.into_owned()),
        SqlArg::Ts(ts) => sql_value::Value::Ts(ts),
    };
    SqlValue { value: Some(v) }
}

/// Convenient params collection API
#[derive(Default, Debug, Clone)]
pub struct Params {
    inner: Vec<NamedParam>,
}
impl Params {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }
    /// name — without '@'. In sql use `@name`.
    pub fn bind(
        mut self,
        name: impl Into<String>,
        val: impl Into<SqlArg<'static>>,
    ) -> Self {
        self.inner.push(NamedParam {
            name: name.into(),
            value: Some(arg_to_sql_value(val.into())),
        });
        self
    }
    pub fn bind_dt(
        mut self,
        name: impl Into<String>,
        dt: OffsetDateTime,
    ) -> Self {
        self.inner.push(NamedParam {
            name: name.into(),
            value: Some(arg_to_sql_value(SqlArg::from(dt))),
        });
        self
    }
    pub fn into_inner(self) -> Vec<NamedParam> {
        self.inner
    }
}

/// SELECT-queries results
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<SqlValue>,
}
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Convenient row conversion to JSON-objec (bytes -> base64)
    fn short(name: &str) -> &str {
        name.rsplit('.').next().unwrap_or(name)
    }

    fn normalize_col(mut s: &str) -> String {
        s = s.trim();
        // Trim outer parentheses: "(groups.name)" -> "groups.name"
        loop {
            let b = s.as_bytes();
            if s.len() >= 2 && b[0] == b'(' && b[s.len() - 1] == b')' {
                s = &s[1..s.len() - 1].trim();
            } else {
                break;
            }
        }
        // quotes/backticks/[] at the edges
        s = s.trim_matches(|c: char| {
            c == '"' || c == '`' || c == '[' || c == ']'
        });
        // table.column -> column
        let seg = s.rsplit('.').next().unwrap_or(s).trim();
        // TODO: Do we need that here?
        seg.trim_matches(|c: char| c == ')' || c == '(')
            .trim()
            .to_string()
    }

    pub fn row_as_json(&self, idx: usize) -> Result<serde_json::Value> {
        let row = self
            .rows
            .get(idx)
            .ok_or_else(|| Error::Decode("row out of bounds".into()))?;
        let mut obj = serde_json::Map::new();

        // At first try per-row labels, otherwise - global
        let names: Vec<String> = if !row.columns.is_empty() {
            row.columns.clone()
        } else {
            self.columns.iter().map(|c| c.name.clone()).collect()
        };

        // If there are no names, synthesize colN
        let synth = names.is_empty();
        let total = row.values.len();

        for i in 0..total {
            let raw = if synth {
                format!("col{}", i + 1)
            } else {
                names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col{}", i + 1))
            };
            let key = Self::normalize_col(&raw);

            let v = row.values.get(i).cloned().unwrap_or(
                crate::protocol::schema::SqlValue {
                    value: Some(sql_value::Value::Null(0)),
                },
            );

            obj.insert(key, sql_value_to_json(v));
        }

        Ok(serde_json::Value::Object(obj))
    }

    /// Deserialize all rows into T (using JSON). Fields are matched by column names.
    pub fn rows_as<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let mut out = Vec::with_capacity(self.rows.len());
        for i in 0..self.rows.len() {
            let v = self.row_as_json(i)?;
            let t = serde_json::from_value::<T>(v)?;
            out.push(t);
        }
        Ok(out)
    }

    /// One scalar (first column, first row)
    pub fn scalar<T: TryFrom<SqlValue, Error = Error>>(&self) -> Result<T> {
        let row = self
            .rows
            .first()
            .ok_or_else(|| Error::Decode("empty result".into()))?;
        let v = row
            .values
            .first()
            .cloned()
            .ok_or_else(|| Error::Decode("no columns".into()))?;
        T::try_from(v)
    }

    pub fn first_col_as<T>(&self) -> Result<Vec<T>>
    where
        T: TryFrom<SqlValue, Error = Error>,
    {
        let mut out = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let v =
                row.values.get(0).cloned().ok_or_else(|| {
                    Error::Decode("row has no columns".into())
                })?;
            out.push(T::try_from(v)?);
        }
        Ok(out)
    }

    pub fn one_as<T: DeserializeOwned>(&self) -> Result<T> {
        if self.rows.len() != 1 {
            return Err(Error::Decode(format!(
                "expected 1 row, got {}",
                self.rows.len()
            )));
        }
        let v = self.row_as_json(0)?;
        Ok(serde_json::from_value::<T>(v)?)
    }
}

fn sql_value_to_json(v: SqlValue) -> JsonValue {
    use sql_value::Value::*;
    match v.value {
        Some(Null(_)) => JsonValue::Null,
        Some(N(n)) => JsonValue::from(n),
        Some(F(f)) => JsonValue::from(f),
        Some(B(b)) => JsonValue::from(b),
        Some(S(s)) => JsonValue::from(s),
        Some(Bs(bs)) => JsonValue::String(BASE64_STANDARD.encode(bs)),
        Some(Ts(ts)) => JsonValue::from(ts),
        None => JsonValue::Null,
    }
}

#[macro_export]
macro_rules! impl_tryfrom_sqlvalue {
    ($ty:ty, $expected:expr, $( $pat:pat => $expr:expr ),+ $(,)?) => {
        impl ::core::convert::TryFrom<$crate::protocol::schema::SqlValue> for $ty {
            type Error = $crate::error::Error;
            fn try_from(v: $crate::protocol::schema::SqlValue)
                -> ::core::result::Result<Self, Self::Error>
            {
                use $crate::protocol::schema::sql_value;
                match v.value {
                    $( Some($pat) => Ok($expr), )+
                    other => Err($crate::error::Error::Decode(
                        format!("expected {}, got {:?}", $expected, other)
                    )),
                }
            }
        }
    };
}

impl_tryfrom_sqlvalue!(i64, "i64",
    sql_value::Value::N(n) => n,
);

impl_tryfrom_sqlvalue!(String, "string or bytes(base64)",
    sql_value::Value::S(s)  => s,
    sql_value::Value::Bs(b) => BASE64_STANDARD.encode(b),
);

impl_tryfrom_sqlvalue!(bool, "bool",
    sql_value::Value::B(b) => b,
);

impl_tryfrom_sqlvalue!(f64, "f64",
    sql_value::Value::F(f) => f,
    sql_value::Value::N(n) => n as f64,
);

impl_tryfrom_sqlvalue!(Vec<u8>, "bytes",
    sql_value::Value::Bs(bs) => bs,
);

impl_tryfrom_sqlvalue!(OffsetDateTime, "timestamp (Ts)",
    sql_value::Value::Ts(us) => {
        let ns = (us as i128) * 1_000;
        OffsetDateTime::from_unix_timestamp_nanos(ns)
            .map_err(|e| crate::error::Error::Decode(e.to_string()))?
    },
);

impl_tryfrom_sqlvalue!(uuid::Uuid, "uuid (16 bytes or string)",
    sql_value::Value::Bs(bs) => uuid::Uuid::from_slice(&bs)
        .map_err(|e| crate::error::Error::Decode(e.to_string()))?,
    sql_value::Value::S(s) => uuid::Uuid::parse_str(&s)
        .map_err(|e| crate::error::Error::Decode(e.to_string()))?,
);

/// Client: exec/query/tx API
#[derive(Clone)]
pub struct SqlClient {
    inner: ImmuServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            SessionInterceptor,
        >,
    >,
    tx_id: Option<MetadataValue<Ascii>>,
}

impl SqlClient {
    pub fn new(db: &ImmuDB) -> Self {
        Self {
            inner: db.raw_main(),
            tx_id: None,
        }
    }

    fn req_with_tx<T>(&self, payload: T) -> Request<T> {
        let mut req = Request::new(payload);
        if let Some(tx) = &self.tx_id {
            req.metadata_mut().insert("transactionid", tx.clone());
        }
        req
    }

    /// Execute DDL/DML; can handle multiple expressions at a time (with BEGIN/COMMIT)
    pub async fn exec<P>(
        &mut self,
        sql: impl Into<String>,
        params: P,
    ) -> Result<SqlExecResult>
    where
        P: Into<Params>,
    {
        let req = SqlExecRequest {
            sql: sql.into(),
            params: params.into().into_inner(),
            no_wait: false,
        };
        let resp = if self.tx_id.is_some() {
            let req = self.req_with_tx(req);
            let _ = self.inner.tx_sql_exec(req).await?;
            SqlExecResult::default()
        } else {
            self.inner.sql_exec(req).await?.into_inner()
        };
        Ok(resp)
    }

    /// SELECT; returns a table
    pub async fn query<P>(
        &mut self,
        sql: impl Into<String>,
        params: P,
    ) -> Result<QueryResult>
    where
        P: Into<Params>,
    {
        let req = SqlQueryRequest {
            sql: sql.into(),
            params: params.into().into_inner(),
            accept_stream: true,
            ..Default::default()
        };
        let req = self.req_with_tx(req);
        let mut stream = if self.tx_id.is_some() {
            self.inner.tx_sql_query(req).await?.into_inner()
        } else {
            self.inner.sql_query(req).await?.into_inner()
        };

        let mut columns_meta: Vec<Column> = Vec::new();
        let mut rows: Vec<Row> = Vec::new();

        while let Some(chunk) = stream.message().await? {
            if columns_meta.is_empty() && !chunk.columns.is_empty() {
                columns_meta = chunk
                    .columns
                    .into_iter()
                    .map(|c| Column {
                        name: c.name,
                        r#type: c.r#type,
                    })
                    .collect();
            }
            rows.extend(chunk.rows.into_iter().map(|r| Row {
                columns: r.columns,
                values: r.values,
            }));
        }

        Ok(QueryResult {
            columns: columns_meta,
            rows,
        })
    }

    pub async fn query_scalar<T>(
        &mut self,
        sql: impl Into<String>,
        params: Params,
    ) -> Result<T>
    where
        T: TryFrom<SqlValue, Error = Error>,
    {
        self.query(sql, params).await?.scalar()
    }

    /// Convenience: struct mapping (serde)
    pub async fn query_as<T: DeserializeOwned>(
        &mut self,
        sql: impl Into<String>,
        params: Params,
    ) -> Result<Vec<T>> {
        self.query(sql, params).await?.rows_as::<T>()
    }

    pub async fn query_col<T, P>(
        &mut self,
        sql: impl Into<String>,
        params: P,
    ) -> Result<Vec<T>>
    where
        P: Into<Params>,
        T: TryFrom<SqlValue, Error = Error>,
    {
        let qr = self.query(sql, params).await?;
        qr.first_col_as()
    }

    pub async fn query_one_as<T, P>(
        &mut self,
        sql: impl Into<String>,
        params: P,
    ) -> Result<T>
    where
        P: Into<Params>,
        T: DeserializeOwned,
    {
        let qr = self.query(sql, params).await?;
        qr.one_as()
    }

    /// Simple transaction (server keeps ongoing_tx in session)
    #[tracing::instrument(skip_all)]
    pub async fn begin(&mut self, mode: TxMode) -> Result<()> {
        let NewTxResponse { transaction_id } = self
            .inner
            .new_tx(NewTxRequest {
                mode: mode.into(),
                ..Default::default()
            })
            .await?
            .into_inner();

        let tx_md = MetadataValue::try_from(transaction_id).map_err(|_| {
            Error::Unexpected("invalid tx id (non-ASCII)".into())
        })?;
        self.tx_id = Some(tx_md);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn commit(&mut self) -> Result<()> {
        if self.tx_id.is_none() {
            return Ok(());
        }
        let req = self.req_with_tx(());
        let _ = self.inner.commit(req).await?;
        self.tx_id = None;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn rollback(&mut self) -> Result<()> {
        if self.tx_id.is_none() {
            return Ok(());
        }
        let req = self.req_with_tx(());
        let _ = self.inner.rollback(req).await;
        self.tx_id = None;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn with_tx<T, F>(&mut self, mode: TxMode, f: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut SqlClient) -> BoxFut<'a, T>,
    {
        self.begin(mode).await?;
        match f(self).await {
            Ok(v) => {
                self.commit().await?;
                Ok(v)
            }
            Err(e) => {
                let _ = self.rollback().await;
                Err(e)
            }
        }
    }
}
