// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use google_cloud_bigquery::model::ProtoRows;
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, MessageDescriptor, Value};
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::fmt;

#[inline]
pub fn put_varint(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

#[inline]
pub fn put_bytes_with_tag(buf: &mut BytesMut, tag: u32, data: &[u8]) {
    put_varint(buf, tag as u64);
    put_varint(buf, data.len() as u64);
    buf.put_slice(data);
}

#[inline]
pub fn put_varint_with_tag(buf: &mut BytesMut, tag: u32, val: u64) {
    put_varint(buf, tag as u64);
    put_varint(buf, val);
}

/// Computes the serialized `ProtoRows` wire size (`repeated bytes serialized_rows = 1`).
pub fn proto_rows_wire_size(rows: &[Bytes]) -> usize {
    rows.iter()
        .map(|r| {
            let mut len = r.len();
            let mut varint_len = 1;
            while len >= 0x80 {
                varint_len += 1;
                len >>= 7;
            }
            1 + varint_len + r.len()
        })
        .sum()
}

/// Howard Hinnant's civil-from-days inverse: days since 1970-01-01.
pub fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i32> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y_adj = y - i32::from(m <= 2);
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = (y_adj - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe as i32 - 719_468)
}

fn parse_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut v = 0u32;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as u32)?;
    }
    Some(v)
}

/// Parses `YYYY-[M]M-[D]D` -> days since 1970-01-01.
pub fn parse_bq_date(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    let p1 = b.iter().position(|&c| c == b'-')?;
    let rest = &b[p1 + 1..];
    let p2 = rest.iter().position(|&c| c == b'-')?;
    if p1 != 4 {
        return None;
    }
    let y = parse_u32(&b[..p1])? as i32;
    let m = parse_u32(&rest[..p2])?;
    let d = parse_u32(&rest[p2 + 1..])?;
    days_from_civil(y, m, d)
}

/// Parses `[H]H:[M]M:[S]S[.ffffff]` -> `(hour, minute, second, micros)`.
fn parse_hms_micros(b: &[u8]) -> Option<(u32, u32, u32, u32)> {
    let c1 = b.iter().position(|&c| c == b':')?;
    let r1 = &b[c1 + 1..];
    let c2 = r1.iter().position(|&c| c == b':')?;
    let r2 = &r1[c2 + 1..];
    let h = parse_u32(&b[..c1])?;
    let mi = parse_u32(&r1[..c2])?;
    let (sec_bytes, us) = if let Some(dot) = r2.iter().position(|&c| c == b'.') {
        let frac = &r2[dot + 1..];
        if frac.is_empty() || frac.len() > 6 {
            return None;
        }
        let mut us = parse_u32(frac)?;
        for _ in frac.len()..6 {
            us *= 10;
        }
        (&r2[..dot], us)
    } else {
        (r2, 0)
    };
    let s = parse_u32(sec_bytes)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some((h, mi, s, us))
}

/// Parses `YYYY-[M]M-[D]D( |T|t)[H]H:[M]M:[S]S[.ffffff]` -> BigQuery packed civil datetime `int64`.
pub fn parse_bq_datetime_packed(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let sep = b
        .iter()
        .position(|&c| c == b'T' || c == b't' || c == b' ')?;
    let db = &b[..sep];
    let p1 = db.iter().position(|&c| c == b'-')?;
    let rest = &db[p1 + 1..];
    let p2 = rest.iter().position(|&c| c == b'-')?;
    if p1 != 4 {
        return None;
    }
    let y = parse_u32(&db[..p1])? as i64;
    let mo = parse_u32(&rest[..p2])? as i64;
    let d = parse_u32(&rest[p2 + 1..])? as i64;
    days_from_civil(y as i32, mo as u32, d as u32)?;
    let (h, mi, sec, us) = parse_hms_micros(&b[sep + 1..])?;
    Some(
        (((y << 26)
            | (mo << 22)
            | (d << 17)
            | ((h as i64) << 12)
            | ((mi as i64) << 6)
            | (sec as i64))
            << 20)
            | (us as i64),
    )
}

/// Parses RFC 3339 (`YYYY-MM-DDTHH:MM:SS[.ffffff](Z|+HH:MM|-HH:MM)`) or integer micros -> epoch microseconds.
pub fn parse_bq_timestamp_micros(s: &str) -> Option<i64> {
    if let Ok(v) = s.parse::<i64>() {
        return Some(v);
    }
    let b = s.as_bytes();
    let sep = b
        .iter()
        .position(|&c| c == b'T' || c == b't' || c == b' ')?;
    let days = parse_bq_date(std::str::from_utf8(&b[..sep]).ok()?)? as i64;
    let tail = &b[sep + 1..];
    let (hms_slice, offset_secs) =
        if let Some(stripped) = tail.strip_suffix(b"Z").or_else(|| tail.strip_suffix(b"z")) {
            (stripped, 0i64)
        } else if let Some(pos) = tail.iter().rposition(|&c| c == b'+' || c == b'-') {
            let sign = if tail[pos] == b'+' { 1i64 } else { -1i64 };
            let tz = &tail[pos + 1..];
            let (tzh, tzm) = if let Some(c) = tz.iter().position(|&x| x == b':') {
                (parse_u32(&tz[..c])?, parse_u32(&tz[c + 1..])?)
            } else if tz.len() == 2 {
                (parse_u32(tz)?, 0)
            } else {
                return None;
            };
            (
                &tail[..pos],
                sign * ((tzh as i64) * 3600 + (tzm as i64) * 60),
            )
        } else {
            (tail, 0i64)
        };
    let (h, mi, sec, us) = parse_hms_micros(hms_slice)?;
    let total_secs =
        days * 86_400 + (h as i64) * 3600 + (mi as i64) * 60 + (sec as i64) - offset_secs;
    Some(total_secs * 1_000_000 + (us as i64))
}

pub enum ColumnKind {
    Int64,
    Bool,
    String,
    Numeric,
    Timestamp,
    Date,
    Datetime,
    Json,
    Struct(MessagePlan),
}

pub struct ColumnPlan {
    pub name: Box<str>,
    pub tag: u32,
    pub repeated: bool,
    pub kind: ColumnKind,
    pub field_desc: FieldDescriptor,
}

pub struct MessagePlan {
    pub columns: Box<[ColumnPlan]>,
    pub by_lower: HashMap<Box<str>, usize>,
    pub msg_desc: MessageDescriptor,
}

impl MessagePlan {
    #[inline]
    pub fn lookup_key(&self, key: &str) -> Option<usize> {
        if self.columns.len() <= 24 {
            self.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(key))
        } else if key.bytes().all(|b| !b.is_ascii_uppercase()) {
            self.by_lower.get(key).copied()
        } else {
            let lower = key.to_ascii_lowercase();
            self.by_lower.get(lower.as_str()).copied()
        }
    }
}

pub struct DirectState {
    pub scratch: BytesMut,
}

impl DirectState {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            scratch: BytesMut::with_capacity(cap),
        }
    }
}

struct MessageSeed<'a> {
    plan: &'a MessagePlan,
    out: &'a mut BytesMut,
    state: &'a mut DirectState,
}

impl<'de> DeserializeSeed<'de> for MessageSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for MessageSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        debug_assert!(self.plan.columns.len() <= u64::BITS as usize);
        let mut seen = 0u64;
        while let Some(key) = map.next_key::<&str>()? {
            let idx = self
                .plan
                .lookup_key(key)
                .ok_or_else(|| de::Error::custom("unknown column"))?;
            let bit = 1u64 << idx;
            if (seen & bit) != 0 {
                return Err(de::Error::custom("duplicate column"));
            }
            seen |= bit;
            let col = &self.plan.columns[idx];
            if col.repeated {
                map.next_value_seed(RepeatedSeed {
                    col,
                    out: self.out,
                    state: self.state,
                })?;
            } else {
                map.next_value_seed(OptionalFieldSeed {
                    col,
                    out: self.out,
                    state: self.state,
                })?;
            }
        }
        Ok(())
    }
}

struct OptionalFieldSeed<'a> {
    col: &'a ColumnPlan,
    out: &'a mut BytesMut,
    state: &'a mut DirectState,
}

impl<'de> DeserializeSeed<'de> for OptionalFieldSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        match &self.col.kind {
            ColumnKind::Numeric => {
                if let Some(raw) =
                    <Option<&'de RawValue> as serde::Deserialize>::deserialize(deserializer)?
                {
                    let s = raw.get();
                    if s.starts_with('"') {
                        let inner: &str = serde_json::from_str(s)
                            .map_err(|_| de::Error::custom("bad numeric"))?;
                        put_bytes_with_tag(self.out, self.col.tag, inner.as_bytes());
                    } else {
                        put_bytes_with_tag(self.out, self.col.tag, s.as_bytes());
                    }
                }
                Ok(())
            }
            ColumnKind::Json => {
                if let Some(raw) =
                    <Option<&'de RawValue> as serde::Deserialize>::deserialize(deserializer)?
                {
                    let s = raw.get();
                    if s.starts_with('"') {
                        let inner: std::borrow::Cow<'de, str> = serde_json::from_str(s)
                            .map_err(|_| de::Error::custom("bad json str"))?;
                        serde_json::from_str::<IgnoredAny>(&inner)
                            .map_err(|_| de::Error::custom("invalid json content"))?;
                        put_bytes_with_tag(self.out, self.col.tag, inner.as_bytes());
                    } else {
                        put_bytes_with_tag(self.out, self.col.tag, s.as_bytes());
                    }
                }
                Ok(())
            }
            _ => deserializer.deserialize_any(ScalarOrStructVisitor {
                col: self.col,
                allow_null: true,
                out: self.out,
                state: self.state,
            }),
        }
    }
}

struct RepeatedSeed<'a> {
    col: &'a ColumnPlan,
    out: &'a mut BytesMut,
    state: &'a mut DirectState,
}

impl<'de> DeserializeSeed<'de> for RepeatedSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RepeatedSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON array or null")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while seq
            .next_element_seed(ElementSeed {
                col: self.col,
                out: self.out,
                state: self.state,
            })?
            .is_some()
        {}
        Ok(())
    }
}

struct ElementSeed<'a> {
    col: &'a ColumnPlan,
    out: &'a mut BytesMut,
    state: &'a mut DirectState,
}

impl<'de> DeserializeSeed<'de> for ElementSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(ScalarOrStructVisitor {
            col: self.col,
            allow_null: false,
            out: self.out,
            state: self.state,
        })
    }
}

struct ScalarOrStructVisitor<'a> {
    col: &'a ColumnPlan,
    allow_null: bool,
    out: &'a mut BytesMut,
    state: &'a mut DirectState,
}

impl<'de> Visitor<'de> for ScalarOrStructVisitor<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a column value")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        if self.allow_null {
            Ok(())
        } else {
            Err(E::custom("null in repeated column"))
        }
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<(), E> {
        match &self.col.kind {
            ColumnKind::Bool => {
                put_varint_with_tag(self.out, self.col.tag, u64::from(v));
                Ok(())
            }
            ColumnKind::String => {
                put_bytes_with_tag(self.out, self.col.tag, if v { b"true" } else { b"false" });
                Ok(())
            }
            _ => Err(E::custom("unexpected bool")),
        }
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<(), E> {
        match &self.col.kind {
            ColumnKind::Int64 | ColumnKind::Timestamp => {
                put_varint_with_tag(self.out, self.col.tag, v as u64);
                Ok(())
            }
            ColumnKind::Date => {
                let d = i32::try_from(v).map_err(|_| E::custom("date out of range"))?;
                put_varint_with_tag(self.out, self.col.tag, d as i64 as u64);
                Ok(())
            }
            _ => Err(E::custom("unexpected int")),
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<(), E> {
        let iv = i64::try_from(v).map_err(|_| E::custom("int64 overflow"))?;
        self.visit_i64(iv)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<(), E> {
        match &self.col.kind {
            ColumnKind::String => {
                put_bytes_with_tag(self.out, self.col.tag, v.as_bytes());
                Ok(())
            }
            ColumnKind::Int64 => {
                let n = v.parse::<i64>().map_err(|_| E::custom("bad int64"))?;
                put_varint_with_tag(self.out, self.col.tag, n as u64);
                Ok(())
            }
            ColumnKind::Bool => {
                let b = if v.eq_ignore_ascii_case("true") {
                    true
                } else if v.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(E::custom("bad bool"));
                };
                put_varint_with_tag(self.out, self.col.tag, u64::from(b));
                Ok(())
            }
            ColumnKind::Date => {
                let d = parse_bq_date(v).ok_or_else(|| E::custom("bad date"))?;
                put_varint_with_tag(self.out, self.col.tag, d as i64 as u64);
                Ok(())
            }
            ColumnKind::Datetime => {
                let dt = parse_bq_datetime_packed(v).ok_or_else(|| E::custom("bad datetime"))?;
                put_varint_with_tag(self.out, self.col.tag, dt as u64);
                Ok(())
            }
            ColumnKind::Timestamp => {
                let ts = parse_bq_timestamp_micros(v).ok_or_else(|| E::custom("bad timestamp"))?;
                put_varint_with_tag(self.out, self.col.tag, ts as u64);
                Ok(())
            }
            _ => Err(E::custom("unexpected string")),
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> std::result::Result<(), A::Error> {
        match &self.col.kind {
            ColumnKind::Struct(sub_plan) => {
                let mark = self.state.scratch.len();
                let mut child_buf = self.state.scratch.split_off(mark);
                let res = (MessageSeed {
                    plan: sub_plan,
                    out: &mut child_buf,
                    state: self.state,
                })
                .visit_map(map);
                res?;
                put_bytes_with_tag(self.out, self.col.tag, &child_buf);
                child_buf.clear();
                self.state.scratch.unsplit(child_buf);
                Ok(())
            }
            _ => Err(de::Error::custom("unexpected object")),
        }
    }
}

#[inline]
pub fn encode_row_direct(
    plan: &MessagePlan,
    json: &str,
    out: &mut BytesMut,
    state: &mut DirectState,
) -> Result<()> {
    let mut de = serde_json::Deserializer::from_str(json);
    MessageSeed { plan, out, state }.deserialize(&mut de)?;
    de.end()?;
    Ok(())
}

pub fn encode_batch_direct(
    plan: &MessagePlan,
    rows: &[String],
    state: &mut DirectState,
) -> Result<ProtoRows> {
    let mut buf = BytesMut::with_capacity(rows.len() * 256);
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        encode_row_direct(plan, r, &mut buf, state)?;
        out.push(buf.split().freeze());
    }
    Ok(ProtoRows::new().set_serialized_rows(out))
}

// --- Path B: Hybrid DeserializeSeed -> prost_reflect::DynamicMessage -> ProtoRows ---

struct DynamicSeed<'a> {
    plan: &'a MessagePlan,
    msg: &'a mut DynamicMessage,
}

impl<'de> DeserializeSeed<'de> for DynamicSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for DynamicSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        debug_assert!(self.plan.columns.len() <= u64::BITS as usize);
        let mut seen = 0u64;
        while let Some(key) = map.next_key::<&str>()? {
            let idx = self
                .plan
                .lookup_key(key)
                .ok_or_else(|| de::Error::custom("unknown column"))?;
            let bit = 1u64 << idx;
            if (seen & bit) != 0 {
                return Err(de::Error::custom("duplicate column"));
            }
            seen |= bit;
            let col = &self.plan.columns[idx];
            if col.repeated {
                if let Some(list) = map.next_value_seed(DynamicListSeed(col))? {
                    self.msg.set_field(&col.field_desc, Value::List(list));
                }
            } else if let Some(val) = map.next_value_seed(DynamicValueSeed {
                col,
                allow_null: true,
            })? {
                self.msg.set_field(&col.field_desc, val);
            }
        }
        Ok(())
    }
}

struct DynamicListSeed<'a>(&'a ColumnPlan);

impl<'de> DeserializeSeed<'de> for DynamicListSeed<'_> {
    type Value = Option<Vec<Value>>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Self::Value, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for DynamicListSeed<'_> {
    type Value = Option<Vec<Value>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON array or null")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element_seed(DynamicValueSeed {
            col: self.0,
            allow_null: false,
        })? {
            if let Some(item) = v {
                out.push(item);
            }
        }
        Ok(Some(out))
    }
}

struct DynamicValueSeed<'a> {
    col: &'a ColumnPlan,
    allow_null: bool,
}

impl<'de> DeserializeSeed<'de> for DynamicValueSeed<'_> {
    type Value = Option<Value>;

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        match &self.col.kind {
            ColumnKind::Numeric => {
                if let Some(raw) =
                    <Option<&'de RawValue> as serde::Deserialize>::deserialize(deserializer)?
                {
                    let s = raw.get();
                    if s.starts_with('"') {
                        let inner: String = serde_json::from_str(s)
                            .map_err(|_| de::Error::custom("bad numeric"))?;
                        Ok(Some(Value::String(inner)))
                    } else {
                        Ok(Some(Value::String(s.to_owned())))
                    }
                } else {
                    Ok(None)
                }
            }
            ColumnKind::Json => {
                if let Some(raw) =
                    <Option<&'de RawValue> as serde::Deserialize>::deserialize(deserializer)?
                {
                    let s = raw.get();
                    if s.starts_with('"') {
                        let inner: String =
                            serde_json::from_str(s).map_err(|_| de::Error::custom("bad json"))?;
                        serde_json::from_str::<IgnoredAny>(&inner)
                            .map_err(|_| de::Error::custom("invalid json"))?;
                        Ok(Some(Value::String(inner)))
                    } else {
                        Ok(Some(Value::String(s.to_owned())))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => deserializer.deserialize_any(self),
        }
    }
}

impl<'de> Visitor<'de> for DynamicValueSeed<'_> {
    type Value = Option<Value>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a column value")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        if self.allow_null {
            Ok(None)
        } else {
            Err(E::custom("null in repeated column"))
        }
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
        match &self.col.kind {
            ColumnKind::Bool => Ok(Some(Value::Bool(v))),
            ColumnKind::String => Ok(Some(Value::String(
                (if v { "true" } else { "false" }).to_owned(),
            ))),
            _ => Err(E::custom("unexpected bool")),
        }
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
        match &self.col.kind {
            ColumnKind::Int64 | ColumnKind::Timestamp => Ok(Some(Value::I64(v))),
            ColumnKind::Date => {
                let d = i32::try_from(v).map_err(|_| E::custom("date out of range"))?;
                Ok(Some(Value::I32(d)))
            }
            _ => Err(E::custom("unexpected int")),
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
        let iv = i64::try_from(v).map_err(|_| E::custom("int64 overflow"))?;
        self.visit_i64(iv)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
        match &self.col.kind {
            ColumnKind::String => Ok(Some(Value::String(v.to_owned()))),
            ColumnKind::Int64 => Ok(Some(Value::I64(
                v.parse().map_err(|_| E::custom("bad int64"))?,
            ))),
            ColumnKind::Bool => {
                let b = if v.eq_ignore_ascii_case("true") {
                    true
                } else if v.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(E::custom("bad bool"));
                };
                Ok(Some(Value::Bool(b)))
            }
            ColumnKind::Date => Ok(Some(Value::I32(
                parse_bq_date(v).ok_or_else(|| E::custom("bad date"))?,
            ))),
            ColumnKind::Datetime => Ok(Some(Value::I64(
                parse_bq_datetime_packed(v).ok_or_else(|| E::custom("bad datetime"))?,
            ))),
            ColumnKind::Timestamp => Ok(Some(Value::I64(
                parse_bq_timestamp_micros(v).ok_or_else(|| E::custom("bad timestamp"))?,
            ))),
            _ => Err(E::custom("unexpected string")),
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> std::result::Result<Self::Value, A::Error> {
        match &self.col.kind {
            ColumnKind::Struct(sub_plan) => {
                let mut sub_msg = DynamicMessage::new(sub_plan.msg_desc.clone());
                (DynamicSeed {
                    plan: sub_plan,
                    msg: &mut sub_msg,
                })
                .visit_map(map)?;
                Ok(Some(Value::Message(sub_msg)))
            }
            _ => Err(de::Error::custom("unexpected object")),
        }
    }
}

#[inline]
pub fn encode_row_hybrid(
    plan: &MessagePlan,
    json: &str,
    msg: &mut DynamicMessage,
    out: &mut BytesMut,
) -> Result<()> {
    msg.clear();
    let mut de = serde_json::Deserializer::from_str(json);
    DynamicSeed { plan, msg }.deserialize(&mut de)?;
    de.end()?;
    msg.encode(out)?;
    Ok(())
}

pub fn encode_batch_hybrid(
    plan: &MessagePlan,
    rows: &[String],
    msg: &mut DynamicMessage,
) -> Result<ProtoRows> {
    let mut buf = BytesMut::with_capacity(rows.len() * 256);
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        encode_row_hybrid(plan, r, msg, &mut buf)?;
        out.push(buf.split().freeze());
    }
    Ok(ProtoRows::new().set_serialized_rows(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        generate_realistic_json_rows, generate_simple_json_rows, realistic_proto_schema_and_plan,
        simple_message_plan,
    };

    #[test]
    fn temporal_parsers() {
        assert_eq!(parse_bq_date("1970-01-01"), Some(0));
        assert_eq!(parse_bq_date("2026-10-21"), Some(20_747));
        assert_eq!(parse_bq_timestamp_micros("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_bq_timestamp_micros("2026-10-30T12:00:00.123456Z"),
            Some(1_793_361_600_123_456)
        );
        assert!(parse_bq_datetime_packed("2026-10-21T12:34:56.123456").is_some());
    }

    #[test]
    fn direct_and_hybrid_match_on_both_schemas() -> Result<()> {
        let simple_plan = simple_message_plan();
        let simple_rows = generate_simple_json_rows(64);
        let mut state = DirectState::with_capacity(256);
        let mut dbuf = BytesMut::new();
        let mut hbuf = BytesMut::new();
        let mut msg = DynamicMessage::new(simple_plan.msg_desc.clone());
        for row in &simple_rows {
            encode_row_direct(&simple_plan, row, &mut dbuf, &mut state)?;
            let d = dbuf.split().freeze();
            encode_row_hybrid(&simple_plan, row, &mut msg, &mut hbuf)?;
            let h = hbuf.split().freeze();
            assert_eq!(d, h);
        }

        let (_proto_schema, realistic_plan) = realistic_proto_schema_and_plan();
        let realistic_rows = generate_realistic_json_rows(64, None);
        let mut msg = DynamicMessage::new(realistic_plan.msg_desc.clone());
        for (idx, row) in realistic_rows.iter().enumerate() {
            encode_row_direct(&realistic_plan, row, &mut dbuf, &mut state)?;
            let d = dbuf.split().freeze();
            encode_row_hybrid(&realistic_plan, row, &mut msg, &mut hbuf)?;
            let h = hbuf.split().freeze();
            assert_eq!(d, h, "mismatch on row {idx}");
            let decoded = DynamicMessage::decode(realistic_plan.msg_desc.clone(), d)?;
            assert_eq!(
                decoded.get_field_by_name("id").and_then(|v| v.as_i64()),
                Some(idx as i64)
            );
        }
        Ok(())
    }
}
