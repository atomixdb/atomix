// automatically generated by the FlatBuffers compiler, do not modify


// @generated

use core::mem;
use core::cmp::Ordering;

extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};

#[allow(unused_imports, dead_code)]
pub mod range_server {

  use core::mem;
  use core::cmp::Ordering;

  extern crate flatbuffers;
  use self::flatbuffers::{EndianScalar, Follow};

#[deprecated(since = "2.0.0", note = "Use associated constants instead. This will no longer be generated in 2021.")]
pub const ENUM_MIN_ENTRY: u8 = 0;
#[deprecated(since = "2.0.0", note = "Use associated constants instead. This will no longer be generated in 2021.")]
pub const ENUM_MAX_ENTRY: u8 = 3;
#[deprecated(since = "2.0.0", note = "Use associated constants instead. This will no longer be generated in 2021.")]
#[allow(non_camel_case_types)]
pub const ENUM_VALUES_ENTRY: [Entry; 4] = [
  Entry::NONE,
  Entry::PrepareRecord,
  Entry::CommitRecord,
  Entry::AbortRecord,
];

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Entry(pub u8);
#[allow(non_upper_case_globals)]
impl Entry {
  pub const NONE: Self = Self(0);
  pub const PrepareRecord: Self = Self(1);
  pub const CommitRecord: Self = Self(2);
  pub const AbortRecord: Self = Self(3);

  pub const ENUM_MIN: u8 = 0;
  pub const ENUM_MAX: u8 = 3;
  pub const ENUM_VALUES: &'static [Self] = &[
    Self::NONE,
    Self::PrepareRecord,
    Self::CommitRecord,
    Self::AbortRecord,
  ];
  /// Returns the variant's name or "" if unknown.
  pub fn variant_name(self) -> Option<&'static str> {
    match self {
      Self::NONE => Some("NONE"),
      Self::PrepareRecord => Some("PrepareRecord"),
      Self::CommitRecord => Some("CommitRecord"),
      Self::AbortRecord => Some("AbortRecord"),
      _ => None,
    }
  }
}
impl core::fmt::Debug for Entry {
  fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
    if let Some(name) = self.variant_name() {
      f.write_str(name)
    } else {
      f.write_fmt(format_args!("<UNKNOWN {:?}>", self.0))
    }
  }
}
impl<'a> flatbuffers::Follow<'a> for Entry {
  type Inner = Self;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    let b = flatbuffers::read_scalar_at::<u8>(buf, loc);
    Self(b)
  }
}

impl flatbuffers::Push for Entry {
    type Output = Entry;
    #[inline]
    unsafe fn push(&self, dst: &mut [u8], _written_len: usize) {
        flatbuffers::emplace_scalar::<u8>(dst, self.0);
    }
}

impl flatbuffers::EndianScalar for Entry {
  type Scalar = u8;
  #[inline]
  fn to_little_endian(self) -> u8 {
    self.0.to_le()
  }
  #[inline]
  #[allow(clippy::wrong_self_convention)]
  fn from_little_endian(v: u8) -> Self {
    let b = u8::from_le(v);
    Self(b)
  }
}

impl<'a> flatbuffers::Verifiable for Entry {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    u8::run_verifier(v, pos)
  }
}

impl flatbuffers::SimpleToVerifyInSlice for Entry {}
pub struct EntryUnionTableOffset {}

pub enum KeyOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct Key<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for Key<'a> {
  type Inner = Key<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> Key<'a> {
  pub const VT_K: flatbuffers::VOffsetT = 4;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    Key { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args KeyArgs<'args>
  ) -> flatbuffers::WIPOffset<Key<'bldr>> {
    let mut builder = KeyBuilder::new(_fbb);
    if let Some(x) = args.k { builder.add_k(x); }
    builder.finish()
  }


  #[inline]
  pub fn k(&self) -> Option<flatbuffers::Vector<'a, u8>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(Key::VT_K, None)}
  }
}

impl flatbuffers::Verifiable for Key<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>("k", Self::VT_K, false)?
     .finish();
    Ok(())
  }
}
pub struct KeyArgs<'a> {
    pub k: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
}
impl<'a> Default for KeyArgs<'a> {
  #[inline]
  fn default() -> Self {
    KeyArgs {
      k: None,
    }
  }
}

pub struct KeyBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> KeyBuilder<'a, 'b> {
  #[inline]
  pub fn add_k(&mut self, k: flatbuffers::WIPOffset<flatbuffers::Vector<'b , u8>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(Key::VT_K, k);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> KeyBuilder<'a, 'b> {
    let start = _fbb.start_table();
    KeyBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<Key<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for Key<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("Key");
      ds.field("k", &self.k());
      ds.finish()
  }
}
pub enum RecordOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct Record<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for Record<'a> {
  type Inner = Record<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> Record<'a> {
  pub const VT_KEY: flatbuffers::VOffsetT = 4;
  pub const VT_VALUE: flatbuffers::VOffsetT = 6;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    Record { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args RecordArgs<'args>
  ) -> flatbuffers::WIPOffset<Record<'bldr>> {
    let mut builder = RecordBuilder::new(_fbb);
    if let Some(x) = args.value { builder.add_value(x); }
    if let Some(x) = args.key { builder.add_key(x); }
    builder.finish()
  }


  #[inline]
  pub fn key(&self) -> Option<Key<'a>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<Key>>(Record::VT_KEY, None)}
  }
  #[inline]
  pub fn value(&self) -> Option<flatbuffers::Vector<'a, u8>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(Record::VT_VALUE, None)}
  }
}

impl flatbuffers::Verifiable for Record<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<Key>>("key", Self::VT_KEY, false)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>("value", Self::VT_VALUE, false)?
     .finish();
    Ok(())
  }
}
pub struct RecordArgs<'a> {
    pub key: Option<flatbuffers::WIPOffset<Key<'a>>>,
    pub value: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
}
impl<'a> Default for RecordArgs<'a> {
  #[inline]
  fn default() -> Self {
    RecordArgs {
      key: None,
      value: None,
    }
  }
}

pub struct RecordBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> RecordBuilder<'a, 'b> {
  #[inline]
  pub fn add_key(&mut self, key: flatbuffers::WIPOffset<Key<'b >>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<Key>>(Record::VT_KEY, key);
  }
  #[inline]
  pub fn add_value(&mut self, value: flatbuffers::WIPOffset<flatbuffers::Vector<'b , u8>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(Record::VT_VALUE, value);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> RecordBuilder<'a, 'b> {
    let start = _fbb.start_table();
    RecordBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<Record<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for Record<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("Record");
      ds.field("key", &self.key());
      ds.field("value", &self.value());
      ds.finish()
  }
}
pub enum PrepareRecordOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct PrepareRecord<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for PrepareRecord<'a> {
  type Inner = PrepareRecord<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> PrepareRecord<'a> {
  pub const VT_TRANSACTION_ID: flatbuffers::VOffsetT = 4;
  pub const VT_PUTS: flatbuffers::VOffsetT = 6;
  pub const VT_DELETES: flatbuffers::VOffsetT = 8;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    PrepareRecord { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args PrepareRecordArgs<'args>
  ) -> flatbuffers::WIPOffset<PrepareRecord<'bldr>> {
    let mut builder = PrepareRecordBuilder::new(_fbb);
    if let Some(x) = args.deletes { builder.add_deletes(x); }
    if let Some(x) = args.puts { builder.add_puts(x); }
    if let Some(x) = args.transaction_id { builder.add_transaction_id(x); }
    builder.finish()
  }


  #[inline]
  pub fn transaction_id(&self) -> Option<&'a str> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<&str>>(PrepareRecord::VT_TRANSACTION_ID, None)}
  }
  #[inline]
  pub fn puts(&self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Record<'a>>>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Record>>>>(PrepareRecord::VT_PUTS, None)}
  }
  #[inline]
  pub fn deletes(&self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Key<'a>>>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Key>>>>(PrepareRecord::VT_DELETES, None)}
  }
}

impl flatbuffers::Verifiable for PrepareRecord<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<&str>>("transaction_id", Self::VT_TRANSACTION_ID, false)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<Record>>>>("puts", Self::VT_PUTS, false)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<Key>>>>("deletes", Self::VT_DELETES, false)?
     .finish();
    Ok(())
  }
}
pub struct PrepareRecordArgs<'a> {
    pub transaction_id: Option<flatbuffers::WIPOffset<&'a str>>,
    pub puts: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Record<'a>>>>>,
    pub deletes: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Key<'a>>>>>,
}
impl<'a> Default for PrepareRecordArgs<'a> {
  #[inline]
  fn default() -> Self {
    PrepareRecordArgs {
      transaction_id: None,
      puts: None,
      deletes: None,
    }
  }
}

pub struct PrepareRecordBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> PrepareRecordBuilder<'a, 'b> {
  #[inline]
  pub fn add_transaction_id(&mut self, transaction_id: flatbuffers::WIPOffset<&'b  str>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(PrepareRecord::VT_TRANSACTION_ID, transaction_id);
  }
  #[inline]
  pub fn add_puts(&mut self, puts: flatbuffers::WIPOffset<flatbuffers::Vector<'b , flatbuffers::ForwardsUOffset<Record<'b >>>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(PrepareRecord::VT_PUTS, puts);
  }
  #[inline]
  pub fn add_deletes(&mut self, deletes: flatbuffers::WIPOffset<flatbuffers::Vector<'b , flatbuffers::ForwardsUOffset<Key<'b >>>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(PrepareRecord::VT_DELETES, deletes);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> PrepareRecordBuilder<'a, 'b> {
    let start = _fbb.start_table();
    PrepareRecordBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<PrepareRecord<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for PrepareRecord<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("PrepareRecord");
      ds.field("transaction_id", &self.transaction_id());
      ds.field("puts", &self.puts());
      ds.field("deletes", &self.deletes());
      ds.finish()
  }
}
pub enum CommitRecordOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct CommitRecord<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for CommitRecord<'a> {
  type Inner = CommitRecord<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> CommitRecord<'a> {
  pub const VT_TRANSACTION_ID: flatbuffers::VOffsetT = 4;
  pub const VT_EPOCH: flatbuffers::VOffsetT = 6;
  pub const VT_VID: flatbuffers::VOffsetT = 8;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    CommitRecord { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args CommitRecordArgs<'args>
  ) -> flatbuffers::WIPOffset<CommitRecord<'bldr>> {
    let mut builder = CommitRecordBuilder::new(_fbb);
    builder.add_vid(args.vid);
    builder.add_epoch(args.epoch);
    if let Some(x) = args.transaction_id { builder.add_transaction_id(x); }
    builder.finish()
  }


  #[inline]
  pub fn transaction_id(&self) -> Option<&'a str> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<&str>>(CommitRecord::VT_TRANSACTION_ID, None)}
  }
  #[inline]
  pub fn epoch(&self) -> i64 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<i64>(CommitRecord::VT_EPOCH, Some(0)).unwrap()}
  }
  #[inline]
  pub fn vid(&self) -> i64 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<i64>(CommitRecord::VT_VID, Some(0)).unwrap()}
  }
}

impl flatbuffers::Verifiable for CommitRecord<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<&str>>("transaction_id", Self::VT_TRANSACTION_ID, false)?
     .visit_field::<i64>("epoch", Self::VT_EPOCH, false)?
     .visit_field::<i64>("vid", Self::VT_VID, false)?
     .finish();
    Ok(())
  }
}
pub struct CommitRecordArgs<'a> {
    pub transaction_id: Option<flatbuffers::WIPOffset<&'a str>>,
    pub epoch: i64,
    pub vid: i64,
}
impl<'a> Default for CommitRecordArgs<'a> {
  #[inline]
  fn default() -> Self {
    CommitRecordArgs {
      transaction_id: None,
      epoch: 0,
      vid: 0,
    }
  }
}

pub struct CommitRecordBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> CommitRecordBuilder<'a, 'b> {
  #[inline]
  pub fn add_transaction_id(&mut self, transaction_id: flatbuffers::WIPOffset<&'b  str>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(CommitRecord::VT_TRANSACTION_ID, transaction_id);
  }
  #[inline]
  pub fn add_epoch(&mut self, epoch: i64) {
    self.fbb_.push_slot::<i64>(CommitRecord::VT_EPOCH, epoch, 0);
  }
  #[inline]
  pub fn add_vid(&mut self, vid: i64) {
    self.fbb_.push_slot::<i64>(CommitRecord::VT_VID, vid, 0);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> CommitRecordBuilder<'a, 'b> {
    let start = _fbb.start_table();
    CommitRecordBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<CommitRecord<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for CommitRecord<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("CommitRecord");
      ds.field("transaction_id", &self.transaction_id());
      ds.field("epoch", &self.epoch());
      ds.field("vid", &self.vid());
      ds.finish()
  }
}
pub enum AbortRecordOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct AbortRecord<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for AbortRecord<'a> {
  type Inner = AbortRecord<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> AbortRecord<'a> {
  pub const VT_TRANSACTION_ID: flatbuffers::VOffsetT = 4;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    AbortRecord { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args AbortRecordArgs<'args>
  ) -> flatbuffers::WIPOffset<AbortRecord<'bldr>> {
    let mut builder = AbortRecordBuilder::new(_fbb);
    if let Some(x) = args.transaction_id { builder.add_transaction_id(x); }
    builder.finish()
  }


  #[inline]
  pub fn transaction_id(&self) -> Option<&'a str> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<&str>>(AbortRecord::VT_TRANSACTION_ID, None)}
  }
}

impl flatbuffers::Verifiable for AbortRecord<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<&str>>("transaction_id", Self::VT_TRANSACTION_ID, false)?
     .finish();
    Ok(())
  }
}
pub struct AbortRecordArgs<'a> {
    pub transaction_id: Option<flatbuffers::WIPOffset<&'a str>>,
}
impl<'a> Default for AbortRecordArgs<'a> {
  #[inline]
  fn default() -> Self {
    AbortRecordArgs {
      transaction_id: None,
    }
  }
}

pub struct AbortRecordBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> AbortRecordBuilder<'a, 'b> {
  #[inline]
  pub fn add_transaction_id(&mut self, transaction_id: flatbuffers::WIPOffset<&'b  str>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(AbortRecord::VT_TRANSACTION_ID, transaction_id);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> AbortRecordBuilder<'a, 'b> {
    let start = _fbb.start_table();
    AbortRecordBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<AbortRecord<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for AbortRecord<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("AbortRecord");
      ds.field("transaction_id", &self.transaction_id());
      ds.finish()
  }
}
pub enum LogEntryOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct LogEntry<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for LogEntry<'a> {
  type Inner = LogEntry<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> LogEntry<'a> {
  pub const VT_ENTRY_TYPE: flatbuffers::VOffsetT = 4;
  pub const VT_ENTRY: flatbuffers::VOffsetT = 6;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    LogEntry { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
    args: &'args LogEntryArgs
  ) -> flatbuffers::WIPOffset<LogEntry<'bldr>> {
    let mut builder = LogEntryBuilder::new(_fbb);
    if let Some(x) = args.entry { builder.add_entry(x); }
    builder.add_entry_type(args.entry_type);
    builder.finish()
  }


  #[inline]
  pub fn entry_type(&self) -> Entry {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<Entry>(LogEntry::VT_ENTRY_TYPE, Some(Entry::NONE)).unwrap()}
  }
  #[inline]
  pub fn entry(&self) -> Option<flatbuffers::Table<'a>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Table<'a>>>(LogEntry::VT_ENTRY, None)}
  }
  #[inline]
  #[allow(non_snake_case)]
  pub fn entry_as_prepare_record(&self) -> Option<PrepareRecord<'a>> {
    if self.entry_type() == Entry::PrepareRecord {
      self.entry().map(|t| {
       // Safety:
       // Created from a valid Table for this object
       // Which contains a valid union in this slot
       unsafe { PrepareRecord::init_from_table(t) }
     })
    } else {
      None
    }
  }

  #[inline]
  #[allow(non_snake_case)]
  pub fn entry_as_commit_record(&self) -> Option<CommitRecord<'a>> {
    if self.entry_type() == Entry::CommitRecord {
      self.entry().map(|t| {
       // Safety:
       // Created from a valid Table for this object
       // Which contains a valid union in this slot
       unsafe { CommitRecord::init_from_table(t) }
     })
    } else {
      None
    }
  }

  #[inline]
  #[allow(non_snake_case)]
  pub fn entry_as_abort_record(&self) -> Option<AbortRecord<'a>> {
    if self.entry_type() == Entry::AbortRecord {
      self.entry().map(|t| {
       // Safety:
       // Created from a valid Table for this object
       // Which contains a valid union in this slot
       unsafe { AbortRecord::init_from_table(t) }
     })
    } else {
      None
    }
  }

}

impl flatbuffers::Verifiable for LogEntry<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_union::<Entry, _>("entry_type", Self::VT_ENTRY_TYPE, "entry", Self::VT_ENTRY, false, |key, v, pos| {
        match key {
          Entry::PrepareRecord => v.verify_union_variant::<flatbuffers::ForwardsUOffset<PrepareRecord>>("Entry::PrepareRecord", pos),
          Entry::CommitRecord => v.verify_union_variant::<flatbuffers::ForwardsUOffset<CommitRecord>>("Entry::CommitRecord", pos),
          Entry::AbortRecord => v.verify_union_variant::<flatbuffers::ForwardsUOffset<AbortRecord>>("Entry::AbortRecord", pos),
          _ => Ok(()),
        }
     })?
     .finish();
    Ok(())
  }
}
pub struct LogEntryArgs {
    pub entry_type: Entry,
    pub entry: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
}
impl<'a> Default for LogEntryArgs {
  #[inline]
  fn default() -> Self {
    LogEntryArgs {
      entry_type: Entry::NONE,
      entry: None,
    }
  }
}

pub struct LogEntryBuilder<'a: 'b, 'b> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> LogEntryBuilder<'a, 'b> {
  #[inline]
  pub fn add_entry_type(&mut self, entry_type: Entry) {
    self.fbb_.push_slot::<Entry>(LogEntry::VT_ENTRY_TYPE, entry_type, Entry::NONE);
  }
  #[inline]
  pub fn add_entry(&mut self, entry: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(LogEntry::VT_ENTRY, entry);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> LogEntryBuilder<'a, 'b> {
    let start = _fbb.start_table();
    LogEntryBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<LogEntry<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for LogEntry<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("LogEntry");
      ds.field("entry_type", &self.entry_type());
      match self.entry_type() {
        Entry::PrepareRecord => {
          if let Some(x) = self.entry_as_prepare_record() {
            ds.field("entry", &x)
          } else {
            ds.field("entry", &"InvalidFlatbuffer: Union discriminant does not match value.")
          }
        },
        Entry::CommitRecord => {
          if let Some(x) = self.entry_as_commit_record() {
            ds.field("entry", &x)
          } else {
            ds.field("entry", &"InvalidFlatbuffer: Union discriminant does not match value.")
          }
        },
        Entry::AbortRecord => {
          if let Some(x) = self.entry_as_abort_record() {
            ds.field("entry", &x)
          } else {
            ds.field("entry", &"InvalidFlatbuffer: Union discriminant does not match value.")
          }
        },
        _ => {
          let x: Option<()> = None;
          ds.field("entry", &x)
        },
      };
      ds.finish()
  }
}
#[inline]
/// Verifies that a buffer of bytes contains a `LogEntry`
/// and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_log_entry_unchecked`.
pub fn root_as_log_entry(buf: &[u8]) -> Result<LogEntry, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::root::<LogEntry>(buf)
}
#[inline]
/// Verifies that a buffer of bytes contains a size prefixed
/// `LogEntry` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `size_prefixed_root_as_log_entry_unchecked`.
pub fn size_prefixed_root_as_log_entry(buf: &[u8]) -> Result<LogEntry, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::size_prefixed_root::<LogEntry>(buf)
}
#[inline]
/// Verifies, with the given options, that a buffer of bytes
/// contains a `LogEntry` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_log_entry_unchecked`.
pub fn root_as_log_entry_with_opts<'b, 'o>(
  opts: &'o flatbuffers::VerifierOptions,
  buf: &'b [u8],
) -> Result<LogEntry<'b>, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::root_with_opts::<LogEntry<'b>>(opts, buf)
}
#[inline]
/// Verifies, with the given verifier options, that a buffer of
/// bytes contains a size prefixed `LogEntry` and returns
/// it. Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_log_entry_unchecked`.
pub fn size_prefixed_root_as_log_entry_with_opts<'b, 'o>(
  opts: &'o flatbuffers::VerifierOptions,
  buf: &'b [u8],
) -> Result<LogEntry<'b>, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::size_prefixed_root_with_opts::<LogEntry<'b>>(opts, buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a LogEntry and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid `LogEntry`.
pub unsafe fn root_as_log_entry_unchecked(buf: &[u8]) -> LogEntry {
  flatbuffers::root_unchecked::<LogEntry>(buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a size prefixed LogEntry and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid size prefixed `LogEntry`.
pub unsafe fn size_prefixed_root_as_log_entry_unchecked(buf: &[u8]) -> LogEntry {
  flatbuffers::size_prefixed_root_unchecked::<LogEntry>(buf)
}
#[inline]
pub fn finish_log_entry_buffer<'a, 'b>(
    fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    root: flatbuffers::WIPOffset<LogEntry<'a>>) {
  fbb.finish(root, None);
}

#[inline]
pub fn finish_size_prefixed_log_entry_buffer<'a, 'b>(fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>, root: flatbuffers::WIPOffset<LogEntry<'a>>) {
  fbb.finish_size_prefixed(root, None);
}
}  // pub mod RangeServer

