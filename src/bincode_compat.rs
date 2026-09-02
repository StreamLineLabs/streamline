//! Crate-internal bincode-1 wire-format compatibility layer.
//!
//! Streamline persists data with the bincode 1 encoding: segment record
//! batches, the raft state/log/snapshot files, time-travel archives and the AI
//! vector stores are all encoded that way on disk. Those files must keep
//! decoding across upgrades, so the encoding is a *stable on-disk format*, not
//! an implementation detail.
//!
//! **The `bincode` crate is no longer a dependency at any version.**
//! RUSTSEC-2025-0141 marks bincode unmaintained with `patched = []` — the
//! advisory applies to 1.x and 2.x alike, so bumping the major version could
//! not clear it. The advisory names [`wincode`] as a replacement, and
//! `serde-wincode` layers a `serde` bridge on top of it whose own test suite
//! asserts byte equality against `bincode::serialize`. This module is the only
//! place that decides how bytes are laid out.
//!
//! # Why the encoding still matches bincode 1
//!
//! wincode is configured with a [`Configuration`] that reproduces bincode 1's
//! defaults exactly:
//!
//! * [`BincodeLen`] — sequence, string and byte-buffer lengths are written as
//!   a `u64`, which is what bincode 1 did.
//! * [`LittleEndian`] + [`FixInt`] — integers are fixed-width little-endian,
//!   never varints.
//! * `u32` tag encoding — `Option` and enum discriminants are `u32`
//!   little-endian, matching bincode 1's variant indices. (`Option` is still a
//!   single byte, as the golden tests below assert.)
//! * **The fixed preallocation size limit is disabled.** This is the one
//!   wincode default that *must* be overridden. wincode caps preallocation at
//!   4 MiB and returns a hard error above it when reading any `String` or byte
//!   buffer (see `read_prealloc_check` in wincode's `SeqLen`). bincode 1's
//!   top-level `serialize`/`deserialize` had no such limit. Worse, the check is
//!   asymmetric: serde-wincode *writes* strings through the unchecked `&str`
//!   schema but *reads* them back through the checked `String` schema, so
//!   leaving the default in place would happily write any record whose value
//!   exceeds 4 MiB and then never be able to read it back — the worst possible
//!   failure mode for an on-disk format. Streamline enforces its own message-
//!   and batch-size limits upstream of this layer; a *fixed* decoder limit here
//!   is not the right control.
//!   `sequences_larger_than_the_default_preallocation_limit` pins this down and
//!   proves the default configuration rejects the same bytes.
//!
//! # Corrupt length prefixes
//!
//! Disabling the fixed limit must not mean trusting the length prefix. Every
//! owned sequence in this format is introduced by a `u64` that a corrupt or
//! hostile file controls completely, and two decode paths would otherwise
//! allocate from it *before* discovering the input is short:
//!
//! * `String` and `Box<[u8]>`/`Vec<u8>` — wincode's schemas call
//!   `C::LengthEncoding::read_prealloc_check::<u8>` and then
//!   `Vec::with_capacity(len)`. `bytes::Bytes` takes the `Vec<u8>` path through
//!   serde's `deserialize_byte_buf`, so `Record::value`, `Record::key` and
//!   `Header::value` are all on it. A `u64::MAX` prefix means
//!   `Vec::with_capacity(usize::MAX)` — a capacity-overflow panic or an OOM
//!   abort, neither of which a `Result` can catch.
//! * generic sequences and maps — the element count is handed to the visitor as
//!   a size hint, and serde would keep asking for elements `len` times.
//!
//! Both are closed here, and both are closed by the *same* principle rather
//! than by a fixed ceiling: **a declared length is only believed as far as the
//! bytes that are actually still in the input.**
//!
//! * [`LengthGuard`] is the configured [`SeqLen`]. It encodes and decodes
//!   exactly like [`BincodeLen`], but its `read_prealloc_check` refuses a length
//!   larger than the reader's remaining byte count, so wincode never reaches
//!   `Vec::with_capacity` with an unbacked length.
//! * [`bounded_de`] is a small local serde bridge that reads sequence and map
//!   lengths through the same rule: the visitor's size hint is clamped to what
//!   the input could possibly contain, so preallocation is bounded by the input
//!   size while the declared count itself is still honoured for zero-width
//!   elements.
//!
//! Neither rule involves a global maximum, a type-name match or a
//! `catch_unwind`, and neither can reject a valid payload: a value that really
//! is `n` bytes long always has those `n` bytes in front of the reader.
//! Payloads above 4 MiB keep working, which
//! `record_with_a_value_larger_than_the_default_limit_round_trips` proves.
//!
//! # Trailing bytes
//!
//! Decoding does **not require the reader to be exhausted**, so bytes trailing
//! the encoded value are ignored. That is exactly bincode 1's top-level
//! `deserialize`, which used `allow_trailing_bytes()`, and call sites depend on
//! it: segment batch payloads are decoded out of length-delimited and
//! decompressed buffers that may be larger than the encoded value. Tolerating
//! *trailing* bytes must not decay into tolerating *missing* bytes, which
//! `truncated_input_still_errors` guards.
//!
//! # Using this module
//!
//! Call sites must use [`serialize`] / [`deserialize`] rather than reaching for
//! `wincode::*` directly, so the configuration cannot drift per module. The
//! tests below lock the format down with hard-coded byte vectors captured from
//! **bincode 1.3.3**, so an accidental config change fails loudly instead of
//! corrupting data.
//!
//! [`BincodeLen`]: wincode::len::BincodeLen
//! [`SeqLen`]: wincode::len::SeqLen

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_wincode::SerdeCompat;
// `wincode` is a **direct, exactly pinned** dependency (`wincode = "=0.4.9"` in
// Cargo.toml) rather than `serde_wincode`'s re-export.
//
// serde-wincode's own requirement is a permissive `>=0.4, <1`. Depending only on
// its re-export would let a downstream consumer of the published `streamline`
// crate resolve serde-wincode against wincode 0.6 — a different ABI with a
// different MSRV — without anything here noticing. Because the configuration
// below is built from *this* crate's `wincode` and then handed to
// `serde_wincode::SerdeCompat`, a graph in which the two differ fails to
// compile instead of silently changing the on-disk format. Cargo unifies the
// two semver-compatible requirements onto one 0.4.9 node, which
// `wincode_stays_msrv_compatible` and `serde_wincode_resolves_to_the_pinned_wincode`
// in tests/dependency_security_test.rs assert.
use wincode::config::{Configuration, PREALLOCATION_SIZE_LIMIT_DISABLED};
use wincode::int_encoding::{FixInt, LittleEndian};
use wincode::WriteError;

pub(crate) use bounded_de::DecodeError;
use length_guard::LengthGuard;

/// The wincode configuration that reproduces bincode 1.x's default format:
/// `u64` lengths, little-endian fixed-width integers, `u32` enum tags and no
/// fixed preallocation limit.
///
/// Every parameter is spelled out rather than relying on wincode's defaults, so
/// that a future change to those defaults is a compile-time diff here instead of
/// a silent change to the on-disk format.
type LegacyConfig = Configuration<
    true, // zero-copy alignment check (unused on the serde path)
    PREALLOCATION_SIZE_LIMIT_DISABLED,
    LengthGuard,
    LittleEndian,
    FixInt,
    u32,
>;

#[inline]
const fn config() -> LegacyConfig {
    LegacyConfig::new()
}

/// Serialize a value using the bincode 1.x-compatible format.
///
/// Drop-in replacement for bincode 1's `bincode::serialize`.
pub(crate) fn serialize<T>(value: &T) -> Result<Vec<u8>, WriteError>
where
    T: Serialize,
{
    // `Patch128` is what routes 128-bit integers around the upstream gap
    // documented on `wide_int`; every other type passes straight through.
    let value = wide_int::Patch128(value);
    <SerdeCompat<wide_int::Patch128<&T>> as wincode::config::Serialize<LegacyConfig>>::serialize(
        &value,
        config(),
    )
}

/// Deserialize a value using the bincode 1.x-compatible format.
///
/// Drop-in replacement for bincode 1's `bincode::deserialize`: bytes trailing
/// the encoded value are ignored rather than treated as an error.
///
/// Decoding goes through [`bounded_de`], a local serde bridge that reads every
/// length prefix with the remaining input in hand (see the module docs). Leaf
/// values are still decoded by wincode's own schemas, so the byte-level format
/// is defined in exactly one place.
///
/// No 128-bit patch is needed on this side: both this bridge and
/// serde-wincode's deserializer implement `deserialize_u128`/`deserialize_i128`;
/// only serde-wincode's *serializer* is missing the matching pair.
pub(crate) fn deserialize<T>(bytes: &[u8]) -> Result<T, DecodeError>
where
    T: DeserializeOwned,
{
    let mut input = bytes;
    T::deserialize(bounded_de::Deserializer::<LegacyConfig>::new(&mut input))
}

mod length_guard {
    //! Remaining-input-aware sequence length encoding.
    //!
    //! [`LengthGuard`] is byte-for-byte [`BincodeLen`] (a `u64` written with the
    //! configuration's integer encoding, i.e. fixed-width little-endian here).
    //! The only behaviour it adds is on the one call that matters: wincode's
    //! `read_prealloc_check`, which every schema calls immediately before
    //! reserving memory for a decoded sequence.
    //!
    //! wincode's stock check compares `len * size_of::<T>()` against a fixed
    //! ceiling, which Streamline must disable because legitimate record values
    //! exceed 4 MiB. This one instead compares the declared length against the
    //! bytes the reader can still supply. Every element of an encoded sequence
    //! occupies at least one byte on the wire, so a sequence of `len` elements
    //! needs at least `len` bytes; a prefix claiming more than that is provably
    //! corrupt and is rejected *before* the caller allocates.
    //!
    //! That bound is deliberately expressed in wire bytes rather than
    //! `size_of::<T>()`: an in-memory element can be larger than its encoding
    //! (padding), and rejecting on the in-memory size would refuse valid input.
    //!
    //! [`BincodeLen`]: wincode::len::BincodeLen

    use wincode::config::ConfigCore;
    use wincode::io::{Reader, Writer};
    use wincode::len::{BincodeLen, SeqLen};
    use wincode::{ReadError, ReadResult, WriteResult};

    /// Length encoding for [`LegacyConfig`](super::LegacyConfig).
    ///
    /// Uninhabited on purpose: it is a type-level marker, never a value.
    pub(crate) enum LengthGuard {}

    /// Build the error returned when a length prefix outruns the input.
    ///
    /// `PreallocationSizeLimit` is reused rather than `Custom` so the message
    /// names both numbers: `limit` is what the input can still supply and
    /// `needed` is what the prefix claimed.
    #[cold]
    fn unbacked_length(needed: usize, available: usize) -> ReadError {
        ReadError::PreallocationSizeLimit {
            needed,
            limit: available,
        }
    }

    // SAFETY: `write_bytes_needed` forwards to `BincodeLen`, whose contract it
    // inherits: the returned count is exactly what `write` emits, because
    // `write` forwards to the same implementation.
    unsafe impl<C: ConfigCore> SeqLen<C> for LengthGuard {
        #[inline(always)]
        fn read<'de>(reader: impl Reader<'de>) -> ReadResult<usize> {
            <BincodeLen as SeqLen<C>>::read(reader)
        }

        #[inline(always)]
        fn write(writer: impl Writer, len: usize) -> WriteResult<()> {
            <BincodeLen as SeqLen<C>>::write(writer, len)
        }

        #[inline(always)]
        fn write_bytes_needed(len: usize) -> WriteResult<usize> {
            <BincodeLen as SeqLen<C>>::write_bytes_needed(len)
        }

        #[inline]
        fn read_prealloc_check<'de, T>(mut reader: impl Reader<'de>) -> ReadResult<usize> {
            let len = <BincodeLen as SeqLen<C>>::read(reader.by_ref())?;

            if len > 0 {
                // `fill_buf` returns *up to* `len` bytes and never advances the
                // reader, so this is a pure capacity probe. Every `Reader` in
                // wincode 0.4.9 is slice-backed and clamps the request to what
                // it holds, making the probe O(1) and allocation-free — the
                // exact-version pin in Cargo.toml is what keeps that true.
                // (It is deprecated in favour of consuming reads, which is
                // precisely what a check performed *before* allocating cannot
                // use.)
                #[allow(deprecated)]
                let available = reader.fill_buf(len)?.len();
                if available < len {
                    return Err(unbacked_length(len, available));
                }
            }

            // Preserve the configured limit as well, so a build that re-enables
            // one still gets it. It is a no-op under `LegacyConfig`.
            <Self as SeqLen<C>>::prealloc_check::<T>(len)?;
            Ok(len)
        }
    }
}

mod bounded_de {
    //! A local, bounded serde bridge over wincode.
    //!
    //! This mirrors `serde_wincode::Deserializer` — same wire format, same
    //! unsupported-feature errors — with one deliberate difference: sequence and
    //! map lengths are read with the remaining input in hand, so a corrupt
    //! prefix cannot be turned into a preallocation or into a long spin.
    //!
    //! Everything below the framing is still decoded by wincode's own schemas
    //! (`<u32 as SchemaRead<C>>::get` and friends), so integer width, byte
    //! order, tag encoding and UTF-8 validation are defined in exactly one place
    //! and cannot drift from the golden vectors in this module's tests.
    //!
    //! Why a bridge is needed at all: `serde_wincode` reads a sequence length as
    //! a bare `usize` and hands it straight to the visitor as a size hint. serde
    //! clamps its own preallocation, so that is not an OOM, but the declared
    //! count still drives the element loop. For elements that consume no input
    //! at all (`()` and other zero-sized types) the loop would be bounded only
    //! by the attacker-supplied `u64`. [`read_len`] closes that without
    //! penalising real data.

    use core::fmt;
    use core::marker::PhantomData;

    use serde::de::value::U32Deserializer;
    use serde::de::{DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
    use wincode::config::Config;
    use wincode::len::SeqLen;
    use wincode::{ReadError, SchemaRead};

    /// Extra elements a sequence may declare beyond the bytes that remain.
    ///
    /// Every encoded element occupies at least one byte *unless* it is
    /// zero-width (`Vec<()>`, a set of empty structs). Zero-width elements are
    /// legal and no Streamline type on disk has any, but the decoder must not
    /// assume that — and it must not try to *infer* the element width either,
    /// which would have nothing to measure for an empty sequence. So the
    /// remaining-bytes bound is simply widened by this allowance, which is the
    /// only case it can affect.
    ///
    /// The effective bound is `max(bytes remaining, this floor)`: for any
    /// element that carries data the remaining-bytes term always dominates, so
    /// no real payload is ever refused, and the work a corrupt prefix can cause
    /// stays `O(input + floor)`.
    const ZERO_WIDTH_ELEMENT_FLOOR: usize = 1 << 20;

    /// Failure while decoding the bincode-1 format.
    #[derive(Debug)]
    pub(crate) enum DecodeError {
        /// A wincode-level read failure (short input, bad UTF-8, bad tag, ...).
        Read(ReadError),
        /// A length prefix claimed more elements than the input can hold.
        UnbackedLength {
            /// What the prefix declared.
            declared: usize,
            /// Bytes left in the input when the prefix was read.
            available: usize,
            /// `"sequence"` or `"map"`.
            kind: &'static str,
        },
        /// A serde feature this format cannot represent.
        Unsupported(&'static str),
        /// An error raised by the `Deserialize` implementation itself.
        Message(String),
    }

    impl fmt::Display for DecodeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Read(err) => write!(f, "{err}"),
                Self::UnbackedLength {
                    declared,
                    available,
                    kind,
                } => write!(
                    f,
                    "corrupt {kind} length prefix: declared {declared} elements but only \
                     {available} bytes remain in the input"
                ),
                Self::Unsupported(what) => {
                    write!(f, "the bincode 1 format does not support serde's `{what}`")
                }
                Self::Message(msg) => write!(f, "{msg}"),
            }
        }
    }

    impl std::error::Error for DecodeError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Read(err) => Some(err),
                _ => None,
            }
        }
    }

    impl From<ReadError> for DecodeError {
        fn from(err: ReadError) -> Self {
            Self::Read(err)
        }
    }

    impl serde::de::Error for DecodeError {
        fn custom<T: fmt::Display>(msg: T) -> Self {
            Self::Message(msg.to_string())
        }
    }

    /// serde `Deserializer` over the remaining input slice.
    ///
    /// The cursor is borrowed rather than owned so that nested values, sequence
    /// elements and map entries all advance the same reader.
    pub(crate) struct Deserializer<'a, 'de, C> {
        input: &'a mut &'de [u8],
        config: PhantomData<C>,
    }

    impl<'a, 'de, C> Deserializer<'a, 'de, C> {
        pub(crate) fn new(input: &'a mut &'de [u8]) -> Self {
            Self {
                input,
                config: PhantomData,
            }
        }
    }

    /// Read an input-declared length prefix and bound what may come of it.
    ///
    /// Returns `(declared, hint)`. `hint` is what the visitor is told and never
    /// exceeds the bytes that remain, so `Vec::with_capacity` cannot be driven
    /// past the input size. `declared` is what the iteration honours, and is
    /// rejected outright once it passes what even zero-width elements could
    /// justify — see [`ZERO_WIDTH_ELEMENT_FLOOR`].
    fn read_len<C: Config>(
        input: &mut &[u8],
        kind: &'static str,
    ) -> Result<(usize, usize), DecodeError> {
        let declared = <C::LengthEncoding as SeqLen<C>>::read(&mut *input)?;
        let available = input.len();

        if declared > available.max(ZERO_WIDTH_ELEMENT_FLOOR) {
            return Err(DecodeError::UnbackedLength {
                declared,
                available,
                kind,
            });
        }

        Ok((declared, declared.min(available)))
    }

    macro_rules! forward_scalar {
        ($($method:ident => $visit:ident($schema:ty)),* $(,)?) => {
            $(
                #[inline]
                fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
                where
                    V: Visitor<'de>,
                {
                    visitor.$visit(<$schema as SchemaRead<'de, C>>::get(self.input)?)
                }
            )*
        };
    }

    impl<'de, C> serde::Deserializer<'de> for Deserializer<'_, 'de, C>
    where
        C: Config,
    {
        type Error = DecodeError;

        fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Err(DecodeError::Unsupported("deserialize_any"))
        }

        forward_scalar! {
            deserialize_bool => visit_bool(bool),
            deserialize_i8 => visit_i8(i8),
            deserialize_i16 => visit_i16(i16),
            deserialize_i32 => visit_i32(i32),
            deserialize_i64 => visit_i64(i64),
            deserialize_i128 => visit_i128(i128),
            deserialize_u8 => visit_u8(u8),
            deserialize_u16 => visit_u16(u16),
            deserialize_u32 => visit_u32(u32),
            deserialize_u64 => visit_u64(u64),
            deserialize_u128 => visit_u128(u128),
            deserialize_f32 => visit_f32(f32),
            deserialize_f64 => visit_f64(f64),
            deserialize_char => visit_char(char),
            // Borrowed forms slice the input directly: no length-driven
            // allocation is possible, a short prefix simply fails the borrow.
            deserialize_str => visit_borrowed_str(&'de str),
            deserialize_bytes => visit_borrowed_bytes(&'de [u8]),
            // Owned forms allocate. They are safe because `LengthGuard`
            // validates the prefix against the remaining input first.
            deserialize_string => visit_string(String),
            deserialize_byte_buf => visit_byte_buf(Vec<u8>),
        }

        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match <u8 as SchemaRead<'de, C>>::get(&mut *self.input)? {
                0 => visitor.visit_none(),
                1 => visitor.visit_some(self),
                tag => Err(ReadError::InvalidTagEncoding(tag.into()).into()),
            }
        }

        fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_unit_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_newtype_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_newtype_struct(self)
        }

        fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let (declared, hint) = read_len::<C>(self.input, "sequence")?;
            visitor.visit_seq(SeqAccessor::<C> {
                input: self.input,
                remaining: declared,
                hint,
                config: PhantomData,
            })
        }

        /// Tuples, tuple structs and structs: the element count comes from the
        /// Rust type, never from the input, so there is nothing to bound.
        fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_seq(SeqAccessor::<C> {
                input: self.input,
                remaining: len,
                hint: len,
                config: PhantomData,
            })
        }

        fn deserialize_tuple_struct<V>(
            self,
            _name: &'static str,
            len: usize,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            serde::Deserializer::deserialize_tuple(self, len, visitor)
        }

        fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let (declared, hint) = read_len::<C>(self.input, "map")?;
            visitor.visit_map(MapAccessor::<C> {
                input: self.input,
                remaining: declared,
                hint,
                config: PhantomData,
            })
        }

        fn deserialize_struct<V>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            serde::Deserializer::deserialize_tuple(self, fields.len(), visitor)
        }

        fn deserialize_enum<V>(
            self,
            _name: &'static str,
            _variants: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_enum(self)
        }

        fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Err(DecodeError::Unsupported("deserialize_identifier"))
        }

        fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Err(DecodeError::Unsupported("deserialize_ignored_any"))
        }

        fn is_human_readable(&self) -> bool {
            false
        }
    }

    impl<'de, C> EnumAccess<'de> for Deserializer<'_, 'de, C>
    where
        C: Config,
    {
        type Error = DecodeError;
        type Variant = Self;

        fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            let index = <u32 as SchemaRead<'de, C>>::get(&mut *self.input)?;
            let value = seed.deserialize(U32Deserializer::<Self::Error>::new(index))?;
            Ok((value, self))
        }
    }

    impl<'de, C> VariantAccess<'de> for Deserializer<'_, 'de, C>
    where
        C: Config,
    {
        type Error = DecodeError;

        fn unit_variant(self) -> Result<(), Self::Error> {
            Ok(())
        }

        fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            seed.deserialize(self)
        }

        fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            serde::Deserializer::deserialize_tuple(self, len, visitor)
        }

        fn struct_variant<V>(
            self,
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            serde::Deserializer::deserialize_tuple(self, fields.len(), visitor)
        }
    }

    struct SeqAccessor<'a, 'de, C> {
        input: &'a mut &'de [u8],
        /// Elements still to yield. Always the count the input declared (or the
        /// arity of the Rust type), never a clamped value — clamping it would
        /// silently truncate a valid sequence instead of failing.
        remaining: usize,
        /// What the visitor is told, clamped to the input size so that
        /// `Vec::with_capacity` cannot be driven past it.
        hint: usize,
        config: PhantomData<C>,
    }

    impl<'de, C> SeqAccess<'de> for SeqAccessor<'_, 'de, C>
    where
        C: Config,
    {
        type Error = DecodeError;

        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            let Some(remaining) = self.remaining.checked_sub(1) else {
                return Ok(None);
            };
            self.remaining = remaining;
            seed.deserialize(Deserializer::<C>::new(&mut *self.input))
                .map(Some)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.hint)
        }
    }

    struct MapAccessor<'a, 'de, C> {
        input: &'a mut &'de [u8],
        remaining: usize,
        hint: usize,
        config: PhantomData<C>,
    }

    impl<'de, C> MapAccess<'de> for MapAccessor<'_, 'de, C>
    where
        C: Config,
    {
        type Error = DecodeError;

        fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: DeserializeSeed<'de>,
        {
            let Some(remaining) = self.remaining.checked_sub(1) else {
                return Ok(None);
            };
            self.remaining = remaining;
            seed.deserialize(Deserializer::<C>::new(&mut *self.input))
                .map(Some)
        }

        fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            seed.deserialize(Deserializer::<C>::new(&mut *self.input))
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.hint)
        }
    }
}

mod wide_int {
    //! Restores 128-bit integer support to the serializer.
    //!
    //! `serde-wincode` 0.1.2 implements `serialize_i128`/`serialize_u128` on its
    //! `SizeOf` serializer but **not** on the `Serializer` that actually emits
    //! bytes, so those two methods fall through to `serde`'s default, which
    //! fails with "i128 is not supported". Its deserializer, meanwhile, handles
    //! both. Encoding a `u128` therefore fails at runtime even though decoding
    //! one works — an asymmetry that would be a latent data-loss trap the first
    //! time a persisted struct grows a 128-bit field.
    //!
    //! bincode 1 wrote 128-bit integers as 16 fixed-width little-endian bytes.
    //! serde-wincode writes tuple elements back to back with no length or
    //! framing bytes, so a `(u64, u64)` tuple holding the low half then the high
    //! half reproduces that layout exactly. `fixed_width_little_endian_integers`
    //! asserts the result against the byte vector captured from bincode 1.3.3.
    //!
    //! The wrapper is applied recursively: every value handed to a nested
    //! serializer is re-wrapped in [`Patch128`], so a 128-bit field nested inside
    //! a struct, enum, sequence or map is patched too — not just a top-level one.
    //!
    //! This module can be deleted wholesale if serde-wincode adds the two
    //! missing methods; `serialize` would then use `SerdeCompat<T>` directly.

    use serde::ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    };
    use serde::{Serialize, Serializer};

    /// Wraps a value so it is serialized through [`Patched`].
    #[repr(transparent)]
    pub(super) struct Patch128<T: ?Sized>(pub(super) T);

    impl<T> Serialize for Patch128<&T>
    where
        T: Serialize + ?Sized,
    {
        #[inline]
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.serialize(Patched(serializer))
        }
    }

    /// Wraps a *borrowed* value, used when forwarding elements/fields into the
    /// inner serializer so the patch keeps applying at every nesting level.
    struct Nested<'a, T: ?Sized>(&'a T);

    impl<T> Serialize for Nested<'_, T>
    where
        T: Serialize + ?Sized,
    {
        #[inline]
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.serialize(Patched(serializer))
        }
    }

    /// A `serde::Serializer` that adds 128-bit support and otherwise forwards
    /// verbatim to `S`.
    pub(super) struct Patched<S>(S);

    /// Forward a method to the inner serializer unchanged.
    macro_rules! forward {
        ($($method:ident($ty:ty)),* $(,)?) => {
            $(
                #[inline]
                fn $method(self, value: $ty) -> Result<Self::Ok, Self::Error> {
                    self.0.$method(value)
                }
            )*
        };
    }

    impl<S: Serializer> Serializer for Patched<S> {
        type Ok = S::Ok;
        type Error = S::Error;
        type SerializeSeq = Patched<S::SerializeSeq>;
        type SerializeTuple = Patched<S::SerializeTuple>;
        type SerializeTupleStruct = Patched<S::SerializeTupleStruct>;
        type SerializeTupleVariant = Patched<S::SerializeTupleVariant>;
        type SerializeMap = Patched<S::SerializeMap>;
        type SerializeStruct = Patched<S::SerializeStruct>;
        type SerializeStructVariant = Patched<S::SerializeStructVariant>;

        forward! {
            serialize_bool(bool),
            serialize_i8(i8),
            serialize_i16(i16),
            serialize_i32(i32),
            serialize_i64(i64),
            serialize_u8(u8),
            serialize_u16(u16),
            serialize_u32(u32),
            serialize_u64(u64),
            serialize_f32(f32),
            serialize_f64(f64),
            serialize_char(char),
            serialize_str(&str),
            serialize_bytes(&[u8]),
        }

        /// The whole point of this module: 16 fixed little-endian bytes, low
        /// half first, exactly as bincode 1 wrote them.
        #[inline]
        fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
            let mut tuple = self.0.serialize_tuple(2)?;
            tuple.serialize_element(&(value as u64))?;
            tuple.serialize_element(&((value >> 64) as u64))?;
            tuple.end()
        }

        /// Two's complement little-endian is bit-identical to the unsigned
        /// encoding, which is also what bincode 1 emitted.
        #[inline]
        fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
            self.serialize_u128(value as u128)
        }

        #[inline]
        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            self.0.serialize_none()
        }

        #[inline]
        fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize + ?Sized,
        {
            self.0.serialize_some(&Nested(value))
        }

        #[inline]
        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            self.0.serialize_unit()
        }

        #[inline]
        fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
            self.0.serialize_unit_struct(name)
        }

        #[inline]
        fn serialize_unit_variant(
            self,
            name: &'static str,
            index: u32,
            variant: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            self.0.serialize_unit_variant(name, index, variant)
        }

        #[inline]
        fn serialize_newtype_struct<T>(
            self,
            name: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize + ?Sized,
        {
            self.0.serialize_newtype_struct(name, &Nested(value))
        }

        #[inline]
        fn serialize_newtype_variant<T>(
            self,
            name: &'static str,
            index: u32,
            variant: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize + ?Sized,
        {
            self.0
                .serialize_newtype_variant(name, index, variant, &Nested(value))
        }

        #[inline]
        fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            self.0.serialize_seq(len).map(Patched)
        }

        #[inline]
        fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            self.0.serialize_tuple(len).map(Patched)
        }

        #[inline]
        fn serialize_tuple_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            self.0.serialize_tuple_struct(name, len).map(Patched)
        }

        #[inline]
        fn serialize_tuple_variant(
            self,
            name: &'static str,
            index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            self.0
                .serialize_tuple_variant(name, index, variant, len)
                .map(Patched)
        }

        #[inline]
        fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            self.0.serialize_map(len).map(Patched)
        }

        #[inline]
        fn serialize_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            self.0.serialize_struct(name, len).map(Patched)
        }

        #[inline]
        fn serialize_struct_variant(
            self,
            name: &'static str,
            index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            self.0
                .serialize_struct_variant(name, index, variant, len)
                .map(Patched)
        }

        /// Mirrors the inner serializer rather than serde's default, so the
        /// wrapper cannot change how a type chooses to encode itself.
        #[inline]
        fn is_human_readable(&self) -> bool {
            self.0.is_human_readable()
        }
    }

    /// Implement a compound-serializer trait by forwarding each value through
    /// [`Nested`], keeping the patch applied at every level of nesting.
    macro_rules! compound {
        ($trait:ident, $($method:ident),+ $(;)?) => {
            impl<S: $trait> $trait for Patched<S> {
                type Ok = S::Ok;
                type Error = S::Error;

                $(
                    #[inline]
                    fn $method<T>(&mut self, value: &T) -> Result<(), Self::Error>
                    where
                        T: Serialize + ?Sized,
                    {
                        self.0.$method(&Nested(value))
                    }
                )+

                #[inline]
                fn end(self) -> Result<Self::Ok, Self::Error> {
                    self.0.end()
                }
            }
        };
    }

    compound!(SerializeSeq, serialize_element);
    compound!(SerializeTuple, serialize_element);
    compound!(SerializeTupleStruct, serialize_field);
    compound!(SerializeTupleVariant, serialize_field);
    compound!(SerializeMap, serialize_key, serialize_value);

    /// `SerializeStruct`/`SerializeStructVariant` take a field name as well, so
    /// they do not fit the `compound!` shape.
    macro_rules! compound_named {
        ($trait:ident) => {
            impl<S: $trait> $trait for Patched<S> {
                type Ok = S::Ok;
                type Error = S::Error;

                #[inline]
                fn serialize_field<T>(
                    &mut self,
                    key: &'static str,
                    value: &T,
                ) -> Result<(), Self::Error>
                where
                    T: Serialize + ?Sized,
                {
                    self.0.serialize_field(key, &Nested(value))
                }

                #[inline]
                fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
                    self.0.skip_field(key)
                }

                #[inline]
                fn end(self) -> Result<Self::Ok, Self::Error> {
                    self.0.end()
                }
            }
        };
    }

    compound_named!(SerializeStruct);
    compound_named!(SerializeStructVariant);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{Header, Record, RecordBatch};
    use bytes::Bytes;
    use serde::Deserialize;
    use std::collections::BTreeMap;

    /// Allocation probe used by the corrupt-length tests.
    ///
    /// The guarantee those tests need is not merely "an error is returned" but
    /// "an error is returned *without* the claimed allocation being attempted".
    /// A pass-through global allocator that records the largest request made on
    /// the calling thread is the only way to observe that directly: a decoder
    /// that trusted the prefix would either panic in `Vec::with_capacity`
    /// (`capacity overflow`) or abort the process in `handle_alloc_error`, and
    /// neither is something a `Result`-based assertion could catch.
    mod alloc_probe {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            /// Largest single allocation requested while armed.
            static PEAK: Cell<usize> = const { Cell::new(0) };
            /// Whether this thread is currently measuring.
            static ARMED: Cell<bool> = const { Cell::new(false) };
        }

        struct Probe;

        /// Record `size` if this thread is measuring.
        ///
        /// Uses `const`-initialised `Cell`s and `try_with`, so it neither
        /// allocates (which would recurse) nor panics during thread teardown.
        fn record(size: usize) {
            let _ = ARMED.try_with(|armed| {
                if armed.get() {
                    let _ = PEAK.try_with(|peak| peak.set(peak.get().max(size)));
                }
            });
        }

        // SAFETY: every method forwards to `System` with the same arguments;
        // `record` performs no allocation of its own.
        unsafe impl GlobalAlloc for Probe {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                record(layout.size());
                unsafe { System.alloc(layout) }
            }

            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                record(layout.size());
                unsafe { System.alloc_zeroed(layout) }
            }

            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                record(new_size);
                unsafe { System.realloc(ptr, layout, new_size) }
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                unsafe { System.dealloc(ptr, layout) }
            }
        }

        #[global_allocator]
        static PROBE: Probe = Probe;

        /// Run `f` and report the largest single allocation it requested.
        pub(super) fn peak_allocation<R>(f: impl FnOnce() -> R) -> (R, usize) {
            PEAK.with(|peak| peak.set(0));
            ARMED.with(|armed| armed.set(true));
            let result = f();
            ARMED.with(|armed| armed.set(false));
            (result, PEAK.with(Cell::get))
        }
    }

    use alloc_probe::peak_allocation;

    // ── Golden vectors ──────────────────────────────────────────────────────
    //
    // Every `expected` hex literal below was captured from bincode **1.3.3**'s
    // top-level `bincode::serialize` before the 2.x migration. They are the
    // regression barrier for the on-disk format: if a future change to the
    // configuration in this module alters the encoding, these fail rather than
    // silently rendering existing segments, raft files and vector stores
    // unreadable.

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum GoldenMode {
        Local,
        Hybrid(u32),
        Diskless { shards: u16, replicated: bool },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct GoldenMetadata {
        name: String,
        partitions: u32,
        retention_ms: i64,
        mode: GoldenMode,
        labels: BTreeMap<String, String>,
        checksum: Option<u32>,
    }

    const GOLDEN_METADATA_HEX: &str = "06000000000000006576656e747303000000005c2605000000000200000007000102000000000000000300000000000000656e76040000000000000070726f640400000000000000746965720300000000000000686f7401efbeadde";

    const GOLDEN_RECORD_HEX: &str = "2a000000000000007b68e5cf8b0100000102000000000000006b31050000000000000068656c6c6f01000000000000000500000000000000747261636503000000000000006162630104030201";

    const GOLDEN_BATCH_HEX: &str = "0a000000000000000068e5cf8b01000002000000000000002a000000000000007b68e5cf8b0100000102000000000000006b31050000000000000068656c6c6f010000000000000005000000000000007472616365030000000000000061626301040302012b00000000000000010000000000000000010000000000000078000000000000000000";

    fn golden_metadata() -> GoldenMetadata {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("tier".to_string(), "hot".to_string());
        GoldenMetadata {
            name: "events".to_string(),
            partitions: 3,
            retention_ms: 86_400_000,
            mode: GoldenMode::Diskless {
                shards: 7,
                replicated: true,
            },
            labels,
            checksum: Some(0xDEAD_BEEF),
        }
    }

    fn golden_record() -> Record {
        Record {
            offset: 42,
            timestamp: 1_700_000_000_123,
            key: Some(Bytes::from_static(b"k1")),
            value: Bytes::from_static(b"hello"),
            headers: vec![Header {
                key: "trace".to_string(),
                value: Bytes::from_static(b"abc"),
            }],
            crc: Some(0x0102_0304),
        }
    }

    fn golden_batch() -> RecordBatch {
        let mut batch = RecordBatch::new(10, 1_700_000_000_000);
        batch.add_record(golden_record());
        batch.add_record(Record::new(43, 1, None, Bytes::from_static(b"x")));
        batch
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn unhex(s: &str) -> Vec<u8> {
        assert!(s.len() % 2 == 0, "hex literal must have even length");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    /// Decode using wincode's **stock** configuration instead of
    /// [`LegacyConfig`], so tests can demonstrate which defaults had to be
    /// overridden for bincode 1 parity.
    fn decode_with_wincode_defaults<T>(bytes: &[u8]) -> Result<T, wincode::ReadError>
    where
        T: DeserializeOwned,
    {
        type DefaultCfg = wincode::config::DefaultConfig;
        <SerdeCompat<T> as wincode::config::Deserialize<'_, DefaultCfg>>::deserialize(
            bytes,
            DefaultCfg::new(),
        )
    }

    /// Encode `value`, assert the bytes match bincode 1 exactly, then decode
    /// the *literal* bincode-1 bytes back and re-encode to confirm both
    /// directions are stable.
    fn assert_golden<T>(value: &T, expected_hex: &str)
    where
        T: Serialize + DeserializeOwned,
    {
        let encoded = serialize(value).expect("serialize");
        assert_eq!(
            hex(&encoded),
            expected_hex,
            "bincode 1 wire format changed for {}",
            std::any::type_name::<T>()
        );

        // Decode from the hard-coded bytes, not from what we just produced, so
        // the reader half is exercised against real bincode-1 output.
        let decoded: T = deserialize(&unhex(expected_hex)).expect("decode bincode-1 bytes");
        assert_eq!(
            hex(&serialize(&decoded).expect("re-serialize")),
            expected_hex,
            "decode/encode is not a fixed point for {}",
            std::any::type_name::<T>()
        );
    }

    #[test]
    fn fixed_width_little_endian_integers() {
        // bincode 1 encodes integers fixed-width and little-endian. bincode 2's
        // *default* configuration would varint-encode these instead, which is
        // exactly the silent corruption this module exists to prevent.
        assert_golden(&0x12u8, "12");
        assert_golden(&0x1234u16, "3412");
        assert_golden(&0x1234_5678u32, "78563412");
        assert_golden(&0x1234_5678_9abc_def0u64, "f0debc9a78563412");
        assert_golden(&-2i8, "fe");
        assert_golden(&-2i16, "feff");
        assert_golden(&-2i32, "feffffff");
        assert_golden(&-2i64, "feffffffffffffff");
        assert_golden(&0x1122_3344u128, "44332211000000000000000000000000");
        // usize is written as a fixed 8-byte u64, never as a varint.
        assert_golden(&300usize, "2c01000000000000");
    }

    #[test]
    fn scalars_and_primitives() {
        assert_golden(&1.5f32, "0000c03f");
        assert_golden(&1.5f64, "000000000000f83f");
        assert_golden(&true, "01");
        assert_golden(&false, "00");
        // char is raw UTF-8 with no length prefix.
        assert_golden(&'A', "41");
        assert_golden(&(1u8, 2u16), "010200");
    }

    #[test]
    fn length_prefixes_are_u64_little_endian() {
        assert_golden(&"hi".to_string(), "02000000000000006869");
        assert_golden(&vec![1u8, 2, 3], "0300000000000000010203");
        assert_golden(&vec![1u32, 2], "02000000000000000100000002000000");
        // `bytes::Bytes` takes serde's byte-buf path; same u64 length prefix.
        assert_golden(&Bytes::from_static(b"ab"), "02000000000000006162");
    }

    #[test]
    fn option_tags_are_single_bytes() {
        assert_golden(&Option::<u32>::None, "00");
        assert_golden(&Some(7u32), "0107000000");
    }

    #[test]
    fn enum_variant_indices_are_u32_little_endian() {
        assert_golden(&GoldenMode::Local, "00000000");
        assert_golden(&GoldenMode::Hybrid(9), "0100000009000000");
        assert_golden(
            &GoldenMode::Diskless {
                shards: 7,
                replicated: true,
            },
            "02000000070001",
        );
    }

    #[test]
    fn metadata_struct_round_trips_byte_for_byte() {
        assert_golden(&golden_metadata(), GOLDEN_METADATA_HEX);
    }

    #[test]
    fn record_round_trips_byte_for_byte() {
        assert_golden(&golden_record(), GOLDEN_RECORD_HEX);
    }

    #[test]
    fn record_batch_round_trips_byte_for_byte() {
        assert_golden(&golden_batch(), GOLDEN_BATCH_HEX);
    }

    #[test]
    fn decodes_record_written_by_bincode_1() {
        // Field-level check that the decoded values, not just the byte lengths,
        // survive a read of genuine bincode-1 output.
        let decoded: Record = deserialize(&unhex(GOLDEN_RECORD_HEX)).expect("decode");
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp, 1_700_000_000_123);
        assert_eq!(decoded.key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(decoded.value, Bytes::from_static(b"hello"));
        assert_eq!(decoded.headers.len(), 1);
        assert_eq!(decoded.headers[0].key, "trace");
        assert_eq!(decoded.headers[0].value, Bytes::from_static(b"abc"));
        assert_eq!(decoded.crc, Some(0x0102_0304));

        let batch: RecordBatch = deserialize(&unhex(GOLDEN_BATCH_HEX)).expect("decode batch");
        assert_eq!(batch.base_offset, 10);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[1].offset, 43);
        assert!(batch.records[1].key.is_none());
        assert_eq!(batch.records[1].crc, None);
    }

    #[test]
    fn trailing_bytes_are_ignored() {
        // bincode 1's top-level `deserialize` used `allow_trailing_bytes()`.
        // Segment, WAL and time-travel readers depend on it: batches are
        // decoded out of length-delimited and decompressed buffers that can be
        // larger than the encoded value itself.
        let mut encoded = unhex(GOLDEN_RECORD_HEX);
        let exact_len = encoded.len();
        encoded.extend_from_slice(&[0xAA; 16]);

        let decoded: Record = deserialize(&encoded).expect("trailing bytes must be tolerated");
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.value, Bytes::from_static(b"hello"));
        assert_eq!(exact_len + 16, encoded.len());

        // The same must hold for a whole batch.
        let mut batch_bytes = unhex(GOLDEN_BATCH_HEX);
        batch_bytes.extend_from_slice(&[0x00; 8]);
        let batch: RecordBatch = deserialize(&batch_bytes).expect("trailing bytes on batch");
        assert_eq!(batch.records.len(), 2);
    }

    #[test]
    fn truncated_input_still_errors() {
        // Tolerating *trailing* bytes must not decay into tolerating *missing*
        // bytes, which would let real corruption through undetected.
        let encoded = unhex(GOLDEN_RECORD_HEX);
        assert!(deserialize::<Record>(&encoded[..encoded.len() - 4]).is_err());
        assert!(deserialize::<Record>(&[]).is_err());
    }

    // ── 128-bit integer coverage ────────────────────────────────────────────

    /// `fixed_width_little_endian_integers` proves the top-level `u128` case.
    /// This proves the recursion in `wide_int`: serde-wincode's missing
    /// `serialize_u128`/`serialize_i128` must be patched at *every* nesting
    /// depth, or a 128-bit field buried inside a struct, enum, option,
    /// sequence, map or tuple would still fail to encode.
    #[test]
    fn wide_integers_are_patched_at_every_nesting_depth() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct WideInner {
            id: u128,
            tag: i128,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        enum WideVariant {
            Wrapped(u128),
        }

        // 0x11223344 as 16 little-endian bytes, then -2 as 16 two's-complement
        // little-endian bytes. Both are exactly what bincode 1 wrote.
        const ID_HEX: &str = "44332211000000000000000000000000";
        const TAG_HEX: &str = "feffffffffffffffffffffffffffffff";

        // Inside a struct.
        assert_golden(
            &WideInner {
                id: 0x1122_3344,
                tag: -2,
            },
            &format!("{ID_HEX}{TAG_HEX}"),
        );

        // Inside an enum variant (u32 little-endian discriminant first).
        assert_golden(
            &WideVariant::Wrapped(0x1122_3344),
            &format!("00000000{ID_HEX}"),
        );

        // Inside an Option (single-byte tag).
        assert_golden(&Some(0x1122_3344u128), &format!("01{ID_HEX}"));
        assert_golden(&Option::<u128>::None, "00");

        // Inside a sequence (u64 little-endian length prefix).
        assert_golden(
            &vec![0x1122_3344u128, 0x1122_3344],
            &format!("0200000000000000{ID_HEX}{ID_HEX}"),
        );

        // Inside a tuple (no framing between elements).
        assert_golden(&(0x1122_3344u128, 5u8), &format!("{ID_HEX}05"));

        // Inside a map value.
        let mut lookup: BTreeMap<String, u128> = BTreeMap::new();
        lookup.insert("k".to_string(), 0x1122_3344);
        assert_golden(
            &lookup,
            &format!("010000000000000001000000000000006b{ID_HEX}"),
        );

        // Deeply nested: struct inside enum inside vec inside option.
        let deep = Some(vec![WideVariant::Wrapped(0x1122_3344)]);
        let some_tag = "01";
        let vec_len_1 = "0100000000000000";
        let variant_0 = "00000000";
        assert_golden(&deep, &format!("{some_tag}{vec_len_1}{variant_0}{ID_HEX}"));
    }

    // ── Preallocation / large-length coverage ───────────────────────────────    //
    // These are new with the wincode migration. bincode had no preallocation
    // limit, wincode's default configuration does, and the golden vectors above
    // are all far too small to notice the difference.

    /// wincode's 4 MiB preallocation limit is the one default that had to be
    /// overridden. It rejects oversized `String`/`Vec<u8>`/`&[u8]` on the
    /// **write** path as well as the read path, so leaving it in place would
    /// have made any record value above 4 MiB both unwritable and unreadable —
    /// silently breaking existing segments rather than failing at compile time.
    #[test]
    fn sequences_larger_than_the_default_preallocation_limit() {
        /// 4 MiB + 4 KiB: just past wincode's `DEFAULT_PREALLOCATION_SIZE_LIMIT`.
        const OVER_LIMIT: usize = (4 << 20) + 4096;

        // Raw byte buffer.
        let big: Vec<u8> = (0..OVER_LIMIT).map(|i| (i % 251) as u8).collect();
        let encoded = serialize(&big).expect("serialize past the default 4 MiB limit");
        // bincode 1's layout: u64 little-endian length prefix, then raw bytes.
        assert_eq!(encoded.len(), 8 + OVER_LIMIT);
        assert_eq!(&encoded[..8], &(OVER_LIMIT as u64).to_le_bytes()[..]);
        let decoded: Vec<u8> = deserialize(&encoded).expect("decode past the default 4 MiB limit");
        assert_eq!(decoded, big);

        // Strings take wincode's length-encoded path, which is the one the
        // preallocation limit actually guards. (`Vec<u8>` does not: serde's
        // `Vec<T>` impl always uses `deserialize_seq`, never `deserialize_bytes`,
        // so its length is read as a plain fixed-width integer.)
        let big_string = "x".repeat(OVER_LIMIT);
        let encoded_string = serialize(&big_string).expect("serialize a >4 MiB String");
        assert_eq!(&encoded_string[..8], &(OVER_LIMIT as u64).to_le_bytes()[..]);
        let decoded: String = deserialize(&encoded_string).expect("decode a >4 MiB String");
        assert_eq!(decoded.len(), OVER_LIMIT);

        // The shape that actually occurs on disk: a record with a large value.
        // `Bytes` goes through serde's byte-buf path, so it is length-encoded
        // and preallocation-checked just like `String`.
        let record = Record {
            offset: 1,
            timestamp: 2,
            key: None,
            value: Bytes::from(vec![0x7Fu8; OVER_LIMIT]),
            headers: Vec::new(),
            crc: None,
        };
        let encoded = serialize(&record).expect("serialize a >4 MiB record value");
        let decoded: Record = deserialize(&encoded).expect("decode a >4 MiB record value");
        assert_eq!(decoded.value.len(), OVER_LIMIT);
        assert_eq!(decoded.offset, 1);

        // Proof the limit is real, and that disabling it was required rather
        // than cosmetic: the *same* bytes must be rejected when read back under
        // wincode's default configuration.
        //
        // The bite is on the read path. serde-wincode writes strings through the
        // `&str` schema, which does not range-check, but reads them back through
        // the `String` schema, which calls `read_prealloc_check`. A >4 MiB value
        // would therefore be written happily and then be permanently unreadable
        // — the worst possible failure mode for an on-disk format.
        assert!(
            decode_with_wincode_defaults::<String>(&encoded_string).is_err(),
            "wincode's default configuration no longer rejects reading a >4 MiB String — \
             recheck whether LegacyConfig still needs PREALLOCATION_SIZE_LIMIT_DISABLED"
        );

        // The same for a record whose `Bytes` value exceeds the limit, which is
        // the shape Streamline actually stores.
        assert!(
            decode_with_wincode_defaults::<Record>(&encoded).is_err(),
            "wincode's default configuration no longer rejects reading a record with a \
             >4 MiB value — recheck whether LegacyConfig still needs \
             PREALLOCATION_SIZE_LIMIT_DISABLED"
        );
    }

    /// Generic sequences take serde's seq path, where the element count is read
    /// as a fixed-width little-endian `u64` rather than through the length
    /// encoding. Exercise a count that needs more than 16 bits, so a narrower or
    /// varint prefix would be caught here.
    #[test]
    fn long_sequences_round_trip_with_u64_length_prefix() {
        const COUNT: usize = 200_000;

        let records: Vec<Record> = (0..COUNT as i64)
            .map(|i| Record::new(i, i, None, Bytes::from_static(b"v")))
            .collect();

        let encoded = serialize(&records).expect("serialize a long sequence");
        assert_eq!(&encoded[..8], &(COUNT as u64).to_le_bytes()[..]);

        let decoded: Vec<Record> = deserialize(&encoded).expect("decode a long sequence");
        assert_eq!(decoded.len(), COUNT);
        assert_eq!(decoded[COUNT - 1].offset, COUNT as i64 - 1);

        // A length prefix that overstates the payload must fail on the short
        // read rather than attempting a huge allocation.
        let mut lying = encoded.clone();
        lying[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(deserialize::<Vec<Record>>(&lying).is_err());
    }

    /// Complements `trailing_bytes_are_ignored`: the tolerance must not depend
    /// on the value being a struct, and must hold for a suffix far larger than
    /// the value itself — decompressed segment buffers are routinely padded
    /// well past the encoded batch.
    #[test]
    fn trailing_bytes_are_ignored_for_scalars_strings_and_long_suffixes() {
        let mut bytes = serialize(&0x1234_5678u32).expect("serialize u32");
        bytes.extend_from_slice(&[0xFF; 1024]);
        assert_eq!(
            deserialize::<u32>(&bytes).expect("u32 with trailing bytes"),
            0x1234_5678
        );

        let mut bytes = serialize(&"hi".to_string()).expect("serialize String");
        bytes.extend_from_slice(&[0x00; 4096]);
        assert_eq!(
            deserialize::<String>(&bytes).expect("String with trailing bytes"),
            "hi"
        );

        let mut bytes = serialize(&Option::<u32>::None).expect("serialize None");
        bytes.extend_from_slice(b"garbage");
        assert_eq!(
            deserialize::<Option<u32>>(&bytes).expect("Option with trailing bytes"),
            None
        );

        // A suffix many times larger than the value itself.
        let mut bytes = serialize(&golden_metadata()).expect("serialize metadata");
        let exact_len = bytes.len();
        bytes.extend_from_slice(&vec![0x5A; exact_len * 64]);
        assert_eq!(
            deserialize::<GoldenMetadata>(&bytes).expect("metadata with a large trailing suffix"),
            golden_metadata()
        );
    }

    // ── Corrupt length prefixes ─────────────────────────────────────────────
    //
    // Every owned sequence in this format is introduced by a `u64` that a
    // corrupt file controls completely. With the fixed preallocation limit
    // disabled (which >4 MiB record values require), nothing but the checks in
    // `length_guard` and `bounded_de` stands between that `u64` and
    // `Vec::with_capacity`.
    //
    // Each test below asserts *both* halves of the guarantee: an error is
    // returned, and the claimed allocation was never attempted.

    /// Length prefixes a corrupt file might contain.
    ///
    /// `u64::MAX` and `u64::MAX / 2` exceed `isize::MAX`, so a decoder that
    /// trusted them would panic inside `Vec::with_capacity`. `1 << 40` and
    /// `1 << 33` are *allocatable* claims — the allocator would very likely hand
    /// back the mapping — so only the probe can tell whether they were honoured.
    const CORRUPT_LENGTHS: &[u64] = &[u64::MAX, u64::MAX / 2, 1 << 40, 1 << 33];

    /// The largest single allocation a bounded decode may request, over and
    /// above the input it was given.
    ///
    /// The invariant being asserted is "allocation is bounded by the input", so
    /// the ceiling is `max(this, input length)`: a decode is allowed to allocate
    /// for the bytes it genuinely holds (a valid 8 MiB record value really does
    /// need 8 MiB) but never for bytes it does not. serde additionally clamps
    /// sequence preallocation to 1 MiB worth of elements, hence the floor. Every
    /// entry of `CORRUPT_LENGTHS` is orders of magnitude above both.
    const ALLOCATION_CEILING: usize = 4 << 20;

    /// Overwrite the little-endian `u64` at `offset`.
    fn corrupt_u64_at(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Assert that decoding `bytes` as `T` fails without allocating for bytes
    /// the input does not actually contain.
    fn assert_rejects_without_allocating<T>(bytes: &[u8], what: &str)
    where
        T: DeserializeOwned,
    {
        let ceiling = ALLOCATION_CEILING.max(bytes.len());
        let (result, peak) = peak_allocation(|| deserialize::<T>(bytes));

        let err = match result {
            Ok(_) => panic!("{what}: a corrupt length prefix decoded successfully"),
            Err(err) => err,
        };
        assert!(
            peak <= ceiling,
            "{what}: rejected with `{err}`, but only after requesting {peak} bytes from an \
             input of {} — the length prefix must be checked against the remaining input \
             *before* allocating",
            bytes.len()
        );
    }

    #[test]
    fn corrupt_top_level_length_prefixes_error_without_allocating() {
        for &claimed in CORRUPT_LENGTHS {
            // `String` takes wincode's length-checked schema.
            let mut bytes = serialize(&"hello".to_string()).expect("serialize String");
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<String>(&bytes, &format!("String/{claimed}"));

            // `Vec<u8>` takes serde's generic sequence path.
            let mut bytes = serialize(&vec![1u8, 2, 3]).expect("serialize Vec<u8>");
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<Vec<u8>>(&bytes, &format!("Vec<u8>/{claimed}"));

            // `bytes::Bytes` takes serde's `deserialize_byte_buf` path, which is
            // wincode's `Vec<u8>` schema — the shape `Record::value` uses.
            let mut bytes = serialize(&Bytes::from_static(b"hello")).expect("serialize Bytes");
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<Bytes>(&bytes, &format!("Bytes/{claimed}"));

            // A sequence whose elements are neither bytes nor zero-width.
            let mut bytes = serialize(&vec!["a".to_string()]).expect("serialize Vec<String>");
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<Vec<String>>(
                &bytes,
                &format!("Vec<String>/{claimed}"),
            );

            // A map, which reaches `deserialize_map` rather than `deserialize_seq`.
            let mut lookup = BTreeMap::new();
            lookup.insert("k".to_string(), 1u32);
            let mut bytes = serialize(&lookup).expect("serialize BTreeMap");
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<BTreeMap<String, u32>>(
                &bytes,
                &format!("BTreeMap/{claimed}"),
            );
        }
    }

    /// Byte offsets of the four `u64` length prefixes inside `GOLDEN_RECORD_HEX`,
    /// which is the real `Record` layout:
    ///
    /// ```text
    /// 0  offset i64 | 8  timestamp i64 | 16 key tag | 17 key len | 25 "k1"
    /// 27 value len  | 35 "hello"       | 40 headers len
    /// 48 header key len | 56 "trace"   | 61 header value len | 69 "abc"
    /// 72 crc tag    | 73 crc u32
    /// ```
    const RECORD_LENGTH_OFFSETS: &[(usize, &str)] = &[
        (17, "Record::key (Option<Bytes>)"),
        (27, "Record::value (Bytes)"),
        (40, "Record::headers (Vec<Header>)"),
        (48, "Header::key (String)"),
        (61, "Header::value (Bytes)"),
    ];

    #[test]
    fn corrupt_length_prefixes_inside_a_record_error_without_allocating() {
        let golden = unhex(GOLDEN_RECORD_HEX);
        assert_eq!(
            golden.len(),
            77,
            "the Record layout changed; RECORD_LENGTH_OFFSETS is now wrong"
        );
        // Sanity: the fixture really is decodable before it is corrupted.
        deserialize::<Record>(&golden).expect("golden record decodes");

        for &claimed in CORRUPT_LENGTHS {
            for &(offset, field) in RECORD_LENGTH_OFFSETS {
                let mut bytes = golden.clone();
                corrupt_u64_at(&mut bytes, offset, claimed);
                assert_rejects_without_allocating::<Record>(
                    &bytes,
                    &format!("Record {field} @ {offset}/{claimed}"),
                );
            }

            // The same corruption one level deeper: a batch of records.
            let mut bytes = unhex(GOLDEN_BATCH_HEX);
            // 0..8 base_offset, 8..16 timestamp, 16..24 records length.
            corrupt_u64_at(&mut bytes, 16, claimed);
            assert_rejects_without_allocating::<RecordBatch>(
                &bytes,
                &format!("RecordBatch::records/{claimed}"),
            );
        }
    }

    #[cfg(feature = "clustering")]
    #[test]
    fn corrupt_length_prefixes_inside_cluster_metadata_error_without_allocating() {
        use crate::cluster::node::{BrokerInfo, NodeState};
        use crate::cluster::raft::state_machine::ClusterMetadata;

        let mut metadata = ClusterMetadata::new("streamline-cluster".to_string());
        metadata.register_broker(BrokerInfo {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().expect("addr"),
            inter_broker_addr: "127.0.0.1:9093".parse().expect("addr"),
            rack: Some("rack-a".to_string()),
            datacenter: None,
            state: NodeState::Running,
        });
        metadata.controller_id = Some(1);
        metadata.version = 7;

        let encoded = serialize(&metadata).expect("serialize ClusterMetadata");
        let decoded: ClusterMetadata = deserialize(&encoded).expect("round trip");
        assert_eq!(decoded.cluster_id, metadata.cluster_id);
        assert_eq!(decoded.brokers.len(), 1);
        assert_eq!(decoded.version, 7);

        for &claimed in CORRUPT_LENGTHS {
            // Offset 0 is `cluster_id`'s `String` length; offset 8 + the string
            // is the `brokers` map length. Corrupting the first covers the
            // allocating schema path, the second the map path.
            let mut bytes = encoded.clone();
            corrupt_u64_at(&mut bytes, 0, claimed);
            assert_rejects_without_allocating::<ClusterMetadata>(
                &bytes,
                &format!("ClusterMetadata::cluster_id/{claimed}"),
            );

            let brokers_len_offset = 8 + metadata.cluster_id.len();
            let mut bytes = encoded.clone();
            corrupt_u64_at(&mut bytes, brokers_len_offset, claimed);
            assert_rejects_without_allocating::<ClusterMetadata>(
                &bytes,
                &format!("ClusterMetadata::brokers/{claimed}"),
            );
        }
    }

    /// The bound is the remaining input, not a constant: a prefix one element
    /// past the end must fail even though it is tiny, and a multi-megabyte
    /// prefix must succeed when the bytes are genuinely there.
    #[test]
    fn the_bound_is_the_remaining_input_not_a_fixed_ceiling() {
        // One byte short.
        let mut bytes = serialize(&"hello".to_string()).expect("serialize");
        corrupt_u64_at(&mut bytes, 0, 6);
        assert!(
            deserialize::<String>(&bytes).is_err(),
            "a prefix one byte past the end of the input must fail"
        );

        // Exactly the input: still fine.
        let bytes = serialize(&"hello".to_string()).expect("serialize");
        assert_eq!(deserialize::<String>(&bytes).expect("exact fit"), "hello");

        // The same for a sequence: 4 elements declared, 3 present.
        let mut bytes = serialize(&vec![1u32, 2, 3]).expect("serialize");
        corrupt_u64_at(&mut bytes, 0, 4);
        assert!(
            deserialize::<Vec<u32>>(&bytes).is_err(),
            "a sequence declaring more elements than the input holds must fail"
        );
    }

    /// The guard must not resurrect the 4 MiB limit it replaced: values far
    /// above it still round-trip, because their bytes are really present.
    #[test]
    fn record_with_a_value_larger_than_the_default_limit_round_trips() {
        /// 4 MiB + 4 KiB: just past wincode's `DEFAULT_PREALLOCATION_SIZE_LIMIT`.
        const OVER_LIMIT: usize = (4 << 20) + 4096;

        let record = Record {
            offset: 9,
            timestamp: 11,
            key: Some(Bytes::from(vec![0x11u8; OVER_LIMIT])),
            value: Bytes::from(vec![0x7Fu8; OVER_LIMIT]),
            headers: vec![Header {
                key: "x".repeat(OVER_LIMIT),
                value: Bytes::from(vec![0x22u8; OVER_LIMIT]),
            }],
            crc: None,
        };

        let encoded = serialize(&record).expect("serialize a record with >4 MiB fields");
        let decoded: Record = deserialize(&encoded).expect("decode a record with >4 MiB fields");
        assert_eq!(decoded.value.len(), OVER_LIMIT);
        assert_eq!(decoded.key.as_ref().map(|k| k.len()), Some(OVER_LIMIT));
        assert_eq!(decoded.headers[0].key.len(), OVER_LIMIT);
        assert_eq!(decoded.headers[0].value.len(), OVER_LIMIT);

        // Truncating that payload must still fail rather than allocate blindly.
        assert_rejects_without_allocating::<Record>(
            &encoded[..encoded.len() / 2],
            "truncated >4 MiB record",
        );
    }

    /// Empty and zero-width sequences must keep decoding: the bound may never be
    /// derived by inferring an element width, because an empty sequence has no
    /// element to infer from.
    #[test]
    fn empty_and_zero_width_sequences_still_decode() {
        assert_eq!(
            deserialize::<Vec<u8>>(&serialize(&Vec::<u8>::new()).expect("ser")).expect("de"),
            Vec::<u8>::new()
        );
        assert_eq!(
            deserialize::<String>(&serialize(&String::new()).expect("ser")).expect("de"),
            ""
        );
        assert_eq!(
            deserialize::<Bytes>(&serialize(&Bytes::new()).expect("ser")).expect("de"),
            Bytes::new()
        );
        assert!(deserialize::<BTreeMap<String, u32>>(
            &serialize(&BTreeMap::<String, u32>::new()).expect("ser")
        )
        .expect("de")
        .is_empty());
        assert!(
            deserialize::<Vec<Record>>(&serialize(&Vec::<Record>::new()).expect("ser"))
                .expect("de")
                .is_empty()
        );

        // Zero-width elements carry no bytes at all, so the "one byte per
        // element" bound cannot apply to them. A reasonable count must survive
        // a round trip even though the encoding is only the 8-byte prefix.
        let units = vec![(); 4096];
        let encoded = serialize(&units).expect("serialize Vec<()>");
        assert_eq!(
            encoded.len(),
            8,
            "zero-width elements must encode to nothing"
        );
        assert_eq!(
            deserialize::<Vec<()>>(&encoded)
                .expect("decode Vec<()>")
                .len(),
            4096
        );

        // ...but a `u64::MAX` count of them must not become an unbounded spin.
        let mut corrupt = encoded.clone();
        corrupt_u64_at(&mut corrupt, 0, u64::MAX);
        assert_rejects_without_allocating::<Vec<()>>(&corrupt, "Vec<()>/u64::MAX");
    }

    /// `deserialize_any`/`ignored_any`/identifiers cannot be represented in a
    /// self-describing-free format. The bridge must reject them the same way
    /// serde-wincode does rather than silently mis-decode.
    #[test]
    fn non_self_describing_features_are_rejected() {
        use serde::de::IgnoredAny;

        let bytes = serialize(&7u32).expect("serialize");
        assert!(deserialize::<IgnoredAny>(&bytes).is_err());

        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum Untagged {
            #[allow(dead_code)]
            Number(u32),
        }
        // `untagged` requires `deserialize_any`.
        assert!(deserialize::<Untagged>(&bytes).is_err());
    }
}
