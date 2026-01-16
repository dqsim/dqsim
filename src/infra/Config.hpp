#pragma once
//---------------------------------------------------------------------------
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
#ifdef __clang__
#define CLANG_REINITIALIZES [[clang::reinitializes]]
#define CLANG_NO_SANITIZE_FUNCTION [[clang::no_sanitize("function")]]
#define CLANG_LIFETIMEBOUND [[clang::lifetimebound]]
#else
#define CLANG_REINITIALIZES
#define CLANG_NO_SANITIZE_FUNCTION
#define CLANG_LIFETIMEBOUND
#endif
//---------------------------------------------------------------------------
/// Unreachable code
[[noreturn]] inline void unreachable() noexcept {
   assert(false && "unreachable");
   __builtin_unreachable();
}
//---------------------------------------------------------------------------
/// Assert an axiom. Hints the compiler to the truth of an expression.
inline constexpr void assertAxiom(bool truth) noexcept {
   if (!truth) {
      assert(truth);
      __builtin_unreachable();
   }
}
//---------------------------------------------------------------------------
/// Convert with range assert
template <class T, class S>
inline constexpr T convert(S v) {
   assert(std::in_range<T>(v));
   return static_cast<T>(v);
}
//---------------------------------------------------------------------------
/// A 128bit data type
struct data128_t {
   uint64_t values[2];
};
//---------------------------------------------------------------------------
/// A 128bit signed integer type
using int128_t = __int128;
//---------------------------------------------------------------------------
/// A 128bit unsigned integer type
using uint128_t = unsigned __int128;
//---------------------------------------------------------------------------
/// Wrapper for unaligned data types
template <class T>
struct [[gnu::packed]] unaligned {
   T value;
   /// Load the value
   constexpr T get() const noexcept { return value; }
   /// Implicit conversion to the value
   constexpr operator T() const noexcept { return value; }
   /// Implicit converting constructor
   constexpr unaligned(T value) noexcept : value(value) {}
   /// Get the potentially unaligned address
   void* getPtr() noexcept { return this; }
   /// Get the potentially unaligned address
   const void* getPtr() const noexcept { return this; }
};
static_assert(alignof(unaligned<void*>) == 1, "unaligned should be packed");
//---------------------------------------------------------------------------
/// Wrapper for unaligned arrays
template <class T, std::size_t N>
struct [[gnu::packed]] unaligned_array {
   T value[N];
   /// Array operator
   unaligned<T>& operator[](std::size_t pos) noexcept {
      assertAxiom(pos < N);
      return *reinterpret_cast<unaligned<T>*>(&value[pos]);
   }
   /// Array operator
   const unaligned<T>& operator[](std::size_t pos) const noexcept {
      assertAxiom(pos < N);
      return *reinterpret_cast<const unaligned<T>*>(&value[pos]);
   }
   /// Implicit pointer decay of the array
   operator const unaligned<T>*() const noexcept { return reinterpret_cast<const unaligned<T>*>(value); }
   /// Iterator
   const unaligned<T>* begin() const noexcept { return reinterpret_cast<const unaligned<T>*>(value); }
   /// Iterator
   const unaligned<T>* end() const noexcept { return reinterpret_cast<const unaligned<T>*>(value + N); }
   /// Implicit pointer decay of the array
   operator unaligned<T>*() noexcept { return reinterpret_cast<unaligned<T>*>(value); }
   /// Iterator
   unaligned<T>* begin() noexcept { return reinterpret_cast<const unaligned<T>*>(value); }
   /// Iterator
   unaligned<T>* end() noexcept { return reinterpret_cast<const unaligned<T>*>(value + N); }
};
//---------------------------------------------------------------------------
/// Unaligned load
template <class T>
[[gnu::always_inline]] inline T unalignedLoad(const void* ptr) noexcept { return reinterpret_cast<const unaligned<T>*>(ptr)->value; }
template <class T>
[[gnu::always_inline]] inline T unalignedArrayLoad(const void* ptr, intptr_t index) noexcept { return reinterpret_cast<const unaligned<T>*>(ptr)[index].value; }
//---------------------------------------------------------------------------
/// Store unaligned
template <class T>
[[gnu::always_inline]] inline void unalignedStore(void* ptr, T value) noexcept { reinterpret_cast<unaligned<T>*>(ptr)->value = value; }
template <class T>
[[gnu::always_inline]] inline void unalignedArrayStore(void* ptr, intptr_t index, T value) noexcept { reinterpret_cast<unaligned<T>*>(ptr)[index].value = value; }
//---------------------------------------------------------------------------
/// Adjust a pointer value by an absolute offset
template <class T>
[[gnu::always_inline]] inline void adjustPointer(T*& ptr, intptr_t delta) { ptr = reinterpret_cast<T*>(reinterpret_cast<char*>(ptr) + delta); }
//---------------------------------------------------------------------------
namespace ByteOrder {
/// Are we using a big endian machine?
constexpr const bool bigEndian =
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
   true
#else
   false
#endif
   ;
}
//---------------------------------------------------------------------------
namespace CompilerToolchain {
/// Is this build intended for debugging?
constexpr const bool debugBuild =
#ifdef NDEBUG
   false
#else
   true
#endif
   ;
/// Is this build optimized?
constexpr const bool optimizedBuild =
#ifdef __OPTIMIZE__
   true
#else
   false
#endif
   ;
/// Is this build for x86-64
constexpr const bool x86_64 =
#ifdef __x86_64__
   true
#else
   false
#endif
   ;
/// Is this build for arm64
constexpr const bool arm64 =
#ifdef __aarch64__
   true
#else
   false
#endif
   ;
}
//---------------------------------------------------------------------------
// NOLINTNEXTLINE
#define _LIBCPP_ENABLE_CXX20_REMOVED_TYPE_TRAITS
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
