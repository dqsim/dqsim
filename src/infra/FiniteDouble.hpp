#pragma once
//---------------------------------------------------------------------------
#include <cassert>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
class NonZeroDouble;
//---------------------------------------------------------------------------
class NonZeroDouble;
//---------------------------------------------------------------------------
#define GENOPS(T)                                                                       \
   constexpr auto operator==(T other) const { return v == static_cast<double>(other); } \
   constexpr auto operator!=(T other) const { return v != static_cast<double>(other); } \
   constexpr auto operator<(T other) const { return v < static_cast<double>(other); }   \
   constexpr auto operator>(T other) const { return v > static_cast<double>(other); }   \
   constexpr auto operator<=(T other) const { return v <= static_cast<double>(other); } \
   constexpr auto operator>=(T other) const { return v >= static_cast<double>(other); }
//---------------------------------------------------------------------------
/// A floating point value will always contain finite values
class FiniteDouble {
   protected:
   /// The largest possile value
   static constexpr double maxValue = __DBL_MAX__;
   /// The largest possile value
   static constexpr double minValue = -__DBL_MAX__;

   /// The underlying value
   double v;

   /// Adjust a non-finite value
   static double adjustNonFinite(double v) { return __builtin_isnan(v) ? 0 : ((v < 0) ? minValue : maxValue); }

   public:
   /// Constructor
   constexpr FiniteDouble() : v(0) {}
   /// Constructor
   explicit constexpr FiniteDouble(double v) : v(__builtin_expect(__builtin_isfinite(v), 1) ? v : adjustNonFinite(v)) {}
   /// Constructor
   explicit constexpr FiniteDouble(float v) : FiniteDouble(static_cast<double>(v)) {}
   /// Constructor
   explicit constexpr FiniteDouble(long double v) : FiniteDouble(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(int v) : v(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(long v) : v(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(long long v) : v(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(unsigned v) : v(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(unsigned long v) : v(static_cast<double>(v)) {}
   /// Constructor
   constexpr FiniteDouble(unsigned long long v) : v(static_cast<double>(v)) {}

   /// Maximum supported value
   static constexpr FiniteDouble max() { return FiniteDouble(maxValue); }
   /// Minimum supported value
   static constexpr FiniteDouble min() { return FiniteDouble(minValue); }

   /// Convert back to double
   constexpr operator double() const { return v; }

   /// Arithmetic
   constexpr FiniteDouble& operator+=(FiniteDouble other);
   /// Arithmetic
   constexpr FiniteDouble& operator-=(FiniteDouble other);
   /// Arithmetic
   constexpr FiniteDouble& operator*=(FiniteDouble other);
   /// Arithmetic
   constexpr FiniteDouble& operator/=(NonZeroDouble other);
   /// Comparison
   constexpr auto operator==(const FiniteDouble& other) const { return v == other.v; }
   /// Comparison
   constexpr auto operator!=(const FiniteDouble& other) const { return v != other.v; }
   /// Comparison
   constexpr auto operator<(const FiniteDouble& other) const { return v < other.v; }
   /// Comparison
   constexpr auto operator>(const FiniteDouble& other) const { return v > other.v; }
   /// Comparison
   constexpr auto operator<=(const FiniteDouble& other) const { return v <= other.v; }
   /// Comparison
   constexpr auto operator>=(const FiniteDouble& other) const { return v >= other.v; }
   GENOPS(float);
   GENOPS(double);
   GENOPS(int);
   GENOPS(long);
   GENOPS(long long);
   GENOPS(unsigned);
   GENOPS(unsigned long);
   GENOPS(unsigned long long);

   /// Exp
   static FiniteDouble exp(FiniteDouble v) { return FiniteDouble(__builtin_exp(v)); }
   /// Exp
   static FiniteDouble exp10(FiniteDouble v);
   /// Log
   static FiniteDouble log(NonZeroDouble v);
   /// Log
   static FiniteDouble log2(NonZeroDouble v);
   /// Log
   static FiniteDouble log10(NonZeroDouble v);
   /// Sqrt
   static FiniteDouble sqrt(FiniteDouble v);
   /// Exp
   static FiniteDouble pow(FiniteDouble v, FiniteDouble e) { return FiniteDouble(__builtin_pow(v, e)); }
};
//---------------------------------------------------------------------------
#undef GENOPS
//---------------------------------------------------------------------------
constexpr FiniteDouble operator""_fd(unsigned long long v) { return FiniteDouble(v); }
constexpr FiniteDouble operator""_fd(long double v) { return FiniteDouble(v); }
//---------------------------------------------------------------------------
/// A floating point value will always contain finite and nonzero values
/// The bounds for NonZeroDouble are relatively large for safety
class NonZeroDouble : public FiniteDouble {
   public:
   /// The smallest value allowed
   static constexpr double epsilon = 1e-15;

   private:
   /// Adjust a non-finite value
   static double adjustSmall(double v) { return __builtin_copysign(epsilon, v); }

   public:
   /// Constructor
   explicit constexpr NonZeroDouble(double v) : FiniteDouble(v) {
      this->v = __builtin_expect(__builtin_fabsl(this->v) < epsilon, 0) ? adjustSmall(this->v) : this->v;
   }

   /// Return maximum of given value and 1.0
   static constexpr NonZeroDouble max1(FiniteDouble v) { return NonZeroDouble(v < 1.0 ? 1.0_fd : v); }

   /// Convert back to double
   using FiniteDouble::operator double;

   /// Arithmetic
   constexpr NonZeroDouble operator-() const { return NonZeroDouble(-FiniteDouble::operator double()); }
   /// Comparison
   constexpr auto operator==(const NonZeroDouble& other) const { return FiniteDouble::operator double() == other.operator double(); }
   /// Comparison
   constexpr auto operator!=(const NonZeroDouble& other) const { return FiniteDouble::operator double() != other.operator double(); }
   /// Comparison
   constexpr auto operator<(const NonZeroDouble& other) const { return FiniteDouble::operator double() < other.operator double(); }
   /// Comparison
   constexpr auto operator>(const NonZeroDouble& other) const { return FiniteDouble::operator double() > other.operator double(); }
   /// Comparison
   constexpr auto operator<=(const NonZeroDouble& other) const { return FiniteDouble::operator double() <= other.operator double(); }
   /// Comparison
   constexpr auto operator>=(const NonZeroDouble& other) const { return FiniteDouble::operator double() >= other.operator double(); }
};
//---------------------------------------------------------------------------
constexpr FiniteDouble operator-(FiniteDouble self) { return FiniteDouble(-static_cast<double>(self)); }
constexpr FiniteDouble operator+(FiniteDouble self, FiniteDouble other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, FiniteDouble other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, FiniteDouble other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator/(FiniteDouble self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(int self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(long self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(long long self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(unsigned int self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(unsigned long self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator/(unsigned long long self, NonZeroDouble other) { return FiniteDouble(static_cast<double>(self) / static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, int other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, int other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, int other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, long other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, long other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, long other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, long long other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, long long other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, long long other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, unsigned int other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, unsigned int other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, unsigned int other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, unsigned long other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, unsigned long other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, unsigned long other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, unsigned long long other) { return FiniteDouble(static_cast<double>(self) + static_cast<double>(other)); }
constexpr FiniteDouble operator-(FiniteDouble self, unsigned long long other) { return FiniteDouble(static_cast<double>(self) - static_cast<double>(other)); }
constexpr FiniteDouble operator*(FiniteDouble self, unsigned long long other) { return FiniteDouble(static_cast<double>(self) * static_cast<double>(other)); }
constexpr FiniteDouble operator+(FiniteDouble self, float other) = delete;
constexpr FiniteDouble operator+(FiniteDouble self, double other) = delete;
constexpr FiniteDouble operator-(FiniteDouble self, float other) = delete;
constexpr FiniteDouble operator-(FiniteDouble self, double other) = delete;
constexpr FiniteDouble operator*(FiniteDouble self, float other) = delete;
constexpr FiniteDouble operator*(FiniteDouble self, double other) = delete;
constexpr FiniteDouble operator/(FiniteDouble self, float other) = delete;
constexpr FiniteDouble operator/(FiniteDouble self, double other) = delete;
constexpr FiniteDouble operator+(int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(unsigned int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(unsigned int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(unsigned int other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(unsigned long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(unsigned long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(unsigned long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(unsigned long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) + static_cast<double>(self)); }
constexpr FiniteDouble operator-(unsigned long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) - static_cast<double>(self)); }
constexpr FiniteDouble operator*(unsigned long long other, FiniteDouble self) { return FiniteDouble(static_cast<double>(other) * static_cast<double>(self)); }
constexpr FiniteDouble operator+(float other, FiniteDouble self) = delete;
constexpr FiniteDouble operator+(double other, FiniteDouble self) = delete;
constexpr FiniteDouble operator-(float other, FiniteDouble self) = delete;
constexpr FiniteDouble operator-(double other, FiniteDouble self) = delete;
constexpr FiniteDouble operator*(float other, FiniteDouble self) = delete;
constexpr FiniteDouble operator*(double other, FiniteDouble self) = delete;
constexpr FiniteDouble operator/(float other, FiniteDouble self) = delete;
constexpr FiniteDouble operator/(double other, FiniteDouble self) = delete;
constexpr FiniteDouble& FiniteDouble::operator+=(FiniteDouble other) { return *this = *this + other; }
constexpr FiniteDouble& FiniteDouble::operator-=(FiniteDouble other) { return *this = *this - other; }
constexpr FiniteDouble& FiniteDouble::operator*=(FiniteDouble other) { return *this = *this * other; }
constexpr FiniteDouble& FiniteDouble::operator/=(NonZeroDouble other) { return *this = *this / other; }
//---------------------------------------------------------------------------
constexpr NonZeroDouble operator""_nz(unsigned long long v) { return NonZeroDouble(static_cast<double>(v)); }
constexpr NonZeroDouble operator""_nz(long double v) { return NonZeroDouble(static_cast<double>(v)); }
//---------------------------------------------------------------------------
inline FiniteDouble FiniteDouble::log(NonZeroDouble v)
// Log
{
   assert(v > 0);
   return FiniteDouble(__builtin_log(v));
}
//---------------------------------------------------------------------------
inline FiniteDouble FiniteDouble::log2(NonZeroDouble v)
// Log
{
   assert(v > 0);
   return FiniteDouble(__builtin_log2(v));
}
//---------------------------------------------------------------------------
inline FiniteDouble FiniteDouble::log10(NonZeroDouble v)
// Log
{
   assert(v > 0);
   return FiniteDouble(__builtin_log10(v));
}
//---------------------------------------------------------------------------
inline FiniteDouble FiniteDouble::sqrt(FiniteDouble v)
// Sqrt
{
   assert(v >= 0);
   return FiniteDouble(__builtin_sqrt(v));
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
