#pragma once
//---------------------------------------------------------------------------
#include <array>
#include <cassert>
#include <string_view>
#include <utility>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
/// Base class for customization
template <class E>
struct EnumInfoCustomizationBase {
   static consteval std::string_view overrideName(E) { return {}; }
};
//---------------------------------------------------------------------------
/// Hook for customization
template <class E>
struct EnumInfoCustomization : public EnumInfoCustomizationBase<E> {
};
//---------------------------------------------------------------------------
namespace enumdetails {
//---------------------------------------------------------------------------
/// Extract the name from __PRETTY_FUNCTION__
consteval std::string_view extractFromPrettyFunction(std::string_view name) noexcept {
   // Take only the text at the end
   for (size_t index = name.length(); index > 0; --index) {
      char c = name[index - 1];
      if (!(((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) || ((c >= '0') && (c <= '9')) || (c == '_'))) {
         name.remove_prefix(index);
         break;
      }
   }

   // Check if we have found a valid name
   if (!name.empty()) {
      char c = name[0];
      if (((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) || (c == '_'))
         return name;
   }

   // No
   return {};
}
//---------------------------------------------------------------------------
/// Get the name of an enum value
template <typename E, E V>
consteval auto n() {
   static_assert(std::is_enum_v<E>);
   return extractFromPrettyFunction({__PRETTY_FUNCTION__, sizeof(__PRETTY_FUNCTION__) - 2});
}
//---------------------------------------------------------------------------
/// Get the name of an enum value
template <typename E, E V>
consteval auto getPrettyName() {
   auto o = EnumInfoCustomization<E>::overrideName(V);
   if (!o.empty()) return o;
   return n<E, V>();
}
//---------------------------------------------------------------------------
/// Get the names of all enum entries
template <typename E, std::size_t... I>
consteval auto getNames(std::index_sequence<I...>) noexcept {
   return std::array<std::string_view, sizeof...(I)>{{getPrettyName<E, static_cast<E>(I)>()...}};
}
//---------------------------------------------------------------------------
/// Get the length of the name descriptor
template <typename E, std::size_t count>
consteval unsigned getNameDescriptorLength() {
   auto names = getNames<E>(std::make_index_sequence<count>{});
   size_t total = 0;
   for (auto n : names) total += n.length();
   assert(std::in_range<unsigned>(total));
   return static_cast<unsigned>(total);
}
//---------------------------------------------------------------------------
/// Get the (lower case) name descriptor
template <typename E, std::size_t count, unsigned totalLen>
consteval std::array<char, totalLen> getNameDescriptor() {
   auto names = getNames<E>(std::make_index_sequence<count>{});
   std::array<char, totalLen> result;
   size_t slot = 0;
   for (auto n : names) {
      for (char c : n) {
         if ((c >= 'A') && (c <= 'Z')) c += 'a' - 'A';
         result[slot++] = c;
      }
   }
   return result;
}
//---------------------------------------------------------------------------
/// Get the name ends
template <typename E, std::size_t count>
consteval std::array<unsigned, count> getNameDescriptorEnds() {
   auto names = getNames<E>(std::make_index_sequence<count>{});
   std::array<unsigned, count> result;
   size_t total = 0, slot = 0;
   for (auto n : names) {
      total += n.length();
      assert(std::in_range<unsigned>(total));
      result[slot++] = static_cast<unsigned>(total);
   }
   return result;
}
//---------------------------------------------------------------------------
/// Check that one array is a prefix of another
template <std::size_t l1, std::size_t l2>
consteval bool hasSameNamePrefix(const char* names1, const std::array<unsigned, l1>& ends1, const char* names2, const std::array<unsigned, l2>& ends2) {
   if (l1 >= l2) return false;
   for (std::size_t index = 0; index < l1; ++index)
      if (ends1[index] != ends2[index])
         return false;
   for (unsigned index = 0; index != ends1[l1 - 1]; ++index)
      if (names1[index] != names2[index])
         return false;
   return true;
}
//---------------------------------------------------------------------------
/// Check if an enum value is valid
template <typename E, E V>
consteval bool isValid() { return !n<E, V>().empty(); }
/// Check if an enum value is the first one
template <typename E, E V>
consteval bool isFirst() { return std::underlying_type_t<E>(V) == 0; }
/// Check if an enum value is the last one
template <typename E, E V>
consteval bool isLast() {
   if constexpr (std::is_same_v<std::underlying_type_t<E>, bool> && static_cast<bool>(V)) {
      return true;
   } else {
      return !isValid<E, static_cast<E>(std::underlying_type_t<E>(V) + 1)>();
   }
}
//---------------------------------------------------------------------------
/// Get the last valid enum
template <typename E, E V>
consteval E getLastImpl() {
   if constexpr (isLast<E, V>()) {
      return V;
   } else {
      return getLastImpl<E, static_cast<E>(static_cast<std::underlying_type_t<E>>(V) + 1)>();
   }
}
//---------------------------------------------------------------------------
/// Get the last valid enum
template <typename E>
consteval E getLast() { return getLastImpl<E, static_cast<E>(0)>(); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
/// Base definitions of enums
template <typename E>
struct EnumInfoBase {
   static_assert(std::is_enum_v<E>);

   /// The first valid enum value
   static constexpr E min = static_cast<E>(0);
   static_assert(enumdetails::isFirst<E, min>());

   /// The last valid enum value
   static constexpr E max = enumdetails::getLast<E>();
   static_assert(enumdetails::isLast<E, max>());

   /// The enum names
   static constexpr unsigned numberOfValues = static_cast<unsigned>(static_cast<std::underlying_type_t<E>>(max) - static_cast<std::underlying_type_t<E>>(min)) + 1;
   static constexpr unsigned descriptorLength = enumdetails::getNameDescriptorLength<E, numberOfValues>();
   static constexpr std::array<char, descriptorLength> nameDescriptor = enumdetails::getNameDescriptor<E, numberOfValues, descriptorLength>();
   static constexpr std::array<unsigned, numberOfValues> nameDescriptorEnds = enumdetails::getNameDescriptorEnds<E, numberOfValues>();

   /// Get the name
   static constexpr std::string_view getName(E v) {
      unsigned slot = static_cast<unsigned>(v), start = slot ? nameDescriptorEnds[slot - 1] : 0, end = nameDescriptorEnds[slot];
      return std::string_view(nameDescriptor.data() + start, end - start);
   }

   /// Conversion
   static constexpr E convert(E v) { return v; }
};
//---------------------------------------------------------------------------
/// A range for an enum. We only support dense enums here
template <class E>
struct EnumInfo : public EnumInfoBase<E> {
   /// Should have min, max, names
};
//---------------------------------------------------------------------------
/// Definition of enums that extend another enum value
template <typename E, typename Base>
struct EnumInfoExtending : public EnumInfoBase<E> {
   static_assert(static_cast<std::underlying_type_t<Base>>(EnumInfo<Base>::min) == static_cast<std::underlying_type_t<E>>(EnumInfoBase<E>::min));
   static_assert(static_cast<std::underlying_type_t<Base>>(EnumInfo<Base>::max) < static_cast<std::underlying_type_t<E>>(EnumInfoBase<E>::max));
   static_assert(enumdetails::hasSameNamePrefix(EnumInfo<Base>::nameDescriptor.data(), EnumInfo<Base>::nameDescriptorEnds, EnumInfoBase<E>::nameDescriptor.data(), EnumInfoBase<E>::nameDescriptorEnds));

   /// Conversion
   static constexpr E convert(E v) { return v; }
   static constexpr E convert(Base v) { return static_cast<E>(v); }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
