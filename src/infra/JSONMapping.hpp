#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Enum.hpp"
#include "src/infra/FiniteDouble.hpp"
#include "src/infra/JSONReader.hpp"
#include "src/infra/JSONWriter.hpp"
#include "src/infra/ParentManaging.hpp"
#include "src/infra/SmallVector.hpp"
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
using namespace std::literals;
//---------------------------------------------------------------------------
template <class T>
struct EnumInfo;
//---------------------------------------------------------------------------
namespace json {
//---------------------------------------------------------------------------
/// The generic input/output logical. Can be overwritten for all necessary types
template <class T>
struct IO {
   public:
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value);
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value);
};
//---------------------------------------------------------------------------
/// The context when mapping structs
struct Context {
   /// The reader object
   infra::JSONReader* reader;
   /// The reader value
   infra::JSONObjectRef* readerValue;
   /// The writer object
   infra::JSONObjectScope* writer;

   /// Constructor
   explicit Context(infra::JSONReader& reader, infra::JSONObjectRef& readerValue) : reader(&reader), readerValue(&readerValue), writer(nullptr) {}
   /// Constructor
   explicit Context(infra::JSONObjectScope& writer) : reader(nullptr), readerValue(nullptr), writer(&writer) {}

   /// Reading?
   bool isReading() const { return reader; }
};
//---------------------------------------------------------------------------
/// A generic I/O interface
template <class T>
struct IOLogic {
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, void* value) { IO<T>::input(reader, in, *static_cast<T*>(value)); }
   /// Write the value
   static void output(infra::JSONWriter& out, void* value) { IO<T>::output(out, *static_cast<T*>(value)); }
};
//---------------------------------------------------------------------------
/// Check for default values
template <bool v>
struct hasDefaultValue {
   template <class T>
   constexpr static bool check(const T&, const T&) { return false; }
};
template <>
struct hasDefaultValue<true> {
   template <class T>
   constexpr static bool check(T v, T defaultValue) { return v == defaultValue; }
};
//---------------------------------------------------------------------------
/// Base class for StructMapper
struct StructMapperBase {
   private:
   /// Requirement levels
   enum Requirement : unsigned { Required,
                                 Optional,
                                 Default };

   /// Handle a struct entry
   static void handleEntry(Context& context, std::string_view name, void* value, void (*input)(infra::JSONReader&, infra::JSONValue, void*), void (*output)(infra::JSONWriter&, void*), Requirement requirement);

   friend struct OperatorMapperBase;

   protected:
   /// Handle a required entry
   template <class T>
   static void mapRequiredImpl(Context& context, std::string_view name, T& value) { handleEntry(context, name, &value, &IOLogic<T>::input, &IOLogic<T>::output, Required); }
   /// Handle a required entry
   template <class T>
   static void mapRequired(Context& context, std::string_view name, T& value) { handleEntry(context, name, &value, &IOLogic<T>::input, &IOLogic<T>::output, Required); }
   /// Handle a required FiniteDouble entry
   static void mapRequired(Context& context, std::string_view name, FiniteDouble& value) {
      double v = value;
      mapRequired<double>(context, name, v);
      value = FiniteDouble(v);
   }
   /// Handle a required entry
   template <ParentManaging T>
   static void mapRequired(Context& context, std::string_view name, ParentPtrVector<T>& value) = delete;
   /// Handle an optional entry
   template <class T>
   static void mapOptionalImpl(Context& context, std::string_view name, T& value, T defaultValue = T{}) {
      auto req = (!context.isReading()) && hasDefaultValue<std::is_integral<T>::value>::check(value, defaultValue) ? Default : Optional;
      if (context.isReading()) value = std::move(defaultValue);
      handleEntry(context, name, &value, &IOLogic<T>::input, &IOLogic<T>::output, req);
   }
   /// Handle an optional entry
   template <class T>
   static void mapOptional(Context& context, std::string_view name, T& value, T defaultValue = T{}) {
      auto req = (!context.isReading()) && hasDefaultValue<std::is_integral<T>::value>::check(value, defaultValue) ? Default : Optional;
      if (context.isReading()) value = std::move(defaultValue);
      handleEntry(context, name, &value, &IOLogic<T>::input, &IOLogic<T>::output, req);
   }
   /// Handle an optional FiniteDouble entry
   static void mapOptional(Context& context, std::string_view name, FiniteDouble& value, FiniteDouble defaultValue = {}) {
      double v = value;
      mapOptional<double>(context, name, v, defaultValue);
      value = FiniteDouble(v);
   }
   /// Handle an optional entry
   template <class T>
   static void mapOptional(Context& context, std::string_view name, ParentPtrVector<T>& value, T defaultValue = T{}) = delete;
   /// Handle an optional entry
   template <class T>
   static void mapOptional(Context& context, std::string_view name, std::optional<T>& value) {
      if (context.isReading()) {
         if (context.readerValue->getEntry(name, false).isValid()) {
            value = T{};
            handleEntry(context, name, &value.value(), &IOLogic<T>::input, &IOLogic<T>::output, Optional);
         } else
            value.reset();
      } else if (value.has_value()) {
         handleEntry(context, name, &value.value(), &IOLogic<T>::input, &IOLogic<T>::output, Optional);
      }
   }
   /// Handle an optional entry
   template <class T>
   static void mapOptional(Context& context, std::string_view name, std::string& value) {
      if (context.isReading()) value.clear();
      handleEntry(context, name, &value, &IOLogic<T>::input, &IOLogic<T>::output, value.empty() ? Default : Optional);
   }
};
//---------------------------------------------------------------------------
/// Helper to handle struct. Does the I/O, the elements only have to be enumerated
template <class T>
struct StructMapper : public StructMapperBase {
   using structType = T;

   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value) {
      auto o = in.getObject();
      Context c(reader, o);
      IO<T>::enumEntries(c, value);
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      auto o = out.writeObject();
      Context c(o);
      IO<T>::enumEntries(c, const_cast<T&>(value));
   }
};
//---------------------------------------------------------------------------
/// Helper for array-like containers
template <class T>
struct ArrayMapper {
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value) {
      auto a = in.getArray();
      value.resize(a.size());
      unsigned index = 0;
      for (auto v : a)
         IO<typename std::remove_const<typename std::remove_reference<decltype(value[0])>::type>::type>::input(reader, v, value[index++]);
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      auto a = out.writeArray();
      for (auto& v : value) {
         a.genericEntry();
         IO<typename std::remove_const<typename std::remove_reference<decltype(value[0])>::type>::type>::output(out, v);
      }
   }
};
//---------------------------------------------------------------------------
/// Helper for fixed-size array-like containers
template <class T>
struct FixedArrayMapper {
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value) {
      auto a = in.getArray();
      if (value.size() != a.size()) JSONReader::throwJSONException("array size mismatch"sv);
      unsigned index = 0;
      for (auto v : a)
         IO<typename std::remove_const<typename std::remove_reference<decltype(value[0])>::type>::type>::input(reader, v, value[index++]);
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      auto a = out.writeArray();
      for (auto& v : value) {
         a.genericEntry();
         IO<typename std::remove_const<typename std::remove_reference<decltype(value[0])>::type>::type>::output(out, v);
      }
   }
};
//---------------------------------------------------------------------------
/// Helper for set-like containers
template <class T>
struct SetMapper {
   using keyType = T::key_type;

   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value) {
      auto a = in.getArray();
      for (auto v : a) {
         keyType t;
         IO<keyType>::input(reader, v, t);
         value.emplace(std::move(t));
      }
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      auto a = out.writeArray();
      for (auto& v : value) {
         a.genericEntry();
         IO<keyType>::output(out, v);
      }
   }
};
//---------------------------------------------------------------------------
/// Base class for VariantMapper
template <typename Derived, typename T>
struct VariantMapperBase {
   using variantType = T;

   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, variantType& value) {
      auto obj = in.getObject();
      for (unsigned i = 0;; ++i) {
         auto name = Derived::getVariantName(i);
         if (name.empty())
            break;
         auto variantValue = obj.getEntry(name, false);
         if (variantValue.isValid()) {
            Derived::inputVariant(reader, variantValue, value, i);
            return;
         }
      }
      JSONReader::throwJSONException("unknown variant value"sv);
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const variantType& value) {
      auto obj = out.writeObject();
      unsigned variantIndex = Derived::getVariantIndex(value);
      obj.genericEntry(Derived::getVariantName(variantIndex));
      Derived::outputVariant(obj.getWriter(), value, variantIndex);
   }
};
//---------------------------------------------------------------------------
/// Helper for variant-like types
template <typename Derived, typename T>
struct VariantMapper : VariantMapperBase<Derived, T> {};
//---------------------------------------------------------------------------
/// Specialization of VariantMapper for std::variant
template <typename Derived, typename... Ts>
struct VariantMapper<Derived, std::variant<Ts...>> : VariantMapperBase<Derived, std::variant<Ts...>> {
   using variantType = std::variant<Ts...>;

   private:
   template <unsigned variantIndex>
   static void inputVariantImpl(infra::JSONReader& reader, infra::JSONValue in, variantType& value) {
      using variantIndexType = std::variant_alternative_t<variantIndex, variantType>;
      auto& variantValue = value.template emplace<variantIndex>();
      IO<variantIndexType>::input(reader, in, variantValue);
   }
   static void inputVariantDispatch(infra::JSONReader&, infra::JSONValue, variantType&, unsigned, std::integer_sequence<unsigned>) {}
   template <unsigned I, unsigned... Is>
   static void inputVariantDispatch(infra::JSONReader& reader, infra::JSONValue in, variantType& value, unsigned variantIndex, std::integer_sequence<unsigned, I, Is...>) {
      if (variantIndex == I)
         inputVariantImpl<I>(reader, in, value);
      else
         inputVariantDispatch(reader, in, value, variantIndex, std::integer_sequence<unsigned, Is...>{});
   }

   template <unsigned variantIndex>
   static void outputVariantImpl(infra::JSONWriter& out, const variantType& value) {
      using variantIndexType = std::variant_alternative_t<variantIndex, variantType>;
      auto& variantValue = std::get<variantIndex>(value);
      IO<variantIndexType>::output(out, variantValue);
   }
   static void outputVariantDispatch(infra::JSONWriter&, const variantType&, unsigned, std::integer_sequence<unsigned>) {}
   template <unsigned I, unsigned... Is>
   static void outputVariantDispatch(infra::JSONWriter& out, const variantType& value, unsigned variantIndex, std::integer_sequence<unsigned, I, Is...>) {
      if (variantIndex == I)
         outputVariantImpl<I>(out, value);
      else
         outputVariantDispatch(out, value, variantIndex, std::integer_sequence<unsigned, Is...>{});
   }

   public:
   /// Get the index of a variant value
   static unsigned getVariantIndex(const variantType& value) { return value.index(); }
   /// Read a variant value
   static void inputVariant(infra::JSONReader& reader, infra::JSONValue in, variantType& value, unsigned variantIndex) {
      inputVariantDispatch(reader, in, value, variantIndex, std::make_integer_sequence<unsigned, std::variant_size_v<variantType>>());
   }
   /// Write a variant value
   static void outputVariant(infra::JSONWriter& out, const variantType& value, unsigned variantIndex) {
      outputVariantDispatch(out, value, variantIndex, std::make_integer_sequence<unsigned, std::variant_size_v<variantType>>());
   }
};
//---------------------------------------------------------------------------
/// Base class for EnumMapper
struct EnumMapperBase {
   /// A lookup table
   struct Lookup {
      /// The implementation
      struct Impl;
      /// The implementation
      std::unique_ptr<Impl> impl;

      /// Constructor
      explicit Lookup(const char* descriptor, const unsigned* ends, unsigned nameCount);
      /// Destructor
      ~Lookup();

      /// Try to resolve a name
      std::optional<unsigned> tryResolve(std::string_view value) const;
      /// Resolve a name
      unsigned resolve(std::string_view value) const;
   };
};
//---------------------------------------------------------------------------
/// Helper for enums
template <class T>
struct EnumMapper : EnumMapperBase {
   using enumType = T;

   /// Get the lookup table
   static const Lookup& getLookupTable() {
      static Lookup lookup(EnumInfo<T>::nameDescriptor.data(), EnumInfo<T>::nameDescriptorEnds.data(), EnumInfo<T>::nameDescriptorEnds.size());
      return lookup;
   }
   /// Lookup a name
   static T lookupName(std::string_view value) {
      return static_cast<T>(getLookupTable().resolve(value));
   }
   /// Try to lookup a name
   static std::optional<T> tryLookupName(std::string_view value) {
      auto res = getLookupTable().tryResolve(value);
      return res ? std::optional(static_cast<T>(*res)) : std::nullopt;
   }
   /// Read the value
   static void input(infra::JSONReader& /*reader*/, infra::JSONValue in, T& value) {
      value = lookupName(in.getString());
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      out.writeString(EnumInfo<T>::getName(value));
   }
};
//---------------------------------------------------------------------------
/// Supported data types
template <>
struct IO<bool> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, bool& value);
   static void output(infra::JSONWriter& out, bool value);
};
template <>
struct IO<char> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, char& value);
   static void output(infra::JSONWriter& out, char value);
};
template <>
struct IO<unsigned char> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, unsigned char& value);
   static void output(infra::JSONWriter& out, unsigned char value);
};
template <>
struct IO<int8_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, int8_t& value);
   static void output(infra::JSONWriter& out, int8_t value);
};
template <>
struct IO<int16_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, int16_t& value);
   static void output(infra::JSONWriter& out, int16_t value);
};
template <>
struct IO<uint16_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, uint16_t& value);
   static void output(infra::JSONWriter& out, uint16_t value);
};
template <>
struct IO<int32_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, int32_t& value);
   static void output(infra::JSONWriter& out, int32_t value);
};
template <>
struct IO<uint32_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, uint32_t& value);
   static void output(infra::JSONWriter& out, uint32_t value);
};
template <>
struct IO<int64_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, int64_t& value);
   static void output(infra::JSONWriter& out, int64_t value);
};
template <>
struct IO<uint64_t> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, uint64_t& value);
   static void output(infra::JSONWriter& out, uint64_t value);
};
template <>
struct IO<double> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, double& value);
   static void output(infra::JSONWriter& out, double value);
};
template <>
struct IO<std::string> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, std::string& value);
   static void output(infra::JSONWriter& out, const std::string& value);
};
template <class T>
struct IO<std::vector<T>> : public ArrayMapper<std::vector<T>> {};
template <class T, unsigned N, typename T2>
struct IO<infra::SmallVector<T, N, T2>> : public ArrayMapper<infra::SmallVector<T, N, T2>> {};
template <class T>
struct IO<std::set<T>> : public SetMapper<std::set<T>> {};
template <class T, std::size_t N>
struct IO<std::array<T, N>> : public FixedArrayMapper<std::array<T, N>> {};
//---------------------------------------------------------------------------
/// Helper for vector<bool>, since we need to explicitly handle std::vector<bool>::reference
struct VectorBoolMapper {
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, std::vector<bool>& value);
   /// Write the value
   static void output(infra::JSONWriter& out, const std::vector<bool>& value);
};
template <>
struct IO<std::vector<bool>> : public VectorBoolMapper {};
//---------------------------------------------------------------------------
/// Helper for map-like containers
template <class T>
struct MapMapper {
   using K = T::key_type;
   using V = T::mapped_type;
   using ElementType = std::pair<K, V>;
   /// Read the value
   static void input(infra::JSONReader& reader, infra::JSONValue in, T& value) {
      auto a = in.getArray();
      for (auto v : a) {
         ElementType current;
         auto vFirst = v.getObject().getEntry("k");
         IO<K>::input(reader, vFirst, current.first);
         auto vSecond = v.getObject().getEntry("v");
         IO<V>::input(reader, vSecond, current.second);
         value.insert(std::move(current));
      }
   }
   /// Write the value
   static void output(infra::JSONWriter& out, const T& value) {
      auto a = out.writeArray();
      for (auto& v : value) {
         a.genericEntry();
         auto o = out.writeObject();
         o.genericEntry("k");
         IO<K>::output(out, v.first);
         o.genericEntry("v");
         IO<V>::output(out, v.second);
      }
   }
};
//---------------------------------------------------------------------------
template <class T>
struct IO<std::unordered_set<T>> : public SetMapper<std::unordered_set<T>> {};
//---------------------------------------------------------------------------
template <class T1, class T2>
struct IO<std::unordered_map<T1, T2>> : public MapMapper<std::unordered_map<T1, T2>> {};
//---------------------------------------------------------------------------
}
}
//---------------------------------------------------------------------------
