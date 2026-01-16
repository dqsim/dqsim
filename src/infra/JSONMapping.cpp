#include "src/infra/JSONMapping.hpp"
#include "thirdparty/hopscotch-map/hopscotch_map.h"
#include <format>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace dqsim::infra { namespace json {
//---------------------------------------------------------------------------
void StructMapperBase::handleEntry(Context& context, std::string_view name, void* value, void (*input)(infra::JSONReader&, infra::JSONValue, void*), void (*output)(infra::JSONWriter&, void*), Requirement requirement)
// Handle a struct entry
{
   if (context.isReading()) {
      auto v = context.readerValue->getEntry(name, requirement == Requirement::Required);
      if (v.isValid())
         input(*context.reader, v, value);
   } else {
      // Do not output default values
      if (requirement == Requirement::Default)
         return;
      context.writer->genericEntry(name);
      output(context.writer->getWriter(), value);
   }
}
//---------------------------------------------------------------------------
struct EnumMapperBase::Lookup::Impl {
   /// The values
   tsl::hopscotch_map<string_view, unsigned> values;
};
//---------------------------------------------------------------------------
EnumMapperBase::Lookup::Lookup(const char* descriptor, const unsigned* ends, unsigned count)
   : impl(make_unique<Impl>())
// Constructor
{
   impl->values.reserve(count);
   for (unsigned index = 0; index != count; ++index) {
      unsigned start = (index ? ends[index - 1] : 0), end = ends[index];
      string_view name(descriptor + start, end - start);
      impl->values[name] = index;
   }
}
//---------------------------------------------------------------------------
EnumMapperBase::Lookup::~Lookup()
// Destructor
{
}
//---------------------------------------------------------------------------
optional<unsigned> EnumMapperBase::Lookup::tryResolve(string_view value) const
// Try to resolve a name
{
   auto iter = impl->values.find(value);
   return (iter == impl->values.end()) ? nullopt : optional(iter->second);
}
//---------------------------------------------------------------------------
unsigned EnumMapperBase::Lookup::resolve(string_view value) const
// Resolve a name
{
   auto res = tryResolve(value);
   if (!res)
      throw std::runtime_error(std::format("unkown enum value {}", value));
   return *res;
}
//---------------------------------------------------------------------------
void IO<char>::input(infra::JSONReader&, infra::JSONValue in, char& value) {
   auto s = in.getString();
   value = (s.length() == 1) ? s[0] : 0;
}
void IO<char>::output(infra::JSONWriter& out, char value) {
   char tmp[2] = {value, 0};
   out.writeString(tmp);
}
//---------------------------------------------------------------------------
void IO<bool>::input(infra::JSONReader&, infra::JSONValue in, bool& value) { value = in.getBool(); }
void IO<bool>::output(infra::JSONWriter& out, bool value) { out.writeBool(value); }
//---------------------------------------------------------------------------
void IO<unsigned char>::input(infra::JSONReader&, infra::JSONValue in, unsigned char& value) { value = in.getInt(); }
void IO<unsigned char>::output(infra::JSONWriter& out, unsigned char value) { out.writeInt(value); }
//---------------------------------------------------------------------------
void IO<int8_t>::input(infra::JSONReader&, infra::JSONValue in, int8_t& value) { value = in.getInt(); }
void IO<int8_t>::output(infra::JSONWriter& out, int8_t value) { out.writeInt(value); }
//---------------------------------------------------------------------------
void IO<int16_t>::input(infra::JSONReader&, infra::JSONValue in, int16_t& value) { value = in.getInt(); }
void IO<int16_t>::output(infra::JSONWriter& out, int16_t value) { out.writeInt(value); }
//---------------------------------------------------------------------------
void IO<uint16_t>::input(infra::JSONReader&, infra::JSONValue in, uint16_t& value) { value = in.getUInt(); }
void IO<uint16_t>::output(infra::JSONWriter& out, uint16_t value) { out.writeUInt(value); }
//---------------------------------------------------------------------------
void IO<int32_t>::input(infra::JSONReader&, infra::JSONValue in, int32_t& value) { value = in.getInt(); }
void IO<int32_t>::output(infra::JSONWriter& out, int32_t value) { out.writeInt(value); }
//---------------------------------------------------------------------------
void IO<uint32_t>::input(infra::JSONReader&, infra::JSONValue in, uint32_t& value) { value = in.getUInt(); }
void IO<uint32_t>::output(infra::JSONWriter& out, uint32_t value) { out.writeUInt(value); }
//---------------------------------------------------------------------------
void IO<int64_t>::input(infra::JSONReader&, infra::JSONValue in, int64_t& value) { value = in.getInt(); }
void IO<int64_t>::output(infra::JSONWriter& out, int64_t value) { out.writeInt(value); }
//---------------------------------------------------------------------------
void IO<uint64_t>::input(infra::JSONReader&, infra::JSONValue in, uint64_t& value) { value = in.getUInt(); }
void IO<uint64_t>::output(infra::JSONWriter& out, uint64_t value) { out.writeUInt(value); }
//---------------------------------------------------------------------------
void IO<double>::input(infra::JSONReader&, infra::JSONValue in, double& value) { value = in.getDouble(); }
void IO<double>::output(infra::JSONWriter& out, double value) { out.writeDouble(value); }
//---------------------------------------------------------------------------
void IO<string>::input(infra::JSONReader&, infra::JSONValue in, string& value) { value = in.getString(); }
void IO<string>::output(infra::JSONWriter& out, const string& value) { out.writeString(value); }
//---------------------------------------------------------------------------
void VectorBoolMapper::input(infra::JSONReader& reader, infra::JSONValue in, std::vector<bool>& value) {
   auto a = in.getArray();
   value.resize(a.size());
   unsigned index = 0;
   for (auto v : a) {
      bool res;
      IO<bool>::input(reader, v, res);
      value[index++] = res;
   }
}
//---------------------------------------------------------------------------
void VectorBoolMapper::output(infra::JSONWriter& out, const std::vector<bool>& value) {
   auto a = out.writeArray();
   for (auto v : value) {
      a.genericEntry();
      IO<bool>::output(out, v);
   }
}
//---------------------------------------------------------------------------
}}
//---------------------------------------------------------------------------
