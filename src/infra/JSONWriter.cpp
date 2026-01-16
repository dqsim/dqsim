#include "src/infra/JSONWriter.hpp"
#include <cmath>
#include <ostream>
#include <sstream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
static void doIndent(std::ostream& out, unsigned level)
// Indent as needed
{
   auto spaces = "                    "sv;
   while (level >= 20) {
      out << spaces;
      level -= 20;
   }
   if (level)
      out << spaces.substr(20 - level);
}
//---------------------------------------------------------------------------
JSONObjectScope::JSONObjectScope(JSONWriter& out, bool indent)
   : out(out), first(true), indent(indent)
// Constructor
{
   out.out << '{';
   if (indent)
      out.level++;
}
//---------------------------------------------------------------------------
JSONObjectScope::~JSONObjectScope() {
   if (indent) {
      out.level--;
      out.out << '\n';
      doIndent(out.out, out.level);
   }
   out.out << '}';
}
//---------------------------------------------------------------------------
void JSONObjectScope::genericEntry(string_view entry)
// A generic entry, the content must be provided by a call to JSONWriter
{
   if (indent) {
      if (first)
         first = false;
      else
         out.out << ',';
      out.out << '\n';
      doIndent(out.out, out.level);
   } else {
      if (first)
         first = false;
      else
         out.out << ", "sv;
   }

   out.writeString(entry);
   out.out << ':';
}
//---------------------------------------------------------------------------
void JSONObjectScope::nullEntry(string_view entry)
// A null entry
{
   genericEntry(entry);
   out.writeNull();
}
//---------------------------------------------------------------------------
void JSONObjectScope::boolEntry(string_view entry, bool value)
// A boolean entry
{
   genericEntry(entry);
   out.writeBool(value);
}
//---------------------------------------------------------------------------
void JSONObjectScope::doubleEntry(string_view entry, double value)
// A number entry
{
   genericEntry(entry);
   out.writeDouble(value);
}
//---------------------------------------------------------------------------
void JSONObjectScope::intEntry(string_view entry, int64_t value)
// A number entry
{
   genericEntry(entry);
   out.writeInt(value);
}
//---------------------------------------------------------------------------
void JSONObjectScope::uintEntry(string_view entry, uint64_t value)
// A number entry
{
   genericEntry(entry);
   out.writeUInt(value);
}
//---------------------------------------------------------------------------
void JSONObjectScope::stringEntry(string_view entry, string_view value)
// A string entry
{
   genericEntry(entry);
   out.writeString(value);
}
//---------------------------------------------------------------------------
JSONObjectScope JSONObjectScope::objectEntry(string_view entry, bool indent)
// An object entry
{
   genericEntry(entry);
   return out.writeObject(indent);
}
//---------------------------------------------------------------------------
JSONArrayScope JSONObjectScope::arrayEntry(string_view entry)
// An array entry
{
   genericEntry(entry);
   return out.writeArray();
}
//---------------------------------------------------------------------------
JSONArrayScope::JSONArrayScope(JSONWriter& out)
   : out(out), first(true)
// Constructor
{
   out.out << "["sv;
}
//---------------------------------------------------------------------------
JSONArrayScope::~JSONArrayScope() {
   out.out << "]"sv;
}
//---------------------------------------------------------------------------
void JSONArrayScope::genericEntry()
// A generic entry, the content must be provided by a call to JSONWriter
{
   if (first)
      first = false;
   else
      out.out << ", "sv;
}
//---------------------------------------------------------------------------
void JSONArrayScope::nullEntry()
// A null entry
{
   genericEntry();
   out.writeNull();
}
//---------------------------------------------------------------------------
void JSONArrayScope::boolEntry(bool value)
// A boolean entry
{
   genericEntry();
   out.writeBool(value);
}
//---------------------------------------------------------------------------
void JSONArrayScope::doubleEntry(double value)
// A number entry
{
   genericEntry();
   out.writeDouble(value);
}
//---------------------------------------------------------------------------
void JSONArrayScope::intEntry(int64_t value)
// A number entry
{
   genericEntry();
   out.writeInt(value);
}
//---------------------------------------------------------------------------
void JSONArrayScope::uintEntry(uint64_t value)
// A number entry
{
   genericEntry();
   out.writeUInt(value);
}
//---------------------------------------------------------------------------
void JSONArrayScope::stringEntry(string_view value)
// A string entry
{
   genericEntry();
   out.writeString(value);
}
//---------------------------------------------------------------------------
JSONObjectScope JSONArrayScope::objectEntry(bool indent)
// An object entry
{
   genericEntry();
   return out.writeObject(indent);
}
//---------------------------------------------------------------------------
JSONArrayScope JSONArrayScope::arrayEntry()
// An array entry
{
   genericEntry();
   return out.writeArray();
}
//---------------------------------------------------------------------------
JSONWriter::JSONWriter(std::ostream& out)
   : out(out), level(0)
// Constructor
{
}
//---------------------------------------------------------------------------
void JSONWriter::writeNull()
// Write a null entry
{
   out << "null"sv;
}
//---------------------------------------------------------------------------
void JSONWriter::writeBool(bool value)
// Write a boolean
{
   if (value)
      out << "true"sv;
   else
      out << "false"sv;
}
//---------------------------------------------------------------------------
void JSONWriter::writeDouble(double value)
// Write a number
{
   if (!isfinite(value)) value = 0;

   // Check for round trip
   char buffer[100];
   snprintf(buffer, sizeof(buffer), "%g", value);
   buffer[sizeof(buffer) - 1] = 0;
   if (atof(buffer) == value) {
      out << buffer;
      return;
   }

   // Increase precision as needed
   for (unsigned precision = 6; precision <= 18; precision += 3) {
      snprintf(buffer, sizeof(buffer), "%.*g", precision, value);
      buffer[sizeof(buffer) - 1] = 0;
      if (atof(buffer) == value) break;
   }
   out << buffer;
}
//---------------------------------------------------------------------------
void JSONWriter::writeInt(int64_t value)
// Write a number
{
   out << value;
}
//---------------------------------------------------------------------------
void JSONWriter::writeUInt(uint64_t value)
// Write a number
{
   out << value;
}
//---------------------------------------------------------------------------
void JSONWriter::escapeString(std::ostream& out, string_view value)
// Write a string
{
   out << '"';
   for (char c : value) {
      // We must escape everything below space
      if ((c & 0xFF) < ' ') {
         switch (c) {
            case '\b': out << "\\b"sv; break;
            case '\f': out << "\\f"sv; break;
            case '\n': out << "\\n"sv; break;
            case '\r': out << "\\r"sv; break;
            case '\t': out << "\\t"sv; break;
            default:
               static constexpr string_view hex = "0123456789ABCDEF"sv;
               out << "\\u00"sv << hex[(c & 0xF0) >> 4] << hex[c & 0x0F];
               break;
         }
      } else {
         // Of the rest we must escape quotes and backslash
         switch (c) {
            case '"': out << "\\\""sv; break;
            case '\\': out << "\\\\"sv; break;
            default: out << c; break;
         }
      }
   }
   out << '"';
}
//---------------------------------------------------------------------------
void JSONWriter::writeString(string_view value)
// Write a string
{
   escapeString(out, value);
}
//---------------------------------------------------------------------------
JSONObjectScope JSONWriter::writeObject(bool indent)
// Write an object
{
   return JSONObjectScope(*this, indent);
}
//---------------------------------------------------------------------------
JSONArrayScope JSONWriter::writeArray()
// Write an array
{
   return JSONArrayScope(*this);
}
//---------------------------------------------------------------------------
string JSONWriter::writeToString(double value)
// Write to a string
{
   auto sstream = std::stringstream();
   auto writer = JSONWriter(sstream);
   writer.writeDouble(value);
   return sstream.str();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
