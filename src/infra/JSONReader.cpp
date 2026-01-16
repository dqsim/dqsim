#include "src/infra/JSONReader.hpp"
#include <format>
#include <istream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Small helper to simplify whitespace handling
class Reader {
   /// The real input
   std::istream& in;
   /// The last character
   char putback;

   public:
   /// Constructor
   explicit Reader(std::istream& in) : in(in), putback(0) {}

   /// Get the next character
   bool next(char& c);
   /// Put a character back
   void unget(char c) { putback = c; }
   /// Skip all whitespace
   void skipWS();
};
//---------------------------------------------------------------------------
bool Reader::next(char& c)
// Get the next character
{
   if (putback) {
      c = putback;
      putback = 0;
      return true;
   }

   c = in.get();
   return !!in;
}
//---------------------------------------------------------------------------
void Reader::skipWS()
// Skip all whitespace
{
   char c;
   while (next(c)) {
      if ((c == ' ') || (c == '\t') || (c == '\n') || (c == '\r')) continue;
      if (c == '#') {
         // We recognize comments as an extension
         while (next(c)) {
            if ((c == '\n') || (c == '\r'))
               break;
         }
         continue;
      }
      unget(c);
      return;
   }
}
//---------------------------------------------------------------------------
static bool checkTail(Reader& in, string_view tail, string& result)
// Check the tail of a literal
{
   for (char t : tail) {
      char c;
      if (!in.next(c)) return false;
      if (c != t)
         return false;
      result += c;
   }
   return true;
}
//---------------------------------------------------------------------------
static bool parseHex(Reader& in, unsigned& result)
// Parse 4 hex digits
{
   result = 0;
   for (unsigned index = 0; index != 4; ++index) {
      char c;
      if (!in.next(c)) return false;
      unsigned n;
      if ((c >= '0') && (c <= '9')) {
         n = c - '0';
      } else if ((c >= 'A') && (c <= 'F')) {
         n = c - 'A' + 10;
      } else if ((c >= 'a') && (c <= 'f')) {
         n = c - 'a' + 10;
      } else
         return false;
      result = (result << 4) | n;
   }
   return true;
}
//---------------------------------------------------------------------------
static bool parseString(Reader& in, string& result)
// Parse a string. The '"' has already been consumed
{
   result.clear();

   // Parse a string
   char c;
   while (true) {
      if (!in.next(c)) return {};
      if (c == '"') break;
      if (c == '\\') {
         if (!in.next(c)) return false;
         switch (c) {
            case 'b': result += '\b'; break;
            case 'f': result += '\f'; break;
            case 'n': result += '\n'; break;
            case 'r': result += '\r'; break;
            case 't': result += '\t'; break;
            case '"':
            case '\\':
            case '/': result += c; break;
            case 'u': {
               assert(false); // We don't have the implementation for this
               unsigned hex;
               if (!parseHex(in, hex)) return {};
               // Detect surrogate pairs
               if ((hex >= 0xD800) && (hex <= 0xD8FF)) {
                  unsigned hex2;
                  if ((!in.next(c)) || (c != '\\')) return false;
                  if ((!in.next(c)) || (c != 'u')) return false;
                  if (!parseHex(in, hex2)) return {};
                  if ((hex2 >= 0xDC00) && (hex2 <= 0xDFFF))
                     hex = (hex << 10) + hex2 - 0x35FDC00;
                  else
                     return {};
               }
               // char buffer[10];
               // char* writer = Utf8::writeCodePoint(buffer, buffer + sizeof(buffer), hex);
               // for (auto iter = buffer; iter != writer; ++iter)
               //    result += *iter;
               break;
            }
            default: return false;
         }
      } else
         result += c;
   }
   return true;
}
//---------------------------------------------------------------------------
unique_ptr<JSONNode> parseNode(Reader& in)
// Parse a node
{
   in.skipWS();
   char c;
   if (!in.next(c))
      return {};

   auto result = make_unique<JSONNode>();
   result->value += c;
   switch (c) {
      case 'n':
         if (!checkTail(in, "ull"sv, result->value)) return {};
         result->type = JSONType::Null;
         return result;
      case 't':
         if (!checkTail(in, "rue"sv, result->value)) return {};
         result->type = JSONType::Bool;
         return result;
      case 'f':
         if (!checkTail(in, "alse"sv, result->value)) return {};
         result->type = JSONType::Bool;
         return result;
      case '"':
         result->value.clear();
         if (!parseString(in, result->value))
            return {};
         result->type = JSONType::String;
         return result;
      case '-':
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9': {
         // Parse a number
         result->type = JSONType::Number;
         if (c == '-') {
            if ((!in.next(c)) || (c < '0') || (c > '9')) return {};
            result->value += c;
         }
         while (true) {
            if (!in.next(c))
               return result;
            if ((c >= '0') && (c <= '9')) {
               result->value += c;
               continue;
            }
            if ((c == '.') || (c == 'e')) break;
            in.unget(c);
            return result;
         }
         if (c == '.') {
            result->value += c;
            if ((!in.next(c)) || (c < '0') || (c > '9')) return {};
            result->value += c;
            while (true) {
               if (!in.next(c))
                  return result;
               if ((c >= '0') && (c <= '9')) {
                  result->value += c;
                  continue;
               }
               if (c == 'e') break;
               in.unget(c);
               return result;
            }
         }
         result->value += c;
         if (!in.next(c)) return {};
         if ((c == '+') || (c == '-')) {
            result->value += c;
            if (!in.next(c)) return {};
         }
         if ((c < '0') || (c > '9')) return {};
         result->value += c;
         while (true) {
            if (!in.next(c))
               return result;
            if ((c >= '0') && (c <= '9')) {
               result->value += c;
               continue;
            }
            in.unget(c);
            return result;
         }
      }
      case '{': {
         // Parse an object
         result->type = JSONType::Object;
         in.skipWS();
         if (!in.next(c)) return {};
         if (c == '}') return result;
         in.unget(c);

         string key;
         JSONNode* last = nullptr;
         while (true) {
            in.skipWS();
            if ((!in.next(c)) || (c != '"')) return {};
            if (!parseString(in, key)) return {};
            in.skipWS();
            if ((!in.next(c)) || (c != ':')) return {};
            auto value = parseNode(in);
            if (!value) return {};
            value->key = move(key);
            if (last) {
               last->next = move(value);
               last = last->next.get();
            } else {
               result->child = move(value);
               last = result->child.get();
            }
            in.skipWS();
            if (!in.next(c)) return {};
            if (c == '}') break;
            if (c != ',') return {};
         }
         return result;
      }
      case '[': {
         // Parse an array
         result->type = JSONType::Array;
         in.skipWS();
         if (!in.next(c)) return {};
         if (c == ']') return result;
         in.unget(c);

         JSONNode* last = nullptr;
         while (true) {
            auto value = parseNode(in);
            if (!value) return {};
            if (last) {
               last->next = move(value);
               last = last->next.get();
            } else {
               result->child = move(value);
               last = result->child.get();
            }
            in.skipWS();
            if (!in.next(c)) return {};
            if (c == ']') break;
            if (c != ',') return {};
         }
         return result;
      }
      default: return {};
   }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
[[noreturn]] void JSONReader::throwJSONException(string_view message)
// Throw an exception when traversing the JSON document
{
   throw std::runtime_error(std::string(message));
}
//---------------------------------------------------------------------------
JSONNode::~JSONNode()
// Destructor
{
   // Non-trivial delete?
   if (next || child) {
      // Tree release based upon rotation. Idea from http://keithschwarz.com/interesting/code/?dir=inplace-tree-delete

      // The real release function. Operates on non-owing pointers, thus we call it for both unique_ptrs
      auto releaseTree = [](JSONNode* root) {
         while (root) {
            // Rotate the tree until we have no left child
            if (root->child) {
               auto left = root->child.release();
               root->child = move(left->next);
               left->next.reset(root);
               root = left;
            } else {
               // We have only a right child, delete the current node and continue
               auto current = root;
               root = root->next.release();
               delete current;
            }
         }
      };
      releaseTree(next.release());
      releaseTree(child.release());
   }
}
//---------------------------------------------------------------------------
JSONReader::JSONReader()
// Constructor
{
}
//---------------------------------------------------------------------------
JSONReader::~JSONReader()
// Destructor
{
}
//---------------------------------------------------------------------------
bool JSONReader::parse(std::istream& in)
// Parse the JSON document
{
   Reader reader(in);
   auto newValue = parseNode(reader);
   if (!newValue)
      return false;

   // Check for trailing data
   reader.skipWS();
   char c;
   if (reader.next(c))
      return false;

   root = move(newValue);
   return true;
}
//---------------------------------------------------------------------------
JSONValue JSONReader::getRoot() const
// Access the root
{
   if (!root)
      throwJSONException("invalid document"sv);
   return JSONValue{root.get()};
}
//---------------------------------------------------------------------------
bool JSONValue::getBool() const
// Get a boolean value
{
   if (node->type != JSONType::Bool)
      JSONReader::throwJSONException("boolean expected"sv);

   return node->value[0] == 't';
}
//---------------------------------------------------------------------------
double JSONValue::getDouble() const
// Get a double value
{
   if (node->type != JSONType::Number)
      JSONReader::throwJSONException("number expected"sv);

   char* endp;
   double v = strtod(node->value.c_str(), &endp);
   if ((*endp) || (node->value.empty()))
      JSONReader::throwJSONException("invalid unsigned integer"sv); // unreachable in practice, we do not parse the number otherwise
   return v;
}
//---------------------------------------------------------------------------
int64_t JSONValue::getInt() const
// Get an integer value
{
   if (node->type != JSONType::Number)
      JSONReader::throwJSONException("number expected"sv);

   char* endp;
   unsigned long long v = strtoll(node->value.c_str(), &endp, 10);
   if ((*endp) || (node->value.empty()))
      JSONReader::throwJSONException("invalid unsigned integer"sv);
   return v;
}
//---------------------------------------------------------------------------
uint64_t JSONValue::getUInt() const
// Get an integer value
{
   if (node->type != JSONType::Number)
      JSONReader::throwJSONException("number expected"sv);

   char* endp;
   unsigned long long v = strtoull(node->value.c_str(), &endp, 10);
   if ((*endp) || (node->value.empty()))
      JSONReader::throwJSONException("invalid unsigned integer"sv);
   return v;
}
//---------------------------------------------------------------------------
const string& JSONValue::getString() const
// Get a string value
{
   if (node->type != JSONType::String)
      JSONReader::throwJSONException("string expected"sv);
   return node->value;
}
//---------------------------------------------------------------------------
JSONObjectRef JSONValue::getObject() const
// Get an object value
{
   if (node->type != JSONType::Object)
      JSONReader::throwJSONException("object expected"sv);
   return JSONObjectRef{node};
}
//---------------------------------------------------------------------------
JSONArrayRef JSONValue::getArray() const
// Get an array value
{
   if (node->type != JSONType::Array)
      JSONReader::throwJSONException("array expected"sv);
   return JSONArrayRef{node};
}
//---------------------------------------------------------------------------
JSONValue JSONObjectRef::getEntry(string_view name, bool check) const
// Get an entry
{
   for (auto iter = node->child.get(); iter; iter = iter->next.get())
      if (iter->key == name) {
         return JSONValue{iter};
      }
   if (check) {
      JSONReader::throwJSONException(std::format("unknown_entry {0}", name));
   }
   return JSONValue{nullptr};
}
//---------------------------------------------------------------------------
bool JSONObjectRef::isNull(string_view name) const
// Is null?
{
   return getEntry(name).isNull();
}
//---------------------------------------------------------------------------
bool JSONObjectRef::getBool(string_view name) const
// Get a boolean value
{
   return getEntry(name).getBool();
}
//---------------------------------------------------------------------------
double JSONObjectRef::getDouble(string_view name) const
// Get a double value
{
   return getEntry(name).getDouble();
}
//---------------------------------------------------------------------------
int64_t JSONObjectRef::getInt(string_view name) const
// Get an integer value
{
   return getEntry(name).getInt();
}
//---------------------------------------------------------------------------
uint64_t JSONObjectRef::getUInt(string_view name) const
// Get an integer value
{
   return getEntry(name).getUInt();
}
//---------------------------------------------------------------------------
const string& JSONObjectRef::getString(string_view name) const
// Get a string value
{
   return getEntry(name).getString();
}
//---------------------------------------------------------------------------
JSONObjectRef JSONObjectRef::getObject(string_view name) const
// Get an object value
{
   return getEntry(name).getObject();
}
//---------------------------------------------------------------------------
JSONArrayRef JSONObjectRef::getArray(string_view name) const
// Get an array value
{
   return getEntry(name).getArray();
}
//---------------------------------------------------------------------------
unsigned JSONArrayRef::size() const
// The array size. Has linear runtime!
{
   unsigned result = 0;
   for (auto iter = node->child.get(); iter; iter = iter->next.get())
      ++result;
   return result;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
