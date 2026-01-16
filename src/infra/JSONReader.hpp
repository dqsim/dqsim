#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Config.hpp"
#include <memory>
#include <string>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
/// Known types
enum class JSONType : unsigned { Null,
                                 Bool,
                                 Number,
                                 String,
                                 Object,
                                 Array };
//---------------------------------------------------------------------------
/// A data node
struct JSONNode {
   /// Other nodes. Used for arrays and objects
   std::unique_ptr<JSONNode> next, child;
   /// The key (if inside an object)
   std::string key;
   /// The values
   std::string value;
   /// The type
   JSONType type;

   /// Destructor
   ~JSONNode();
};
//---------------------------------------------------------------------------
struct JSONObjectRef;
struct JSONArrayRef;
//---------------------------------------------------------------------------
/// A non-owing value reference
struct JSONValue {
   /// The node
   const JSONNode* node;

   /// Is valid?
   bool isValid() const { return node; }
   /// Get the type
   JSONType getType() const { return node->type; }

   /// Is null?
   bool isNull() const { return node->type == JSONType::Null; }
   /// Get a boolean value
   bool getBool() const;
   /// Get a double value
   double getDouble() const;
   /// Get an integer value
   int64_t getInt() const;
   /// Get an integer value
   uint64_t getUInt() const;
   /// Get a string value
   const std::string& getString() const;
   /// Get an object value
   JSONObjectRef getObject() const;
   /// Get an array value
   JSONArrayRef getArray() const;
};
//---------------------------------------------------------------------------
/// An object
struct JSONObjectRef {
   /// The node
   const JSONNode* node;

   /// Convert back to value
   JSONValue asValue() const { return JSONValue{node}; }

   /// Get an entry
   JSONValue getEntry(std::string_view name, bool check = true) const;
   /// Is null?
   bool isNull(std::string_view name) const;
   /// Get a boolean value
   bool getBool(std::string_view name) const;
   /// Get a double value
   double getDouble(std::string_view name) const;
   /// Get an integer value
   int64_t getInt(std::string_view name) const;
   /// Get an integer value
   uint64_t getUInt(std::string_view name) const;
   /// Get a string value
   const std::string& getString(std::string_view name) const;
   /// Get an object value
   JSONObjectRef getObject(std::string_view name) const;
   /// Get an array value
   JSONArrayRef getArray(std::string_view name) const;
};
/// An array
struct JSONArrayRef {
   /// The node
   const JSONNode* node;

   /// An iterator
   struct iterator {
      /// The current position
      const JSONNode* pos;

      /// Compare
      bool operator==(iterator o) const { return pos == o.pos; }
      /// Compare
      bool operator!=(iterator o) const { return pos != o.pos; }
      /// Dereference
      JSONValue operator*() const { return JSONValue{pos}; }
      /// Increment
      void operator++() { pos = pos->next.get(); }
   };
   /// Iterator
   iterator begin() const { return iterator{node->child.get()}; }
   /// Iterator
   iterator end() const { return iterator{nullptr}; }
   /// The array size. Has linear runtime!
   unsigned size() const;
};
//---------------------------------------------------------------------------
/// A JSON reader
class JSONReader {
   public:
   /// Throw an exception when traversing the JSON document
   [[noreturn]] static void throwJSONException(std::string_view message);

   private:
   /// The nodes
   std::unique_ptr<JSONNode> root;

   public:
   /// Constructor
   JSONReader();
   /// Destructor
   ~JSONReader();

   /// Parse a JSON document
   bool parse(std::istream& in);
   /// Get the root node
   JSONValue getRoot() const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
