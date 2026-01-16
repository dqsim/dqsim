#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Config.hpp"
#include <string_view>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
class JSONWriter;
class JSONArrayScope;
//---------------------------------------------------------------------------
/// An object scope
class JSONObjectScope {
   public:
   using string_view = std::string_view;

   private:
   /// The target
   JSONWriter& out;
   /// First entry?
   bool first;
   /// Use indentation?
   bool indent;

   /// Constructor
   JSONObjectScope(JSONWriter& out, bool indent);

   friend class JSONWriter;

   public:
   /// Destructor
   ~JSONObjectScope();

   /// Get the writer
   JSONWriter& getWriter() { return out; }
   /// A generic entry, the content must be provided by a call to JSONWriter
   void genericEntry(string_view entry);
   /// A null entry
   void nullEntry(string_view entry);
   /// A boolean entry
   void boolEntry(string_view entry, bool value);
   /// A number entry
   void doubleEntry(string_view entry, double value);
   /// A number entry
   void intEntry(string_view entry, int64_t value);
   /// A number entry
   void uintEntry(string_view entry, uint64_t value);
   /// A string entry
   void stringEntry(string_view entry, string_view value);
   /// An object entry
   JSONObjectScope objectEntry(string_view entry, bool indent = false);
   /// An array entry
   JSONArrayScope arrayEntry(string_view entry);
};
//---------------------------------------------------------------------------
/// An array scope
class JSONArrayScope {
   private:
   /// The target
   JSONWriter& out;
   /// First entry?
   bool first;

   /// Constructor
   JSONArrayScope(JSONWriter& out);

   friend class JSONWriter;

   public:
   /// Destructor
   ~JSONArrayScope();

   /// A generic entry, the content must be provided by a call to JSONWriter
   void genericEntry();
   /// A null entry
   void nullEntry();
   /// A boolean entry
   void boolEntry(bool value);
   /// A number entry
   void doubleEntry(double value);
   /// A number entry
   void intEntry(int64_t value);
   /// A number entry
   void uintEntry(uint64_t value);
   /// A string entry
   void stringEntry(std::string_view value);
   /// An object entry
   JSONObjectScope objectEntry(bool indent = false);
   /// An array entry
   JSONArrayScope arrayEntry();
};
//---------------------------------------------------------------------------
/// A JSON writer
class JSONWriter {
   public:
   using string_view = std::string_view;
   friend class JSONObjectScope;
   friend class JSONArrayScope;

   private:
   /// The target
   std::ostream& out;
   /// The indentation level
   unsigned level;

   public:
   /// Get the target
   std::ostream& getOut() { return out; }

   /// Constructor
   explicit JSONWriter(std::ostream& out);

   /// Write a null entry
   void writeNull();
   /// Write a boolean
   void writeBool(bool value);
   /// Write a number
   void writeDouble(double value);
   /// Write a number
   void writeInt(int64_t value);
   /// Write a number
   void writeUInt(uint64_t value);
   /// Write a string
   void writeString(string_view value);
   /// Write an object
   JSONObjectScope writeObject(bool indent = false);
   /// Write an array
   JSONArrayScope writeArray();

   /// Write out an escaped string
   static void escapeString(std::ostream& out, string_view value);

   /// Write to a string
   [[nodiscard]] static std::string writeToString(double value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
