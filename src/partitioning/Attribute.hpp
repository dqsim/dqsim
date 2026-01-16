#pragma once
//---------------------------------------------------------------------------
#include "src/CustomTypes.hpp"
#include "src/infra/JSONMapping.hpp"
#include <string>
#include <unordered_set>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
using namespace dqsim::infra;
//---------------------------------------------------------------------------
/// An attribute of a data unit
struct Attribute {
   /// Name
   std::string name;
   /// Id of the data unit
   DataUnitId dataUnitId;
   /// Index of the attribute within the data unit
   uint64_t attributeIndex;
   /// The size of an element of this attribute in bytes
   double size;

   /// Default Constructor
   Attribute() = default;
   /// Constructor
   Attribute(std::string name, DataUnitId dataUnitId, uint64_t attributeIndex, double size) : name(std::move(name)), dataUnitId(dataUnitId), attributeIndex(attributeIndex), size(size) {}
   /// Equality
   bool operator==(const Attribute& other) const = default;
   /// Read the object
   void read(JSONReader& in, JSONValue value);
   /// Write the object to json
   void write(JSONWriter& out) const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::Attribute> : public StructMapper<dqsim::Attribute> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace std {
//---------------------------------------------------------------------------
template <>
struct hash<dqsim::Attribute> {
   size_t operator()(dqsim::Attribute const& attribute) const noexcept {
      size_t seed = 0;
      size_t h1 = hash<dqsim::DataUnitId>{}(attribute.dataUnitId);
      size_t h2 = hash<uint64_t>{}(attribute.attributeIndex);
      size_t h3 = hash<string>{}(attribute.name);
      boost::hash_combine(seed, h1);
      boost::hash_combine(seed, h2);
      boost::hash_combine(seed, h3);
      return seed;
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
