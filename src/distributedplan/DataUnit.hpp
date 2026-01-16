#pragma once
//---------------------------------------------------------------------------
#include "src/CustomTypes.hpp"
#include "src/partitioning/Attribute.hpp"
#include "src/partitioning/PartitionLayout.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Represents either a relation or an intermediate result
class DataUnit {
   public:
   /// Name
   std::string name;
   /// Attributes
   std::vector<Attribute> attributes;
   /// The partition layout of the data unit
   std::optional<PartitionLayout> partitionLayout;
   /// The id of the data unit
   DataUnitId id;
   /// Estimated number of tuples in the data unit
   double estimatedCard;
   /// True if the data unit represents an intermediate result
   bool isIntermediate;

   /// Constructor
   DataUnit(std::string name, std::vector<Attribute> attributes, std::optional<PartitionLayout> partitionLayout, DataUnitId id, double estimatedCard, bool isIntermediate)
      : name(std::move(name)), attributes(std::move(attributes)), partitionLayout(std::move(partitionLayout)), id(id), estimatedCard(estimatedCard), isIntermediate(isIntermediate) {
      assert(!partitionLayout || partitionLayout->nPartitions);
   }
   /// Constructor
   DataUnit() = default;

   /// Equality
   bool operator==(const DataUnit& other) const = default;
   /// Compute the tuple size in bytes
   double tupleSize() const;
   /// Compute the size of a partition in bytes
   double partitionSize() const;

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
struct IO<dqsim::DataUnit> : public StructMapper<dqsim::DataUnit> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
