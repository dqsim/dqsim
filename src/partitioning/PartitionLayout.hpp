#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Enum.hpp"
#include "src/infra/JSONMapping.hpp"
#include "src/partitioning/Attribute.hpp"
#include <string_view>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
using namespace dqsim::infra;
//---------------------------------------------------------------------------
/// Struct to define how a data unit should be partitioned
struct PartitionLayout {
   enum class Type {
      /// Data is stored in an unordered way
      Scattered,
      /// Data is hash partitioned by a set of attributes
      HashPartitioned,
      /// Data is stored at all nodes
      Broadcast,
      /// Data is stored at a single node
      SingleNode
   };

   /// The type of partitioning
   Type type;
   /// The number of partitions
   uint64_t nPartitions;
   /// The attributes on which it is partitioned (only relevant for hash partitioned)
   std::unordered_set<Attribute> attributes;
   /// Constructor
   PartitionLayout();
   /// Constructor
   PartitionLayout(Type type, std::size_t nPartitions, std::unordered_set<Attribute> attributes);
   /// Equality
   bool operator==(const PartitionLayout& other) const = default;
   /// Checks whether a layout satisfies another layout
   bool satisfies(const PartitionLayout& other) const;
   /// Write as string
   void prettyPrint(std::ostream& out) const;
   /// Checks whether the data unit uses multiple partitions
   bool isPartitioned() const { return type == Type::Scattered || type == Type::HashPartitioned; }
};
//---------------------------------------------------------------------------
/// Class to define base relation partitioning without knowing the DU and attribute index in advance
struct BaseRelationLayoutDefinition {
   /// Simplified class for relation names
   struct Layout {
      /// The name of the relation
      std::string name;
      /// The type of layout
      PartitionLayout::Type type;
      /// The number of partitions, 1 if not partitioned
      uint64_t nPartitions;
      /// Attributes (for hash distributed)
      std::vector<std::string> attributes;
   };
   /// List of all layouts
   std::vector<Layout> layouts;
   const Layout* getLayout(std::string_view name) const;

   /// Read the object
   void read(JSONReader& in, JSONValue value);
   /// Write the object to json
   void write(JSONWriter& out) const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
template <>
struct infra::EnumInfo<dqsim::PartitionLayout::Type> : public EnumInfoBase<dqsim::PartitionLayout::Type> {};
//---------------------------------------------------------------------------
template <>
struct infra::json::IO<dqsim::PartitionLayout::Type> : public EnumMapper<dqsim::PartitionLayout::Type> {
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::PartitionLayout> : public StructMapper<dqsim::PartitionLayout> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::BaseRelationLayoutDefinition::Layout> : public StructMapper<dqsim::BaseRelationLayoutDefinition::Layout> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::BaseRelationLayoutDefinition> : public StructMapper<dqsim::BaseRelationLayoutDefinition> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
