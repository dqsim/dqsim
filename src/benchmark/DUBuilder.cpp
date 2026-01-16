#include "DUBuilder.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
DataUnit DUBuilder::buildDU(std::string name, double size, std::size_t nPartitions, bool intermediate,
                            std::span<const Attribute> attributeDefinitions, std::optional<size_t> partitionAttributeIndex,
                            bool makeSingleNode) {
   DataUnitId duId{dus.size()};
   std::vector<dqsim::Attribute> attributes;
   size_t iAttr = 0;
   for (const Attribute& attr : attributeDefinitions) {
      attributes.push_back(dqsim::Attribute{attr.name, duId, iAttr++, attr.size});
   }
   PartitionLayout layout{PartitionLayout::Type::Scattered, nPartitions, {}};
   if (partitionAttributeIndex.has_value()) {
      layout = PartitionLayout{PartitionLayout::Type::HashPartitioned, nPartitions, {attributes[*partitionAttributeIndex]}};
   }
   if (makeSingleNode) {
      layout = PartitionLayout{PartitionLayout::Type::SingleNode, nPartitions, {}};
   }
   DataUnit result{name, std::move(attributes), layout, duId, size, intermediate};
   dus.push_back(result);
   return result;
}
//---------------------------------------------------------------------------
const std::vector<DataUnit>& DUBuilder::getDUs() const {
   return dus;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
