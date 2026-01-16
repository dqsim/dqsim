#include "src/MemorySimulator.hpp"
#include <iostream>
#include <ranges>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
MemorySimulator::MemorySimulator(std::span<const DataUnit> dataUnits, const Cluster& cluster)
   : dataUnits(dataUnits) {
   for (const Node& n : cluster.nodes) {
      freeMemory.push_back(n.memorySize * 1024 * 1024 * 1024);
   }
}
//---------------------------------------------------------------------------
double MemorySimulator::getPartitionSize(Partition partition) {
   const auto& dataUnit = dataUnits[partition.dataUnitId];
   assert(dataUnit.partitionLayout->nPartitions != 0);
   return dataUnit.tupleSize() * dataUnit.estimatedCard / dataUnit.partitionLayout->nPartitions;
}
//---------------------------------------------------------------------------
bool MemorySimulator::allocate(NodeId node, double size) {
   if (freeMemory[node] < size) {
      return false;
   }
   freeMemory[node] -= size;
   return true;
}
//---------------------------------------------------------------------------
void MemorySimulator::free(NodeId node, double size) {
   freeMemory[node] += size;
}
//---------------------------------------------------------------------------
bool MemorySimulator::storePartition(NodeId node, Partition partition) {
   return allocate(node, getPartitionSize(partition));
}
//---------------------------------------------------------------------------
std::span<const double> MemorySimulator::getFreeMemory() const {
   return std::span<const double>(freeMemory.begin(), freeMemory.end());
}
//---------------------------------------------------------------------------

//---------------------------------------------------------------------------
}
