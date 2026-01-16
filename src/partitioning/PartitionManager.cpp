#include "src/partitioning/PartitionManager.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
PartitionManager::PartitionManager(const Cluster& cluster) {
   /// register the nodes in the respective maps
   for (NodeId iNode{0}; iNode < cluster.nodes.size(); ++iNode) {
      ownedPartitions.emplace(iNode, std::unordered_set<Partition>{});
      cachedPartitions.emplace(iNode, std::unordered_set<Partition>{});
   }
}
//---------------------------------------------------------------------------
void PartitionManager::registerPartition(Partition partition, NodeId node) {
   assert(ownedPartitions.find(node) != ownedPartitions.end());
   assert(cachedPartitions.find(node) != cachedPartitions.end());

   // add partition to data unit map
   if (dataUnits.find(partition.dataUnitId) == dataUnits.end()) {
      dataUnits[partition.dataUnitId] = std::unordered_set<Partition>{};
   }
   dataUnits[partition.dataUnitId].insert(partition);

   // if the partition was already assigned to a node, remove it from the node
   if (owningNode.find(partition) != owningNode.end()) {
      NodeId previousNode = owningNode[partition];
      assert(ownedPartitions[previousNode].find(partition) != ownedPartitions[previousNode].end());
      ownedPartitions[previousNode].erase(partition);
   }
   owningNode[partition] = node;
   ownedPartitions[node].insert(partition);
   if (!cachingNodes.contains(partition)) cachingNodes[partition] = std::unordered_set<NodeId>{};
}
//---------------------------------------------------------------------------
void PartitionManager::registerDU(DataUnitId du, size_t nPartitions, NodeId node) {
   for (size_t i = 0; i < nPartitions; ++i) {
      registerPartition(Partition{du, i}, node);
   }
}
//---------------------------------------------------------------------------
void PartitionManager::cachePartition(Partition partition, NodeId node) {
   assert(cachingNodes.find(partition) != cachingNodes.end());
   assert(cachedPartitions.find(node) != cachedPartitions.end());
   assert(owningNode.contains(partition));
   assert(owningNode.find(partition)->second != node);
   cachingNodes[partition].insert(node);
   cachedPartitions[node].insert(partition);
}
//---------------------------------------------------------------------------
void PartitionManager::deregisterDataUnit(DataUnitId dataUnitId) {
   assert(dataUnits.find(dataUnitId) != dataUnits.end());
   for (const Partition& partition : dataUnits[dataUnitId]) {
      removePartitionFromMaps(partition);
   }
   dataUnits.erase(dataUnitId);
}
//---------------------------------------------------------------------------
void PartitionManager::deregisterPartition(Partition partition) {
   removePartitionFromMaps(partition);
   assert(dataUnits.find(partition.dataUnitId) != dataUnits.end());
   dataUnits[partition.dataUnitId].erase(partition);
}
//---------------------------------------------------------------------------
void PartitionManager::removePartitionFromMaps(Partition partition) {
   assert(owningNode.find(partition) != owningNode.end());
   assert(cachingNodes.find(partition) != cachingNodes.end());
   for (const NodeId& node : cachingNodes[partition]) {
      assert(cachedPartitions[node].find(partition) != cachedPartitions[node].end());
      cachedPartitions[node].erase(partition);
   }
   cachingNodes.erase(partition);

   assert(ownedPartitions[owningNode[partition]].find(partition) != ownedPartitions[owningNode[partition]].end());
   ownedPartitions[owningNode[partition]].erase(partition);
   owningNode.erase(partition);
}
//---------------------------------------------------------------------------
void PartitionManager::evictPartitionCache(Partition partition, NodeId node) {
   assert(cachingNodes.find(partition) != cachingNodes.end());
   assert(cachingNodes[partition].find(node) != cachingNodes[partition].end());
   assert(cachedPartitions.find(node) != cachedPartitions.end());
   assert(cachedPartitions[node].find(partition) != cachedPartitions[node].end());
   cachingNodes[partition].erase(node);
   cachedPartitions[node].erase(partition);
}
//---------------------------------------------------------------------------
Partitioning PartitionManager::getPartitioning() const {
   return Partitioning{owningNode, cachingNodes, ownedPartitions.size()};
}
//---------------------------------------------------------------------------
PartitionManager PartitionManager::loadPartitioning(const Partitioning& partitioning, const Cluster* cluster) {
   PartitionManager result{{}};
   if (!cluster) {
      Cluster dummy;
      dummy.nodes.resize(partitioning.nNodes);
      result = PartitionManager{dummy};
   } else {
      result = PartitionManager{*cluster};
   }
   for (auto [partition, node] : partitioning.owningNode) {
      result.registerPartition(partition, node);
   }
   for (auto& [partition, cacheNodes] : partitioning.cachingNodes) {
      for (auto node : cacheNodes) {
         result.cachePartition(partition, node);
      }
   }
   return result;
}
//---------------------------------------------------------------------------
bool PartitionManager::hasPartition(NodeId node, Partition partition) const {
   return (owningNode.contains(partition) && owningNode.find(partition)->second == node) || (cachingNodes.contains(partition) && cachingNodes.find(partition)->second.contains(node));
}
//---------------------------------------------------------------------------
void PartitionManager::print(std::ostream& out) const {
   for (const auto& [du, partitions] : dataUnits) {
      out << "DataUnit " << du << "\n";
      for (const auto& p : partitions) {
         out << " " << p.partitionIndex << ": " << owningNode.find(p)->second << " (";
         for (const auto& cache : cachingNodes.find(p)->second) {
            out << cache << ", ";
         }
         out << ")\n";
      }
      out << "\n";
   }
}
//---------------------------------------------------------------------------
bool PartitionManager::partitionStored(Partition partition) const {
   return owningNode.contains(partition);
}
//---------------------------------------------------------------------------
std::vector<NodeId> PartitionManager::owningAndCachingNodes(Partition p) const {
   if (!partitionStored(p)) return std::vector<NodeId>();
   std::vector<NodeId> result;
   assert(owningNode.contains(p));
   result.push_back(owningNode.find(p)->second);
   assert(cachingNodes.contains(p));
   for (auto n : cachingNodes.find(p)->second) {
      result.push_back(n);
   }
   return result;
}
//---------------------------------------------------------------------------
Partitioning::Partitioning(std::unordered_map<Partition, NodeId> owningNode, std::unordered_map<Partition, std::unordered_set<NodeId>> cachingNodes, size_t nNodes)
   : owningNode(std::move(owningNode)), cachingNodes(std::move(cachingNodes)), nNodes(nNodes) {}
//---------------------------------------------------------------------------
void Partitioning::read(infra::JSONReader& in, infra::JSONValue value) {
   infra::json::IO<Partitioning>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void Partitioning::write(infra::JSONWriter& out) const {
   infra::json::IO<Partitioning>::output(out, *this);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::Partitioning>::enumEntries(Context& context, dqsim::Partitioning& value) {
   mapRequired(context, "owningNode", value.owningNode);
   mapRequired(context, "cachingNodes", value.cachingNodes);
   mapRequired(context, "nNodes", value.nNodes);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
