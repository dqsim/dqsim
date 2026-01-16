#pragma once
//---------------------------------------------------------------------------
#include "src/CustomTypes.hpp"
#include "src/infra/JSONMapping.hpp"
#include <cstdint>
#include <span>
#include <vector>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Represents the necessary information of a node for the simulator
struct Node {
   /// The number of cores
   uint64_t nCores;
   /// The main memory size in GiB
   double memorySize;
   /// The network speed in g_bit per second
   double networkSpeed;
   /// The expected network latency in seconds
   double networkLatency;

   /// Equality
   bool operator==(const Node& other) const = default;

   /// Read the object
   void read(infra::JSONReader& in, infra::JSONValue value);
   /// Write the object to json
   void write(infra::JSONWriter& out) const;
   /// Get the bandwidth in bytes/s
   double byteBandwidth() const;
};
//---------------------------------------------------------------------------
/// Represents all available nodes
struct Cluster {
   /// Ordered list of nodes
   std::vector<Node> nodes;
   /// Storage service like S3 can be simulated like a virtual node with very large bandwidth
   /// The last element in nodes can be this storage service node
   bool storageService = false;

   /// Constructor
   explicit Cluster(std::span<Node> nodes, bool storageService = false) : nodes(nodes.begin(), nodes.end()), storageService(storageService) {}
   /// Constructor
   explicit Cluster(std::initializer_list<Node> nodes, bool storageService = false) : nodes(nodes.begin(), nodes.end()), storageService(storageService) {}
   /// Constructor
   Cluster() = default;
   /// Equality
   bool operator==(const Cluster& other) const = default;
   /// Get the compute nodes excluding storage service
   std::span<Node const> getComputeNodes() const;
   /// Check whether a node is a compute node
   bool isComputeNode(NodeId node) const;

   /// Read the object
   void read(infra::JSONReader& in, infra::JSONValue value);
   /// Write the object to json
   void write(infra::JSONWriter& out) const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::Node> : public StructMapper<dqsim::Node> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::Cluster> : public StructMapper<dqsim::Cluster> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
