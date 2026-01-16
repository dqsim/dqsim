#pragma once
//---------------------------------------------------------------------------
#include "src/benchmark/ClusterBuilder.hpp"
#include "src/infra/JSONWriter.hpp"
#include <vector>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
enum class PlannerType {
   // Round Robin planners
   RR,
   ARR,

   // EFT planners
   HEFT,
   NetHEFT,
   CEFT,

   // Greedy Planners
   //GMN
   Greedy1,
   // Fallback to aligned RR
   Greedy2,
   // Fallback to EST
   Greedy3,
   // Fallback to EFT
   Greedy4,

   // Enumeration planners
   BruteForce,
   Rand1k,
   Rand10k,
   Rand100k,

   // State Sep planners
   StateSep,
   StateSepB,
   StateSepP2P,
   StateSepP2PB,
   StateSepNoUp,
   StateSepNoUpB,
};
//---------------------------------------------------------------------------
enum class PartitionerType {
   /// Partitions are only on storage service
   None,
   /// All nodes have all partitions
   Copy,
   /// The first node gets all partitions
   One,
   /// One random node gets all partitions
   OneRand,
   /// Aligned round robin. Partition n of each DU will be mapped to node n % nNodes
   /// All nodes get partitions
   RRAll,
   /// Only the first half of nodes gets partitions
   RRHalf,
   /// Partitions are assigned round robin to half the nodes, but the list of nodes is randomized
   RRHalfRand,
   /// Partitions are randomly assigned to nodes
   /// One copy of each partition
   Rand1,
   /// Two copies of each partition
   Rand2,
   /// Four copies of each partition
   Rand4,
};
//---------------------------------------------------------------------------
struct BenchmarkResult {
   /// The name of the assignment algorithm
   std::string assignmentAlgorithm;
   /// The date of the benchmark
   std::string date;
   /// Time to execute all benchmarks in seconds
   double totalExecutionTime;
   /// Time to optimize assignments for all benchmarks in seconds
   double totalSchedulingTime;
   /// Names of all benchmarks
   std::vector<std::string> benchmarkNames;
   /// Execution times of all benchmarks
   std::vector<double> executionTimes;
   /// Scheduling times of all benchmarks
   std::vector<double> schedulingTimes;

   /// Write the object to json
   void write(dqsim::infra::JSONWriter& out) const;
};
//---------------------------------------------------------------------------
enum class QueryCategory {
   // The TPC-H queries
   TPCH,
   // The growing query subset from SQLStorm
   Growing,
   // The handcrafted queries
   Handcrafted,
};
//---------------------------------------------------------------------------
struct BenchmarkDefinition {
   /// The name of the benchmark
   std::string name;
   /// The clusters to use for all queries
   std::vector<dqsim::ClusterBuilderDefinition> clusters;
   /// The partition counts to use for all queries
   std::vector<uint64_t> partitionCounts;
   /// All queries to benchmark
   std::vector<QueryCategory> queryCategories;
   /// The partitioners to use for base relations
   std::vector<PartitionerType> partitioners;
   /// The planners (scheduling algorithms) to use
   std::vector<PlannerType> planners;
   /// Number of tasks that can be executed in parallel on a node
   uint64_t nParallelTasks;
   /// The maximum number of tasks a problem may have. This can be used for full enumeration experiments.
   std::optional<uint64_t> taskLimit;

   /// Write the benchmark definition
   void write(infra::JSONWriter out) const;

   /// Load definition from file
   static BenchmarkDefinition loadBenchmarkFromFile(const std::string& path);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<BenchmarkResult> : public StructMapper<BenchmarkResult> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<BenchmarkDefinition> : public StructMapper<BenchmarkDefinition> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<PartitionerType> : public EnumMapper<PartitionerType> {};
//---------------------------------------------------------------------------
template <>
struct IO<PlannerType> : public EnumMapper<PlannerType> {};
//---------------------------------------------------------------------------
template <>
struct IO<QueryCategory> : public EnumMapper<QueryCategory> {};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
