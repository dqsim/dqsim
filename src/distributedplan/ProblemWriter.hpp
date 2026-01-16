#pragma once
//---------------------------------------------------------------------------
#include "src/Cluster.hpp"
#include "src/CustomTypes.hpp"
#include "src/distributedplan/DistributedQueryPlan.hpp"
#include "src/infra/JSONWriter.hpp"
#include "src/partitioning/PartitionManager.hpp"
#include "src/partitioning/Partitioner.hpp"
#include "src/tasks/Task.hpp"
#include <optional>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// The entire problem instance that is written out to json
struct AssignmentProblem {
   /// The cluster
   Cluster cluster;
   /// The query plan
   DistributedQueryPlan plan;
   /// The initial partitioning
   Partitioning partitioning;
   /// The tasks that need to be located
   std::unordered_map<PipelineId, std::vector<Task>> tasks;
   /// The broadcast tasks that need to be executed on every node
   std::unordered_map<PipelineId, Task> broadcastTasks;
   /// A name for the problem for debugging
   std::string name;
   /// The node on which the result should be present.
   /// If it is set, the result has to be single node and on that node.
   /// If there is no result node, the result may have any partitioning
   std::optional<NodeId> resultNode;
   /// Number of tasks that can be executed in parallel on a node
   uint64_t nParallelTasks;

   /// Equality
   bool operator==(const AssignmentProblem& other) const noexcept = default;
   /// Read the object
   void read(JSONReader& in, JSONValue value);
   /// Write the object to json
   void write(JSONWriter& out) const;
};
//---------------------------------------------------------------------------
/// Class to create a complete problem instance and write it to json
class ProblemWriter {
   /// The plan to use for the problem
   DistributedQueryPlan* plan;
   /// The cluster to distribute the tasks over
   const Cluster* cluster;
   /// Initial Partitioning
   std::unique_ptr<PartitionManager> initialPartitioning;
   /// The tasks to which need to be assigned to machines (created by buildTasks())
   std::optional<std::unordered_map<PipelineId, std::vector<Task>>> tasks = std::nullopt;
   /// The tasks to which are broadcast (created by buildTasks())
   std::optional<std::unordered_map<PipelineId, Task>> broadcastTasks = std::nullopt;
   /// Compute the required partitions
   std::vector<Partition> getRequired(const Pipeline& pipeline, std::optional<uint64_t> partitionIndex);
   /// Name of the problem for debugging
   std::string name;
   /// The node on which the result should be present.
   /// If it is set, the result has to be single node and on that node.
   /// If there is no result node, the result may have any partitioning
   std::optional<NodeId> resultNode;
   /// Number of tasks that can be executed in parallel on a node
   size_t nParallelTasks;

   public:
   /// Constructor
   ProblemWriter(DistributedQueryPlan& plan, const Cluster& cluster, std::string name, std::optional<NodeId> resultNode, size_t nParallelTasks);
   /// Distribute partitions of base relations to nodes
   void distributeBaseRelations(Partitioner& planner);
   /// Load an initial partitioning for base relations
   void loadBaseRelationPartitioning(const Partitioning& partitioning);
   /// Build the tasks
   void buildTasks();
   /// Get a problem instance
   AssignmentProblem getProblem();
   /// Write the problem
   void write(JSONWriter out);
};
//---------------------------------------------------------------------------
/// An object to track the solution of a problem
struct TaskAssignment {
   /// Task with assigned location
   struct LocatedTask {
      /// The task
      Task task;
      /// The node it is located at
      NodeId node;
   };

   /// Transfer of a partition from one node to another
   struct Transfer {
      /// The partition to transfer
      Partition partition;
      /// The node that sends the partition
      NodeId from;
      /// The node that receives the partition
      NodeId to;
   };

   /// During a shuffle, we repartition data and collect it on a target node.
   struct ShuffleTarget {
      /// The target shuffle partition
      Partition partition;
      /// The node the partition should be shuffled to
      NodeId location;
   };

   /// Located Tasks
   std::vector<LocatedTask> locatedTasks;
   /// Transfers
   std::vector<Transfer> transfers;
   /// Shuffle Targets
   std::vector<ShuffleTarget> shuffleTargets;

   /// Write the assignment
   void write(JSONWriter out) const;
   /// Read the object
   void read(JSONReader& in, JSONValue value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::AssignmentProblem> : public StructMapper<dqsim::AssignmentProblem> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::TaskAssignment::LocatedTask> : public StructMapper<dqsim::TaskAssignment::LocatedTask> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::TaskAssignment::Transfer> : public StructMapper<dqsim::TaskAssignment::Transfer> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::TaskAssignment::ShuffleTarget> : public StructMapper<dqsim::TaskAssignment::ShuffleTarget> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::TaskAssignment> : public StructMapper<dqsim::TaskAssignment> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
