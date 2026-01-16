#pragma once
//---------------------------------------------------------------------------
#include "src/MemorySimulator.hpp"
#include "src/Simulator.hpp"
#include "src/TempFile.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/network/NetworkSimulator.hpp"
#include <memory>
#include <string>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
struct SimulatorRecording {
   std::unique_ptr<TempFile> states;
   AssignmentProblem problem;
   TaskAssignment taskAssignment;
   void write(infra::JSONWriter& out) const;
};
//---------------------------------------------------------------------------
/// State of a simulator during execution
class SimulatorImpl {
   public:
   /// A running task that has a progress
   struct RunningTask {
      TaskAssignment::LocatedTask task;
      /// Progress in percent
      double progress = 0;
      /// Constructor
      explicit RunningTask(const TaskAssignment::LocatedTask* task) : task(*task) {}
      /// Constructor
      RunningTask() : task(), progress(0) {}
   };
   /// A running transfer that has a progress
   struct RunningTransfer {
      TaskAssignment::Transfer transfer;
      /// Remaining waiting time for network latency in seconds. Progress only changes if latency time is 0.
      double latencyTime;
      /// Progress in percent
      double progress = 0;
      /// Constructor
      explicit RunningTransfer(const TaskAssignment::Transfer* transfer, double latencyTime) : transfer(*transfer), latencyTime(latencyTime) {}
      /// Constructor
      RunningTransfer() : transfer(), latencyTime(0), progress(0) {}
   };
   /// A partially collected shuffle output partition
   struct ShuffleOutPartition {
      /// The partition that is collected
      Partition partition;
      /// The node at which the partition is collected
      NodeId node;
      /// The collected amount in percent
      double progress = 0;
   };
   /// A running shuffle operation
   struct RunningShuffle {
      /// The partition to send to
      Partition targetPartition;
      /// The node that sends the shard
      NodeId from;
      /// The node that collects the partition
      NodeId to;
      /// The fraction of the target partition that is sent in this shard
      double shardFraction;
      /// Remaining waiting time for network latency in seconds. Progress only changes if latency time is 0.
      double latencyTime;
      /// Progress in percent of the shard
      double progress;
   };
   /// A serializable struct that keeps the state of the simulation
   struct State {
      /// The tasks that have not started yet
      std::vector<TaskAssignment::LocatedTask> pendingTasks;
      /// The transfers that have not started yet
      std::vector<TaskAssignment::Transfer> pendingTransfers;
      /// A map to look up the targets of shuffle partitions
      std::unordered_map<Partition, std::vector<NodeId>> shuffleTargets;
      /// The currently running tasks
      std::vector<RunningTask> runningTasks;
      /// The currently running transfers
      std::vector<RunningTransfer> runningTransfers;
      /// The currently running transfers of partitions shards during shuffle stages
      std::vector<RunningShuffle> runningShuffles;
      /// The partially collected partitions of a shuffle
      std::unordered_map<LocatedPartition, ShuffleOutPartition> collectedShuffles;
      /// Partitioning keeps track of localization of all partitions
      Partitioning partitions;
      /// The available memory at each node
      std::vector<double> freeMemory;
      /// The simulated elapsed time in seconds
      double runtime;
      /// The number of executed steps
      uint64_t nSteps;
      /// Flag to indicate whether simulation is done
      bool done;
      /// Flag to indicate whether simulation is aborted
      bool aborted;
      /// Abort message
      std::string abortMessage;
      /// Write the object to json
      [[maybe_unused]] void write(infra::JSONWriter& out) const;
   };

   public:
   /// Marker to keep track of data units that do not have to be shuffled anymore
   ///  This only contains data units that are created by broadcast pipelines where the shuffle
   ///  could be triggered more than once
   std::unordered_set<DataUnitId> completedShuffles;
   /// Map that contains data units that will be shuffled
   std::unordered_map<DataUnitId, const ShuffleStage*> shuffledDataUnits;
   /// The tasks that have not started yet
   std::vector<const TaskAssignment::LocatedTask*> pendingTasks;
   /// The transfers that have not started yet
   std::vector<const TaskAssignment::Transfer*> pendingTransfers;
   /// A map to look up the targets of shuffle partitions
   std::unordered_map<Partition, std::vector<NodeId>> shuffleTargets;
   /// The currently running tasks
   std::vector<RunningTask> runningTasks{};
   /// The currently running transfers
   std::vector<RunningTransfer> runningTransfers{};
   /// The currently running transfers of partitions shards during shuffle stages
   std::vector<RunningShuffle> runningShuffles{};
   /// The partially collected partitions of a shuffle
   std::unordered_map<LocatedPartition, ShuffleOutPartition> collectedShuffles;
   /// PartitionManager that keeps track of localization of all partitions
   PartitionManager partitions;
   /// MemorySimulator that keeps track of the available memory at each node
   MemorySimulator memorySimulator;
   /// A network simulator to compute transfer times
   NetworkSimulator networkSimulator;
   /// The cluster to simulate on
   const Cluster* cluster;
   /// The query plan
   const DistributedQueryPlan* plan;
   /// The node on which the result should be present.
   /// If it is set, the result has to be single node and on that node.
   /// If there is no result node, the result may have any partitioning
   std::optional<NodeId> resultNode;
   /// Number of tasks that can be executed in parallel on a node
   size_t nParallelTasks;
   /// The simulated elapsed time in seconds
   double runtime = 0;
   /// The number of executed steps
   uint64_t nSteps = 0;
   /// Flag to indicate whether simulation is done
   bool done = false;
   /// Flag to indicate whether simulation is aborted
   bool aborted = false;
   /// Abort message
   std::string abortMessage{};
   /// Tolerance for finishing times
   static constexpr double timeTolerance = 1e-12;
   /// Tolerance for progress
   static constexpr double progressTolerance = 1e-12;

   public:
   /// Constructor
   SimulatorImpl(const Cluster& cluster, const TaskAssignment* solution, const Partitioning& partitioning, const DistributedQueryPlan* plan, std::optional<NodeId> resultNode, size_t nParallelTasks);
   /// Run the simulation
   Simulator::Result simulate();
   /// Run the simulation and record the states
   SimulatorRecording recordSimulation(const TaskAssignment& solution, const AssignmentProblem& problem);

   public:
   /// Check whether the simulation is still running
   bool isRunning() const;
   /// Store the current State
   State getState() const;
   /// Find the shuffle jobs that are running at the beginning
   void findInitialShuffles();
   /// Find ready tasks and transfers
   void findReadyJobs();
   /// Run on step in the simulation (a step can have arbitrary simulated running time and ends when a job finishes)
   void runStep(bool allowEmptyTasks = false);
   /// Compute the remaining times for all tasks
   std::vector<double> taskRuntimes();
   /// Compute the remaining times for all transfers and shuffles
   std::pair<std::vector<double>, std::vector<double>> transferRuntimes();
   /// Compute the complete execution time of a task on a node when it does not have to share performance
   double executionTime(const Task& task, const Node& node);
   /// Compute the size of a partition
   double partitionSize(Partition partition);
   /// Start shuffles
   void startShuffle(DataUnitId sourceDU, DataUnitId targetDU, NodeId sourceNode);
   /// Finish a shuffle
   void finishShuffle(const RunningShuffle& shuffle);
   /// Get information about why simulation got stuck
   std::string getStuckInfo() const;

   /// Check whether all inputs of a task are present anywhere in the cluster
   bool inputAvailable(const Task& task) const;
};
//---------------------------------------------------------------------------
}
