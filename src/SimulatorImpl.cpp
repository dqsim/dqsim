#include "src/SimulatorImpl.hpp"
#include "src/MemorySimulator.hpp"
#include "src/TempFile.hpp"
#include "src/Util.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/infra/JSONMapping.hpp"
#include "src/network/NetworkSimulator.hpp"
#include "src/partitioning/PartitionManager.hpp"
#include <algorithm>
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
double taskProgress(double previousProgress, double remainingTime, double stepTime) {
   if (remainingTime == std::numeric_limits<double>::infinity()) return 0;
   if (remainingTime == 0) return 1 - previousProgress;
   double completionFraction = stepTime / remainingTime;
   assert(!(completionFraction != completionFraction));
   return (1 - previousProgress) * completionFraction;
}
//---------------------------------------------------------------------------
bool checkReady(const TaskAssignment::LocatedTask& task, const PartitionManager& partitions) {
   for (Partition partition : task.task.getInputPartitions()) {
      if (!partitions.hasPartition(task.node, partition)) return false;
   }
   return true;
}
//---------------------------------------------------------------------------
bool checkReady(const TaskAssignment::Transfer& transfer, const PartitionManager& partitions) {
   return partitions.hasPartition(transfer.from, transfer.partition);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
SimulatorImpl::SimulatorImpl(const Cluster& cluster, const TaskAssignment* solution, const Partitioning& partitioning, const DistributedQueryPlan* plan, std::optional<NodeId> resultNode, size_t nParallelTasks)
   : shuffledDataUnits(),
     pendingTasks(),
     pendingTransfers(),
     shuffleTargets(),
     runningTasks(),
     runningTransfers(),
     runningShuffles(),
     collectedShuffles(),
     partitions(PartitionManager::loadPartitioning(partitioning, &cluster)),
     memorySimulator(std::span{plan->dataUnits.begin(), plan->dataUnits.end()}, cluster),
     networkSimulator(&cluster),
     cluster(&cluster),
     plan(plan),
     resultNode(resultNode),
     nParallelTasks(nParallelTasks) {
   for (const auto& task : solution->locatedTasks) {
      pendingTasks.push_back(&task);
   }
   for (const auto& transfer : solution->transfers) {
      pendingTransfers.push_back(&transfer);
   }
   for (const auto& shuffleTarget : solution->shuffleTargets) {
      shuffleTargets[shuffleTarget.partition].push_back(shuffleTarget.location);
   }
   for (const auto& shuffleStage : plan->shuffleStages) {
      shuffledDataUnits[shuffleStage.from] = &shuffleStage;
   }
   for (const auto& [p, n] : partitioning.owningNode) {
      memorySimulator.storePartition(n, p);
   }
   for (const auto& [p, nodes] : partitioning.cachingNodes) {
      for (NodeId n : nodes) {
         memorySimulator.storePartition(n, p);
      }
   }
}
//---------------------------------------------------------------------------
bool SimulatorImpl::isRunning() const {
   return (!done) & (!aborted);
}
//---------------------------------------------------------------------------
Simulator::Result SimulatorImpl::simulate() {
   findInitialShuffles();
   while (isRunning()) {
      findReadyJobs();
      runStep();
   }

   return Simulator::Result{runtime, done, abortMessage};
}
//---------------------------------------------------------------------------
SimulatorRecording SimulatorImpl::recordSimulation(const TaskAssignment& assignment, const AssignmentProblem& problem) {
   auto states = std::make_unique<TempFile>();
   auto& stateWriter = states->getWriteStream();
   infra::JSONWriter jsonWriter(stateWriter);
   getState().write(jsonWriter);
   stateWriter << ",";
   findInitialShuffles();
   while (isRunning()) {
      findReadyJobs();
      getState().write(jsonWriter);
      stateWriter << ",";
      runStep();
   }
   getState().write(jsonWriter);
   return {std::move(states), problem, assignment};
}
//---------------------------------------------------------------------------
SimulatorImpl::State SimulatorImpl::getState() const {
   std::vector<TaskAssignment::LocatedTask> pendingTasksCopy;
   pendingTasksCopy.reserve(pendingTasks.size());
   for (const auto* t : pendingTasks) {
      pendingTasksCopy.push_back(*t);
   }
   std::vector<TaskAssignment::Transfer> pendingTransfersCopy;
   pendingTransfersCopy.reserve(pendingTransfers.size());
   for (const auto* t : pendingTransfers) {
      pendingTransfersCopy.push_back(*t);
   }
   std::unordered_map<Partition, std::vector<NodeId>> shuffleTargetCopy;
   for (const auto& [p, t] : shuffleTargets) {
      shuffleTargetCopy[p] = t;
   }
   std::vector<double> freeMemory(memorySimulator.getFreeMemory().begin(), memorySimulator.getFreeMemory().end());
   return State{
      pendingTasksCopy,
      pendingTransfersCopy,
      shuffleTargetCopy,
      runningTasks,
      runningTransfers,
      runningShuffles,
      collectedShuffles,
      partitions.getPartitioning(),
      freeMemory,
      runtime,
      nSteps,
      done,
      aborted,
      abortMessage,
   };
}
//---------------------------------------------------------------------------
void SimulatorImpl::findInitialShuffles() {
   auto baseRelations = plan->getBaseRelations();
   std::unordered_set<DataUnitId> baseRelationSet(baseRelations.begin(), baseRelations.end());
   for (const auto& shuffle : plan->shuffleStages) {
      if (baseRelationSet.contains(shuffle.from)) {
         std::size_t nFromPartitions = plan->dataUnits[shuffle.from].partitionLayout->nPartitions;
         for (size_t i = 0; i < nFromPartitions; ++i) {
            Partition currentSourcePartition{shuffle.from, i};
            assert(partitions.getOwningNode().contains(currentSourcePartition));
            startShuffle(shuffle.from, shuffle.to, partitions.getOwningNode().find(currentSourcePartition)->second);
         }
      }
   }
}
//---------------------------------------------------------------------------
void SimulatorImpl::findReadyJobs() {
   std::vector<size_t> nTasksPerNode(cluster->nodes.size(), 0);
   for (const auto& t : runningTasks) {
      ++nTasksPerNode[t.task.node];
   }
   std::vector<size_t> startedTasks;
   for (size_t i = 0; i < pendingTasks.size(); ++i) {
      std::size_t node = pendingTasks[i]->node;
      if (checkReady(*pendingTasks[i], partitions) && nTasksPerNode[node] < nParallelTasks) {
         ++nTasksPerNode[node];
         runningTasks.emplace_back(pendingTasks[i]);
         startedTasks.push_back(i);
         memorySimulator.storePartition(pendingTasks[i]->node, pendingTasks[i]->task.outputPartition);
      }
   }
   filter(pendingTasks, startedTasks);
   std::vector<size_t> startedTransfers;
   for (size_t i = 0; i < pendingTransfers.size(); ++i) {
      const TaskAssignment::Transfer* currentTransfer = pendingTransfers[i];
      if (checkReady(*currentTransfer, partitions)) {
         startedTransfers.push_back(i);
         if (!partitions.hasPartition(currentTransfer->to, currentTransfer->partition)) {
            // immediately finish transfer if it is not necessary
            double latency = std::max(cluster->nodes[currentTransfer->from].networkLatency, cluster->nodes[currentTransfer->to].networkLatency);
            runningTransfers.emplace_back(currentTransfer, latency);
            memorySimulator.storePartition(currentTransfer->to, currentTransfer->partition);
         } else {
            // When this encounters, we assume there is a problem in the assignment algorithm
            aborted = true;
            abortMessage = "Transferred a partition to a node that already has it. "
                           "This implies planning inefficiencies.";
         }
      }
   }
   filter(pendingTransfers, startedTransfers);
}
//---------------------------------------------------------------------------
void SimulatorImpl::finishShuffle(const RunningShuffle& shuffle) {
   // update shuffle collects, if a partition is fully collected, store it in partition manager
   auto& partition = shuffle.targetPartition;
   LocatedPartition target{partition, shuffle.to};
   assert(collectedShuffles.contains(target));
   collectedShuffles[target].progress += 1.0 * shuffle.shardFraction;
   if (collectedShuffles[target].progress >= 1.0 - progressTolerance) {
      if (partitions.partitionStored(partition)) {
         partitions.cachePartition(partition, collectedShuffles[target].node);
      } else {
         partitions.registerPartition(partition, collectedShuffles[target].node);
      }
      collectedShuffles.erase(target);
   }
}
//---------------------------------------------------------------------------
bool SimulatorImpl::inputAvailable(const dqsim::Task& task) const {
   for (const auto& d : task.getInputPartitions()) {
      if (!partitions.partitionStored(d)) return false;
   }
   return true;
}
//---------------------------------------------------------------------------
std::string SimulatorImpl::getStuckInfo() const {
   std::unordered_map<PipelineId, size_t> dependencyIndex;
   for (size_t i = 0; i < plan->dependencyOrder.size(); ++i) {
      dependencyIndex[plan->dependencyOrder[i]] = i;
   }
   size_t bestDepIndex = plan->dependencyOrder.size();
   size_t smallestPartitionId = std::numeric_limits<size_t>::max();
   const TaskAssignment::LocatedTask* bestTask = nullptr;
   for (const auto& t : pendingTasks) {
      if (bestTask && bestDepIndex < dependencyIndex[t->task.pipeline])
         continue;
      if (bestTask && smallestPartitionId < t->task.scannedPartition.partitionIndex)
         continue;
      bestTask = t;
      bestDepIndex = dependencyIndex[t->task.pipeline];
      smallestPartitionId = t->task.scannedPartition.partitionIndex;
   }
   if (!bestTask)
      return "";
   std::string missingPartitionInfo;
   std::vector<Partition> missingPartitions;
   for (const auto& p : bestTask->task.getInputPartitions()) {
      if (partitions.hasPartition(bestTask->node, p))
         continue;
      std::string nodes;
      for (const auto& n : partitions.owningAndCachingNodes(p)) {
         nodes.append(std::format("Node {}, ", static_cast<size_t>(n)));
      }
      if (nodes.empty())
         nodes = "No node stores this partition";
      missingPartitionInfo.append(std::format(
         "{}.{}: present on [{}]",
         static_cast<size_t>(p.dataUnitId),
         static_cast<size_t>(p.partitionIndex),
         nodes));
   }
   return std::format(
      "Task {}.{} should be executed on Node {}.\n{}",
      static_cast<size_t>(bestTask->task.pipeline),
      static_cast<size_t>(bestTask->task.scannedPartition.partitionIndex),
      static_cast<size_t>(bestTask->node),
      missingPartitionInfo);
}
//---------------------------------------------------------------------------
void SimulatorImpl::runStep(bool allowEmptyTasks) {
   // Check for deadlock
   if (runningTasks.empty() && runningTransfers.empty() && runningShuffles.empty() && !done && !allowEmptyTasks) {
      aborted = true;
      abortMessage = "No tasks or transfers can run anymore before the simulation finished\n";
      abortMessage.append(getStuckInfo());
      return;
   }
   // {
   //    auto resultDU = plan->getDataUnit(plan->getResultDataUnit());
   //    assert(!resultDU.partitionLayout->isPartitioned());
   //    if (partitions.hasPartition(NodeId{0}, Partition{resultDU.id, 0})) {
   //       std::cout << "WHY ARE YOU RUNNING?" << std::endl;
   //    }
   // }
   {
      std::vector<size_t> nTasksPerNode(cluster->nodes.size(), 0);
      for (const auto& t : runningTasks) {
         ++nTasksPerNode[t.task.node];
         assert(nTasksPerNode[t.task.node] <= nParallelTasks);
      }
   }

   // Compute step time
   auto taskTimes = taskRuntimes();
   auto [transferTimes, shuffleTimes] = transferRuntimes();
   assert(taskTimes.size() + transferTimes.size() + shuffleTimes.size() > 0);
   double stepTime = std::numeric_limits<double>::infinity();
   if (taskTimes.size() > 0)
      stepTime = std::ranges::min_element(taskTimes)[0];
   if (transferTimes.size() > 0)
      stepTime = std::min(stepTime, std::ranges::min_element(transferTimes)[0]);
   if (shuffleTimes.size() > 0)
      stepTime = std::min(stepTime, std::ranges::min_element(shuffleTimes)[0]);
   assert(stepTime != std::numeric_limits<double>::infinity());

   // Update progress and find all tasks that finish in step
   std::vector<size_t> finishingTasks;
   for (size_t i = 0; i < taskTimes.size(); ++i) {
      if (taskTimes[i] - stepTime < timeTolerance) {
         finishingTasks.push_back(i);
      }
      runningTasks[i].progress += taskProgress(runningTasks[i].progress, taskTimes[i], stepTime);
   }
   [[maybe_unused]] size_t finishingLatencies = 0;
   std::vector<size_t> finishingTransfers;
   for (size_t i = 0; i < transferTimes.size(); ++i) {
      if (runningTransfers[i].latencyTime > 0) {
         assert(runningTransfers[i].latencyTime >= stepTime);
         runningTransfers[i].latencyTime -= stepTime;
         if (runningTransfers[i].latencyTime < timeTolerance) {
            runningTransfers[i].latencyTime = 0;
            ++finishingLatencies;
         }
      } else {
         if (transferTimes[i] - stepTime < timeTolerance) {
            finishingTransfers.push_back(i);
         }
         runningTransfers[i].progress += taskProgress(runningTransfers[i].progress, transferTimes[i], stepTime);
      }
   }
   std::vector<size_t> finishingShuffles;
   for (size_t i = 0; i < runningShuffles.size(); ++i) {
      if (runningShuffles[i].latencyTime > 0) {
         assert(runningShuffles[i].latencyTime >= stepTime);
         runningShuffles[i].latencyTime -= stepTime;
         if (runningShuffles[i].latencyTime < timeTolerance) {
            runningShuffles[i].latencyTime = 0;
            ++finishingLatencies;
         }
      } else {
         if (shuffleTimes[i] - stepTime < timeTolerance) {
            finishingShuffles.push_back(i);
         }
         runningShuffles[i].progress += taskProgress(runningShuffles[i].progress, shuffleTimes[i], stepTime);
      }
   }

   assert(finishingTasks.size() + finishingTransfers.size() + finishingShuffles.size() + finishingLatencies > 0);

   // move output partitions of finished tasks to target nodes
   for (size_t task : finishingTasks) {
      auto node = runningTasks[task].task.node;
      if (partitions.getOwningNode().contains(runningTasks[task].task.task.outputPartition)) {
         assert(plan->getDataUnit(runningTasks[task].task.task.outputPartition.dataUnitId).partitionLayout->type == PartitionLayout::Type::Broadcast);
         partitions.cachePartition(runningTasks[task].task.task.outputPartition, node);
      } else {
         partitions.registerPartition(runningTasks[task].task.task.outputPartition, node);
      }
   }
   for (size_t transfer : finishingTransfers) {
      partitions.cachePartition(runningTransfers[transfer].transfer.partition, runningTransfers[transfer].transfer.to);
   }
   for (size_t shuffle : finishingShuffles) {
      finishShuffle(runningShuffles[shuffle]);
   }

   // start shuffles
   {
      // broadcast tasks should not immediately start a shuffle
      std::unordered_map<DataUnitId, std::vector<NodeId>> broadcastAvailable;
      for (size_t task : finishingTasks) {
         const auto& runningTask = runningTasks[task];
         const Pipeline& pipeline = plan->getPipeline(runningTask.task.task.pipeline);
         if (shuffledDataUnits.contains(pipeline.output)) {
            if (completedShuffles.contains(pipeline.output))
               continue;
            if (runningTask.task.task.broadcast) {
               broadcastAvailable[pipeline.output].push_back(runningTask.task.node);
            } else {
               DataUnitId targetDU = shuffledDataUnits[pipeline.output]->to;
               DataUnitId sourceDU = shuffledDataUnits[pipeline.output]->from;
               startShuffle(sourceDU, targetDU, runningTask.task.node);
            }
         }
      }
      for (const auto& [du, nodes] : broadcastAvailable) {
         bool foundPerfect = false;
         DataUnitId targetDU = shuffledDataUnits[du]->to;
         DataUnitId sourceDU = shuffledDataUnits[du]->from;
         assert(plan->getDataUnit(targetDU).partitionLayout->nPartitions == 1);
         Partition targetPartition{targetDU, 0};
         assert(shuffleTargets.contains(targetPartition));
         assert(shuffleTargets[targetPartition].size() == 1);
         NodeId perfectNode = shuffleTargets[targetPartition].front();
         for (const auto& n : nodes) {
            if (n == perfectNode) {
               foundPerfect = true;
               break;
            }
         }
         if (foundPerfect)
            startShuffle(sourceDU, targetDU, perfectNode);
         else
            startShuffle(sourceDU, targetDU, nodes.front());
         completedShuffles.insert(sourceDU);
      }
   }

   // remove finished tasks from running tasks
   filter(runningTasks, finishingTasks);
   filter(runningTransfers, finishingTransfers);
   filter(runningShuffles, finishingShuffles);

   runtime += stepTime;
   if (pendingTasks.size() + pendingTransfers.size() + runningTasks.size() + runningTransfers.size() + runningShuffles.size() == 0) {
      auto resultDU = plan->getDataUnit(plan->getResultDataUnit());
      assert(!resultNode.has_value() || !resultDU.partitionLayout->isPartitioned());
      if (!resultNode.has_value() || partitions.hasPartition(*resultNode, Partition{resultDU.id, 0})) {
         done = true;
      } else if (!(allowEmptyTasks && finishingShuffles.size() + finishingTasks.size() + finishingLatencies + finishingTransfers.size() != 0)) {
         aborted = true;
         abortMessage = "All tasks and transfers have finished but the result is not present on node 0";
         return;
      }
   }
   ++nSteps;
}
//---------------------------------------------------------------------------
std::vector<double> SimulatorImpl::taskRuntimes() {
   // for each node compute the number of tasks that are executed
   std::vector<size_t> nodeTasks;
   nodeTasks.resize(cluster->nodes.size(), 0);
   for (const auto& task : runningTasks) {
      nodeTasks[task.task.node] += 1;
   }
   std::vector<double> result;
   // for each task compute the remaining time
   for (const auto& task : runningTasks) {
      result.push_back(executionTime(task.task.task, cluster->nodes[task.task.node]) * nodeTasks[task.task.node] * (1 - task.progress));
   }
   return result;
}
//---------------------------------------------------------------------------
double SimulatorImpl::executionTime(const Task& task, const Node& node) {
   return plan->estimateRuntime(node, task.pipeline);
}
//---------------------------------------------------------------------------
std::pair<std::vector<double>, std::vector<double>> SimulatorImpl::transferRuntimes() {
   std::vector<NetworkSimulator::Connection> connections;
   // Exclude transfers and shuffles that are still waiting for network latency
   std::vector<size_t> activeTransferIndex;
   activeTransferIndex.reserve(runningTransfers.size());
   std::vector<double> transferTimes;
   transferTimes.reserve(runningTransfers.size());
   connections.reserve(runningTransfers.size() + runningShuffles.size());
   for (size_t i = 0; i < runningTransfers.size(); ++i) {
      const auto& transfer = runningTransfers[i];
      if (transfer.latencyTime == 0) {
         connections.emplace_back(NetworkSimulator::Connection{transfer.transfer.from, transfer.transfer.to});
         activeTransferIndex.push_back(i);
         transferTimes.push_back(0);
      } else {
         transferTimes.push_back(transfer.latencyTime);
      }
   }
   std::vector<size_t> activeShuffleIndex;
   activeShuffleIndex.reserve(runningShuffles.size());
   std::vector<double> shuffleTimes;
   shuffleTimes.reserve(runningShuffles.size());
   for (size_t i = 0; i < runningShuffles.size(); ++i) {
      const auto& shuffle = runningShuffles[i];
      if (shuffle.latencyTime == 0) {
         connections.emplace_back(NetworkSimulator::Connection{shuffle.from, shuffle.to});
         activeShuffleIndex.push_back(i);
         shuffleTimes.push_back(0);
      } else {
         shuffleTimes.push_back(shuffle.latencyTime);
      }
   }

   assert(activeTransferIndex.size() + activeShuffleIndex.size() == connections.size());
   assert(transferTimes.size() == runningTransfers.size());
   assert(shuffleTimes.size() == runningShuffles.size());

   std::vector speeds = networkSimulator.computeTransferSpeeds(connections);

   for (size_t i = 0; i < activeTransferIndex.size(); ++i) {
      size_t resultIndex = activeTransferIndex[i];
      const RunningTransfer& transfer = runningTransfers[activeTransferIndex[i]];
      transferTimes[resultIndex] = partitionSize(transfer.transfer.partition) / speeds[i] * (1 - transfer.progress);
   }
   for (size_t j = 0; j < activeShuffleIndex.size(); ++j) {
      size_t resultIndex = activeShuffleIndex[j];
      const RunningShuffle& shuffle = runningShuffles[activeShuffleIndex[j]];
      double speed = speeds[activeTransferIndex.size() + j];
      shuffleTimes[resultIndex] = partitionSize(shuffle.targetPartition) * shuffle.shardFraction / speed * (1 - shuffle.progress);
   }

   return {transferTimes, shuffleTimes};
}
//---------------------------------------------------------------------------
double SimulatorImpl::partitionSize(Partition partition) {
   auto nPartitions = plan->dataUnits[partition.dataUnitId].partitionLayout->nPartitions;
   assert(nPartitions);
   return plan->dataUnits[partition.dataUnitId].tupleSize() * plan->dataUnits[partition.dataUnitId].estimatedCard / nPartitions;
}
//---------------------------------------------------------------------------
void SimulatorImpl::startShuffle(DataUnitId sourceDU, DataUnitId targetDU, NodeId sourceNode) {
   auto nOutPartitions = plan->dataUnits[targetDU].partitionLayout->nPartitions;
   auto nInPartitions = plan->dataUnits[sourceDU].partitionLayout->nPartitions;
   assert(nOutPartitions);
   for (size_t i = 0; i < nOutPartitions; ++i) {
      Partition targetPartition{targetDU, i};
      assert(shuffleTargets.contains(targetPartition));
      for (NodeId targetNode : shuffleTargets[targetPartition]) {
         // if collected shuffle does not exist for partition, create it
         LocatedPartition locatedTargetPartition{targetPartition, targetNode};
         if (!collectedShuffles.contains(locatedTargetPartition)) {
            collectedShuffles[locatedTargetPartition] = ShuffleOutPartition{targetPartition, targetNode};
            memorySimulator.storePartition(targetNode, targetPartition);
         }
         double shardFraction = 1.0 / nInPartitions;
         if (sourceNode == targetNode) {
            RunningShuffle localShuffle{
               targetPartition,
               sourceNode,
               targetNode,
               shardFraction,
               0,
               0};
            finishShuffle(localShuffle);
         } else {
            double latency = std::max(cluster->nodes[sourceNode].networkLatency,
                                      cluster->nodes[targetNode].networkLatency);
            runningShuffles.emplace_back(RunningShuffle{
               targetPartition,
               sourceNode,
               targetNode,
               shardFraction,
               latency,
               0});
         }
      }
   }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<SimulatorImpl::RunningTask> : public StructMapper<SimulatorImpl::RunningTask> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
void IO<SimulatorImpl::RunningTask>::enumEntries(Context& context, SimulatorImpl::RunningTask& value) {
   mapRequired(context, "task", value.task);
   mapRequired(context, "progress", value.progress);
}
//---------------------------------------------------------------------------
template <>
struct IO<SimulatorImpl::RunningTransfer> : public StructMapper<SimulatorImpl::RunningTransfer> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
void IO<SimulatorImpl::RunningTransfer>::enumEntries(Context& context, SimulatorImpl::RunningTransfer& value) {
   mapRequired(context, "transfer", value.transfer);
   mapRequired(context, "progress", value.progress);
}
//---------------------------------------------------------------------------
template <>
struct IO<SimulatorImpl::ShuffleOutPartition> : public StructMapper<SimulatorImpl::ShuffleOutPartition> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
void IO<SimulatorImpl::ShuffleOutPartition>::enumEntries(Context& context, SimulatorImpl::ShuffleOutPartition& value) {
   mapRequired(context, "partition", value.partition);
   mapRequired(context, "node", value.node);
   mapRequired(context, "progress", value.progress);
}
//---------------------------------------------------------------------------
template <>
struct IO<SimulatorImpl::RunningShuffle> : public StructMapper<SimulatorImpl::RunningShuffle> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
void IO<SimulatorImpl::RunningShuffle>::enumEntries(Context& context, SimulatorImpl::RunningShuffle& value) {
   mapRequired(context, "targetPartition", value.targetPartition);
   mapRequired(context, "from", value.from);
   mapRequired(context, "to", value.to);
   mapRequired(context, "shardFraction", value.shardFraction);
   mapRequired(context, "progress", value.progress);
}
//---------------------------------------------------------------------------
template <>
struct IO<SimulatorImpl::State> : public StructMapper<SimulatorImpl::State> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
void IO<SimulatorImpl::State>::enumEntries(Context& context, SimulatorImpl::State& value) {
   mapRequired(context, "pendingTasks", value.pendingTasks);
   mapRequired(context, "pendingTransfers", value.pendingTransfers);
   mapRequired(context, "shuffleTargets", value.shuffleTargets);
   mapRequired(context, "runningTasks", value.runningTasks);
   mapRequired(context, "runningTransfers", value.runningTransfers);
   mapRequired(context, "runningShuffles", value.runningShuffles);
   mapRequired(context, "collectedShuffles", value.collectedShuffles);
   mapRequired(context, "partitions", value.partitions);
   mapRequired(context, "freeMemory", value.freeMemory);
   mapRequired(context, "runtime", value.runtime);
   mapRequired(context, "nSteps", value.nSteps);
   mapRequired(context, "done", value.done);
   mapRequired(context, "aborted", value.aborted);
   mapRequired(context, "abortMessage", value.abortMessage);
}
//---------------------------------------------------------------------------
}
namespace {
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
void copyStream(std::istream& in, std::ostream& out, std::size_t len) {
   std::size_t bufferSize = 4096;
   std::vector<char> buffer(bufferSize);
   std::size_t remaining = len;
   while (remaining >= bufferSize) {
      in.read(buffer.data(), bufferSize);
      out.write(buffer.data(), bufferSize);
      remaining -= bufferSize;
   }
   in.read(buffer.data(), remaining);
   out.write(buffer.data(), remaining);
}
//---------------------------------------------------------------------------
}
namespace dqsim {
//---------------------------------------------------------------------------
void SimulatorImpl::State::write(infra::JSONWriter& out) const {
   infra::json::IO<SimulatorImpl::State>::output(out, *this);
}
//---------------------------------------------------------------------------
void SimulatorRecording::write(infra::JSONWriter& out) const {
   auto obj = out.writeObject();
   // states
   obj.genericEntry("states");
   {
      auto arr = out.writeArray();
      auto reader = states->getReadStream();
      copyStream(*reader, out.getOut(), states->size());
   }
   // problem
   obj.genericEntry("problem");
   problem.write(out);
   // taskAssignment
   obj.genericEntry("taskAssignment");
   taskAssignment.write(out);
}
//---------------------------------------------------------------------------
}
