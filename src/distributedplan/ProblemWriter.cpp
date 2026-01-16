//---------------------------------------------------------------------------
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/CustomTypes.hpp"
#include "src/partitioning/Partitioner.hpp"
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::AssignmentProblem>::enumEntries(Context& context, dqsim::AssignmentProblem& value) {
   mapRequired(context, "cluster", value.cluster);
   mapRequired(context, "plan", value.plan);
   mapRequired(context, "tasks", value.tasks);
   mapRequired(context, "broadcastTasks", value.broadcastTasks);
   mapRequired(context, "initialPartitioning", value.partitioning);
   mapRequired(context, "name", value.name);
   mapOptional(context, "resultNode", value.resultNode);
   mapRequired(context, "nParallelTasks", value.nParallelTasks);
}
//---------------------------------------------------------------------------
void IO<dqsim::TaskAssignment::LocatedTask>::enumEntries(Context& context, dqsim::TaskAssignment::LocatedTask& value) {
   mapRequired(context, "task", value.task);
   mapRequired(context, "node", value.node);
}
//---------------------------------------------------------------------------
void IO<dqsim::TaskAssignment::Transfer>::enumEntries(Context& context, dqsim::TaskAssignment::Transfer& value) {
   mapRequired(context, "partition", value.partition);
   mapRequired(context, "from", value.from);
   mapRequired(context, "to", value.to);
}
//---------------------------------------------------------------------------
void IO<dqsim::TaskAssignment::ShuffleTarget>::enumEntries(Context& context, dqsim::TaskAssignment::ShuffleTarget& value) {
   mapRequired(context, "partition", value.partition);
   mapRequired(context, "location", value.location);
}
//---------------------------------------------------------------------------
void IO<dqsim::TaskAssignment>::enumEntries(Context& context, dqsim::TaskAssignment& value) {
   mapRequired(context, "locatedTasks", value.locatedTasks);
   mapRequired(context, "transfers", value.transfers);
   mapRequired(context, "shuffleTargets", value.shuffleTargets);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void AssignmentProblem::read(JSONReader& in, JSONValue value) {
   json::IO<AssignmentProblem>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void AssignmentProblem::write(JSONWriter& out) const {
   json::IO<AssignmentProblem>::output(out, *this);
}
//---------------------------------------------------------------------------
std::vector<Partition> ProblemWriter::getRequired(const Pipeline& pipeline, std::optional<uint64_t> partitionIndex) {
   std::vector<Partition> required;
   for (auto du : pipeline.requiredData) {
      auto nRequiredPartitions = plan->dataUnits[du].partitionLayout->nPartitions;
      if (plan->dataUnits[du].partitionLayout->isPartitioned()) {
         if (partitionIndex) {
            required.emplace_back(du, *partitionIndex);
         } else {
            for (uint64_t i = 0; i < nRequiredPartitions; ++i) required.emplace_back(du, i);
         }
      } else {
         required.emplace_back(du, 0);
      }
   }
   return required;
};
//---------------------------------------------------------------------------
void ProblemWriter::buildTasks() {
   assert(tasks == std::nullopt);
   assert(broadcastTasks == std::nullopt);
   tasks = std::unordered_map<PipelineId, std::vector<Task>>();
   broadcastTasks = std::unordered_map<PipelineId, Task>();
   for (const Pipeline& pipeline : plan->getPipelines()) {
      auto nInPartitions = plan->dataUnits[pipeline.scan].partitionLayout->nPartitions;
      auto nOutPartitions = plan->dataUnits[pipeline.output].partitionLayout->nPartitions;
      // normal data parallel pipeline
      if (plan->getDataUnit(pipeline.scan).partitionLayout->isPartitioned()) {
         std::vector<Task> currentTasks;
         for (uint64_t partitionIndex = 0; partitionIndex < nInPartitions; ++partitionIndex) {
            // compute required partitions to execute this task based on distribution requirements
            currentTasks.emplace_back(pipeline.id, Partition{pipeline.scan, partitionIndex}, getRequired(pipeline, partitionIndex), Partition{pipeline.output, partitionIndex}, false);
         }
         (*tasks)[pipeline.id] = std::move(currentTasks);
      } else {
         auto outType = plan->getDataUnit(pipeline.output).partitionLayout->type;
         if (outType == PartitionLayout::Type::Broadcast) {
            // Broadcast task
            (*broadcastTasks)[pipeline.id] = Task{pipeline.id, Partition{pipeline.scan, 0}, getRequired(pipeline, std::nullopt), Partition(pipeline.output, 0), true};
         } else if (outType == PartitionLayout::Type::SingleNode) {
            // Only a single task to compute the single output partition
            (*tasks)[pipeline.id] = {Task{pipeline.id, Partition{pipeline.scan, 0}, getRequired(pipeline, std::nullopt), Partition(pipeline.output, 0), false}};
         } else if (outType == PartitionLayout::Type::HashPartitioned || outType == PartitionLayout::Type::Scattered) {
            // Create one task for each output partition
            std::vector<Task> currentTasks;
            for (uint64_t partitionIndex = 0; partitionIndex < nOutPartitions; ++partitionIndex) {
               currentTasks.emplace_back(pipeline.id, Partition{pipeline.scan, 0}, getRequired(pipeline, partitionIndex), Partition{pipeline.output, partitionIndex}, false);
            }
            (*tasks)[pipeline.id] = std::move(currentTasks);
         }
      }
   }
}
//---------------------------------------------------------------------------
void ProblemWriter::distributeBaseRelations(Partitioner& planner) {
   initialPartitioning = std::make_unique<PartitionManager>(*cluster);
   for (DataUnitId id{0}; id < plan->getDataUnits().size(); ++id) {
      const DataUnit& du = plan->getDataUnit(id);
      auto nPartitions = du.partitionLayout->nPartitions;
      assert(nPartitions);
      if (!du.isIntermediate) {
         // assert(du.partitionLayout->type == );
         if (du.partitionLayout->type == PartitionLayout::Type::Broadcast) {
            assert(nPartitions == 1);
            Partition partition{du.id, 0};
            for (std::size_t i = 0; i < cluster->getComputeNodes().size(); ++i) {
               if (i == 0) {
                  initialPartitioning->registerPartition(partition, NodeId{i});
               } else {
                  initialPartitioning->cachePartition(partition, NodeId{i});
               }
            }
         } else {
            planner.assignPartitions(*initialPartitioning, *cluster, nPartitions, du);
         }
      }
   }
}
//---------------------------------------------------------------------------
ProblemWriter::ProblemWriter(DistributedQueryPlan& plan,
                             const Cluster& cluster,
                             std::string name,
                             std::optional<NodeId> resultNode, size_t nParallelTasks)
   : plan(&plan),
     cluster(&cluster),
     initialPartitioning(std::make_unique<PartitionManager>(cluster)), name(std::move(name)),
     resultNode(std::move(resultNode)),
     nParallelTasks(nParallelTasks) {}
//---------------------------------------------------------------------------
void ProblemWriter::loadBaseRelationPartitioning(const Partitioning& partitioning) {
   initialPartitioning = std::make_unique<PartitionManager>(PartitionManager::loadPartitioning(partitioning));
}
//---------------------------------------------------------------------------
AssignmentProblem ProblemWriter::getProblem() {
   assert(tasks);
   assert(broadcastTasks);
   return {*cluster,
           *plan,
           initialPartitioning->getPartitioning(),
           *tasks,
           *broadcastTasks,
           name,
           resultNode,
           nParallelTasks};
}
//---------------------------------------------------------------------------
void ProblemWriter::write(JSONWriter out) {
   getProblem().write(out);
}
//---------------------------------------------------------------------------
void TaskAssignment::write(JSONWriter out) const {
   json::IO<TaskAssignment>::output(out, *this);
}
//---------------------------------------------------------------------------
void TaskAssignment::read(JSONReader& in, JSONValue value) {
   json::IO<TaskAssignment>::input(in, value, *this);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
