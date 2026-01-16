#include "src/distributedplan/DistributedQueryPlan.hpp"
#include "src/Cluster.hpp"
#include <cmath>
#include <iomanip>
#include <iostream>
#include <ranges>
#include <unordered_set>
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
void formatBytes(std::ostream& out, double bytes) {
   const char* sizes[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
   int order = 0;

   while (bytes >= 1024 && order < static_cast<int>(std::size(sizes)) - 1) {
      bytes /= 1024;
      ++order;
   }

   out << std::fixed << std::setprecision(2) << bytes << " " << sizes[order];
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
std::span<DataUnit> DistributedQueryPlan::getDataUnits() {
   return dataUnits;
}
//---------------------------------------------------------------------------
DataUnit& DistributedQueryPlan::getDataUnit(DataUnitId dataUnitId) {
   assert(dataUnitId < dataUnits.size());
   assert(dataUnitId == dataUnits[dataUnitId].id);
   return dataUnits[dataUnitId];
}
//---------------------------------------------------------------------------
std::ranges::iota_view<uint64_t, uint64_t> DistributedQueryPlan::getDataUnitIds() {
   return std::ranges::iota_view<uint64_t, uint64_t>{0, dataUnits.size()};
}
//---------------------------------------------------------------------------
const std::vector<Pipeline>& DistributedQueryPlan::getPipelines() const {
   return pipelines;
}
//---------------------------------------------------------------------------
void DistributedQueryPlan::read(JSONReader& in, JSONValue value) {
   json::IO<DistributedQueryPlan>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void DistributedQueryPlan::write(JSONWriter& out) const {
   json::IO<DistributedQueryPlan>::output(out, *this);
}
//---------------------------------------------------------------------------
std::vector<DataUnitId> DistributedQueryPlan::getBaseRelations() const {
   std::vector<DataUnitId> result;
   for (const auto& du : dataUnits) {
      // Assume id 0 is always result relation
      if (du.id != DataUnitId{0} && !du.isIntermediate) {
         result.push_back(du.id);
      }
   }
   return result;
}
//---------------------------------------------------------------------------
DataUnitId DistributedQueryPlan::getResultDataUnit() const {
   auto lastPipelineOutput = getPipeline(dependencyOrder.back()).output;
   DataUnitId result = lastPipelineOutput;
   for (const auto& shuffleStage : shuffleStages) {
      if (shuffleStage.from == lastPipelineOutput) {
         assert(result == lastPipelineOutput);
         result = shuffleStage.to;
      }
   }
   /// The output relation should always be single node
   // assert(getDataUnit(result).partitionLayout->type == PartitionLayout::Type::SingleNode);
   // assert(getDataUnit(result).partitionLayout->nPartitions == 1);
   // assert(!getDataUnit(result).partitionLayout->isPartitioned());
   return result;
}
//---------------------------------------------------------------------------
const Pipeline& DistributedQueryPlan::getPipeline(PipelineId pipelineId) const {
   if (pipelines[pipelineId].id != pipelineId)
      std::cout << "Internal error: Pipelines not sorted by index!\n";
   assert(pipelines.size() > static_cast<size_t>(pipelineId));
   assert(pipelines[pipelineId].id == pipelineId);
   return pipelines[pipelineId];
}
//---------------------------------------------------------------------------
void DistributedQueryPlan::toDOT(std::ostream& out) const
/// Print the plan in DOT format
{
   out << "digraph distributed_plan {\n";
   // data units
   for (const DataUnit& du : dataUnits) {
      out << "\"du" << du.id << "\" [shape = Mrecord, style = bold, label = \"{" << du.name << " (" << du.id << ") | size: " << du.estimatedCard << "(";
      formatBytes(out, du.estimatedCard * du.tupleSize());
      out << ") | ";
      if (du.partitionLayout.has_value())
         du.partitionLayout->prettyPrint(out);
      out << " | ";
      out << " | ";
      size_t nAttrs = 1;
      for (const auto& attr : du.attributes) {
         out << attr.name << ", ";
         if (nAttrs++ >= 3) {
            nAttrs = 1;
            out << "\\n";
         }
      }
      out << "}\"]\n";
   }
   // pipelines
   for (const Pipeline& pipeline : pipelines) {
      out << "\"p" << pipeline.id << "\" [shape = record, label = \"{Pipeline " << pipeline.id << " | "
          << pipeline.description << "}\"]\n";
   }
   // edges between pipelines and data units
   for (const Pipeline& pipeline : pipelines) {
      out << "du" << pipeline.scan << " -> p" << pipeline.id << "\n";
      out << "p" << pipeline.id << " -> du" << pipeline.output << "\n";
      for (DataUnitId id : pipeline.requiredData) {
         out << "du" << id << " -> p" << pipeline.id << " [color = \"grey\", arrowhead = onormal]\n";
      }
   }
   // shuffle stages
   for (const ShuffleStage& shuffle : shuffleStages) {
      out << "du" << shuffle.from << " -> du" << shuffle.to << " [label = \"shuffle\"]\n";
   }
   out << "}\n";
}
//---------------------------------------------------------------------------
const DataUnit& DistributedQueryPlan::getDataUnit(DataUnitId dataUnitId) const {
   return const_cast<DistributedQueryPlan&>(*this).getDataUnit(dataUnitId);
}
//---------------------------------------------------------------------------
void DistributedQueryPlan::verifyIndexes() const {
   for (size_t i = 0; i < dataUnits.size(); ++i) {
      assert(dataUnits[i].id == DataUnitId{static_cast<uint64_t>(i)});
   }
   for (size_t i = 0; i < pipelines.size(); ++i) {
      assert(pipelines[i].id == PipelineId{static_cast<uint64_t>(i)});
   }
}
//---------------------------------------------------------------------------
bool DistributedQueryPlan::isShuffleInput(DataUnitId du) const {
   for (const auto& s : shuffleStages) {
      if (s.from == du) return true;
   }
   return false;
}
//---------------------------------------------------------------------------
bool DistributedQueryPlan::isShuffleOutput(DataUnitId du) const {
   for (const auto& s : shuffleStages) {
      if (s.to == du) return true;
   }
   return false;
}
//---------------------------------------------------------------------------
std::optional<DataUnitId> DistributedQueryPlan::getShuffleOutput(dqsim::DataUnitId du) const {
   for (const auto& s : shuffleStages) {
      if (s.from == du) return s.to;
   }
   return std::nullopt;
}
//---------------------------------------------------------------------------
bool DistributedQueryPlan::isBroadcast(dqsim::PipelineId p) const {
   const Pipeline& pipeline = getPipeline(p);
   return getDataUnit(pipeline.scan).partitionLayout->type == PartitionLayout::Type::Broadcast &&
      getDataUnit(pipeline.output).partitionLayout->type == PartitionLayout::Type::Broadcast;
}
//---------------------------------------------------------------------------
double DistributedQueryPlan::estimateRuntime(const Node& node, PipelineId pipelineId) const {
   const Pipeline& pipeline = getPipeline(pipelineId);
   auto nInPartitions = dataUnits[pipeline.scan].partitionLayout->nPartitions;
   assert(nInPartitions);
   assert(node.nCores > 0);
   double taskTuples = (dataUnits[pipeline.scan].estimatedCard / nInPartitions);
   double relativePerformance = (static_cast<double>(node.nCores) / 16.0);
   return pipeline.estimatedLoad * taskTuples / relativePerformance;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::DistributedQueryPlan>::enumEntries(Context& context, dqsim::DistributedQueryPlan& value) {
   mapRequired(context, "dataUnits", value.dataUnits);
   mapRequired(context, "pipelines", value.pipelines);
   mapRequired(context, "shuffleStages", value.shuffleStages);
   mapRequired(context, "dependencyOrder", value.dependencyOrder);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
