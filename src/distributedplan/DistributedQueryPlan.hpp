#pragma once
//---------------------------------------------------------------------------
#include "src/distributedplan/Pipeline.hpp"
#include <iostream>
#include <optional>
#include <ranges>
#include <span>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
struct Node;
//---------------------------------------------------------------------------
class DistributedQueryPlan {
   public:
   /// All data units used in the query
   std::vector<DataUnit> dataUnits;
   /// All pipelines of the Query
   std::vector<Pipeline> pipelines;
   /// All shuffle stages of the Query
   std::vector<ShuffleStage> shuffleStages;
   /// Get pipelines in dependency order (i.e. dependent pipeline comes after its dependencies)
   std::vector<PipelineId> dependencyOrder;

   /// Constructor
   DistributedQueryPlan(std::vector<DataUnit> dataUnits,
                        std::vector<Pipeline>
                           pipelines,
                        std::vector<ShuffleStage>
                           shuffleStages,
                        std::vector<PipelineId>
                           dependencyOrder)
      : dataUnits(std::move(dataUnits)),
        pipelines(std::move(pipelines)),
        shuffleStages(std::move(shuffleStages)),
        dependencyOrder(std::move(dependencyOrder)) {
      verifyIndexes();
      getResultDataUnit();
   };
   /// Constructor
   DistributedQueryPlan() = default;
   /// Copy Constructor
   DistributedQueryPlan(const DistributedQueryPlan& other) = default;
   /// Copy Assignment
   DistributedQueryPlan& operator=(const DistributedQueryPlan& other) = default;
   /// Move Constructor
   DistributedQueryPlan(DistributedQueryPlan&& other) noexcept = default;
   /// Move Assignment
   DistributedQueryPlan& operator=(DistributedQueryPlan&& other) noexcept = default;
   /// Equality
   bool operator==(const DistributedQueryPlan& other) const = default;
   /// Verify the indexes of data units and pipelines
   void verifyIndexes() const;
   /// Get the data units
   std::span<DataUnit> getDataUnits();
   /// Get the data unit ids
   std::ranges::iota_view<uint64_t, uint64_t> getDataUnitIds();
   /// Get a specific data unit
   DataUnit& getDataUnit(DataUnitId dataUnitId);
   /// Get a specific data unit
   const DataUnit& getDataUnit(DataUnitId dataUnitId) const;
   /// Get the pipelines
   const std::vector<Pipeline>& getPipelines() const;
   /// Get a pipeline by id
   const Pipeline& getPipeline(PipelineId pipelineId) const;
   /// Get the base relations, i.e. all relations that are present in the beginning
   std::vector<DataUnitId> getBaseRelations() const;
   /// Get the result data unit
   DataUnitId getResultDataUnit() const;
   /// Read the object
   void read(JSONReader& in, JSONValue value);
   /// Write the object to json
   void write(JSONWriter& out) const;
   /// Print the plan in DOT format
   void toDOT(std::ostream& out) const;
   /// Check whether the data unit will be used by a shuffle
   bool isShuffleInput(DataUnitId du) const;
   /// Check whether the data unit is created by a shuffle
   bool isShuffleOutput(DataUnitId du) const;
   /// Find the shuffle output of the data unit. Nullopt if du is not shuffled
   std::optional<DataUnitId> getShuffleOutput(DataUnitId du) const;
   /// Check whether the pipeline is executed as broadcast
   bool isBroadcast(PipelineId p) const;
   /// Estimate the execution time of a task of this pipeline for a given node
   double estimateRuntime(const Node& node, PipelineId pipeline) const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::DistributedQueryPlan> : public StructMapper<dqsim::DistributedQueryPlan> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
