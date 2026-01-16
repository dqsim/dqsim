#include "src/benchmark/BenchmarkDefinition.hpp"
#include "src/Simulator.hpp"
#include "src/distributedplan/DistributedQueryPlan.hpp"
#include "src/infra/Enum.hpp"
#include "src/partitioning/Partitioner.hpp"
#include "src/planning/ComponentPlanner.hpp"
#include "src/planning/EnumerationPlanner.hpp"
#include "src/planning/GreedyPlanner.hpp"
#include "src/planning/HEFTPlanner.hpp"
#include "src/planning/NetHEFTPlanner.hpp"
#include "src/planning/PolarisPlanner.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
[[maybe_unused]] void writeBenchmark(const std::string& path, const std::vector<AssignmentProblem>& problems, const std::vector<std::string>& names) {
   for (size_t i = 0; i < problems.size(); ++i) {
      std::ofstream outFile(path + "/" + names[i] + "_problem.json");
      JSONWriter writer(outFile);
      problems[i].write(writer);
   }
}
//---------------------------------------------------------------------------
[[maybe_unused]] void writeAssignments(const std::string& path, const std::vector<TaskAssignment>& assignments, const std::vector<std::string>& names) {
   for (size_t i = 0; i < assignments.size(); ++i) {
      std::ofstream outFile(path + "/" + names[i] + "_assignment.json");
      dqsim::infra::JSONWriter writer(outFile);
      assignments[i].write(writer);
   }
}
//---------------------------------------------------------------------------
}
void BenchmarkDefinition::write(JSONWriter out) const {
   json::IO<BenchmarkDefinition>::output(out, *this);
}
//---------------------------------------------------------------------------
void BenchmarkResult::write(infra::JSONWriter& out) const {
   infra::json::IO<BenchmarkResult>::output(out, *this);
}
//---------------------------------------------------------------------------
BenchmarkDefinition BenchmarkDefinition::loadBenchmarkFromFile(const std::string& path) {
   std::ifstream file(path);
   if (!file.is_open()) {
      throw std::runtime_error(std::format("Failed to open benchmark file: {}", path));
   }

   dqsim::infra::JSONReader reader;
   if (!reader.parse(file)) {
      throw std::runtime_error(std::format("Failed to parse JSON from file: {}", path));
   }

   BenchmarkDefinition benchmark;
   auto root = reader.getRoot().getObject();
   dqsim::infra::json::Context context(reader, root);
   dqsim::infra::json::IO<BenchmarkDefinition>::enumEntries(context, benchmark);

   return benchmark;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<BenchmarkResult>::enumEntries(Context& context, BenchmarkResult& value) {
   mapRequired(context, "assignmentAlgorithm", value.assignmentAlgorithm);
   mapRequired(context, "date", value.date);
   mapRequired(context, "totalExecutionTime", value.totalExecutionTime);
   mapRequired(context, "totalSchedulingTime", value.totalSchedulingTime);
   mapRequired(context, "benchmarkNames", value.benchmarkNames);
   mapRequired(context, "executionTimes", value.executionTimes);
   mapRequired(context, "schedulingTimes", value.schedulingTimes);
}
//---------------------------------------------------------------------------
void IO<BenchmarkDefinition>::enumEntries(Context& context, BenchmarkDefinition& value) {
   mapRequired(context, "name", value.name);
   mapRequired(context, "partitionCounts", value.partitionCounts);
   mapRequired(context, "queryCategories", value.queryCategories);
   mapRequired(context, "partitioners", value.partitioners);
   mapRequired(context, "clusters", value.clusters);
   mapRequired(context, "planners", value.planners);
   mapRequired(context, "nParallelTasks", value.nParallelTasks);
   mapOptional(context, "taskLimit", value.taskLimit);
}
//---------------------------------------------------------------------------
}
