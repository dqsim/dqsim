#pragma once
//---------------------------------------------------------------------------
#include "src/benchmark/BenchmarkDefinition.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
#include <vector>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
struct BenchmarkRunner {
   /// Create a planner object from the corresponding type
   static std::unique_ptr<TaskAssignmentPlanner> getPlanner(PlannerType type);
   static void run(const std::vector<AssignmentProblem>& problems, const BenchmarkDefinition& bench);
};
//---------------------------------------------------------------------------
}
