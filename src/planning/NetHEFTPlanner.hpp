#pragma once
//---------------------------------------------------------------------------
#include "src/Cluster.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/planning/HEFTScheduler.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// This planner will have the problem that it will place some pipeline first and can never consider future pipelines in this step
class NetHEFTPlanner : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
