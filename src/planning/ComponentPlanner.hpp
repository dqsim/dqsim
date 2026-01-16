#pragma once
//---------------------------------------------------------------------------
#include "src/planning/TaskAssignmentPlanner.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Simple round robin task assignment that fulfills distribution requirements
class ComponentPlanner : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
