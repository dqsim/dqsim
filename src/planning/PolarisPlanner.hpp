#pragma once
//---------------------------------------------------------------------------
#include "src/planning/TaskAssignmentPlanner.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
class PolarisPlanner : public TaskAssignmentPlanner {
   /// Whether to allow P2P communication between nodes
   bool allowP2P;
   /// Whether to force uploading every DU to the storage service
   bool forceUpload;
   /// Whether to allow instantiating the same task multiple times for broadcasts
   bool allowBroadcastPipelines;

   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
   /// Constructor
   PolarisPlanner(bool allowP2P, bool forceUpload, bool allowBroadcastPipelines)
      : allowP2P(allowP2P), forceUpload(forceUpload), allowBroadcastPipelines(allowBroadcastPipelines) {}
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
