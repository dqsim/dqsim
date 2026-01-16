#pragma once
//---------------------------------------------------------------------------
#include "src/planning/TaskAssignmentPlanner.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Simple greedy task assignment that assigns tasks to nodes which need to load the least data and fulfills distribution requirements
/// Falls back to simple round robin when no node is best
class GreedyPlanner1 : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
/// Simple greedy task assignment that assigns tasks to nodes which need to load the least data and fulfills distribution requirements
/// Falls back to aligned round robin when no node is best
class GreedyPlanner2 : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
/// Simple greedy task assignment that assigns tasks to nodes which need to load the least data and fulfills distribution requirements
/// Falls back to EST-based load-balanced aligned assignment when no node is best
class GreedyPlanner3 : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
/// Simple greedy task assignment that assigns tasks to nodes which need to load the least data and fulfills distribution requirements
/// Falls back to EFT-based load-balanced aligned assignment when no node is best
class GreedyPlanner4 : public TaskAssignmentPlanner {
   public:
   /// Assign tasks to nodes
   TaskAssignment assignTasks(const AssignmentProblem& problem) override;
   /// Get the name of the planner
   std::string getName() const override;
};
//---------------------------------------------------------------------------
//  Then use dependency order and compute EFT with load on compute and network to step-by-step assign
//  each component to a node
//  (maybe not EFT, but simply minimize max of used time across 3 resources (CPU, Upload, Download))
//  Also, create a simpler version of this, that simply assigns each partition number to a node
//---------------------------------------------------------------------------
//  1. disconnect plan by shuffle stages
//  2. for each component, determine the most expensive (or most determined) pipeline: best_cost/median_cost
//  3. perform a graph search to step to neighboring pipeline frontier, keep track of cheapest "re-shuffling" DU
//  4. when you find a pipeline at which the cost of re-shuffling is lower than the delta between
//     selected and optimal assignment, re-shuffle at that part, re-assign for between pipelines
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
