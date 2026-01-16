#pragma once
//---------------------------------------------------------------------------
#include <memory>
#include <string>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
struct AssignmentProblem;
struct TaskAssignment;
//---------------------------------------------------------------------------
/// Simulator to simulate the distributed execution of queries on a virtual cluster
class Simulator {
   public:
   /// Result of a simulation
   struct Result {
      /// Execution time in seconds
      double executionTime;
      /// True if simulation finished successfully
      bool done;
      /// Error message if simulation was aborted
      std::string abortMessage;
   };

   private:
   /// The problem definition
   const AssignmentProblem* problem;

   public:
   /// Constructor
   explicit Simulator(const AssignmentProblem* problem);
   /// Simulate a proposed task assignment
   Result simulate(const TaskAssignment& solution);
   /// Record a simulation
   std::string recordSimulation(const TaskAssignment& solution);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
