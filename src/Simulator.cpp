#include "src/Simulator.hpp"
#include "src/SimulatorImpl.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
/// Check whether the solution can be valid
void verifySolution(const AssignmentProblem& problem, const TaskAssignment& solution) {
   // stream to collect errors
   std::stringstream str;
   // check whether all tasks are located
   std::unordered_map<Task, const TaskAssignment::LocatedTask*> locatedTasks;
   for (const auto& task : solution.locatedTasks) {
      locatedTasks[task.task] = &task;
   }
   for (const auto& [pipeline, tasks] : problem.tasks) {
      for (const auto& task : tasks) {
         if (!locatedTasks.contains(task)) {
            str << "Task(Pipeline " << task.pipeline << ", Partition " << task.scannedPartition.partitionIndex << ") missing in solution\n";
         }
      }
   }

   // check whether all shuffle targets are located
   std::unordered_map<Partition, const TaskAssignment::ShuffleTarget*> shuffleTargets;
   for (const auto& shuffleTarget : solution.shuffleTargets) {
      shuffleTargets[shuffleTarget.partition] = &shuffleTarget;
   }
   for (const auto& shuffle : problem.plan.shuffleStages) {
      std::size_t nToPartitions = problem.plan.dataUnits[shuffle.to].partitionLayout->nPartitions;
      for (size_t i = 0; i < nToPartitions; ++i) {
         if (!shuffleTargets.contains(Partition{shuffle.to, i})) {
            str << "Shuffle Target(DataUnit " << shuffle.to << " Partition " << i << ") is not located in solution\n";
         }
      }
   }

   // Issue a message if an error occurred
   std::string message = str.str();
   if (!message.empty())
      throw std::runtime_error(message);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
Simulator::Simulator(const AssignmentProblem* problem) : problem(problem) {}
//---------------------------------------------------------------------------
Simulator::Result Simulator::simulate(const TaskAssignment& solution) {
   verifySolution(*problem, solution);
   SimulatorImpl simulator(problem->cluster,
                           &solution,
                           problem->partitioning,
                           &problem->plan,
                           problem->resultNode,
                           problem->nParallelTasks);
   return simulator.simulate();
}
//---------------------------------------------------------------------------
std::string Simulator::recordSimulation(const TaskAssignment& solution) {
   verifySolution(*problem, solution);
   SimulatorImpl simulator(problem->cluster,
                           &solution, problem->partitioning,
                           &problem->plan,
                           problem->resultNode,
                           problem->nParallelTasks);
   std::stringstream str;
   JSONWriter writer(str);
   simulator.recordSimulation(solution, *problem).write(writer);
   return std::string{str.str()};
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
