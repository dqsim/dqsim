//---------------------------------------------------------------------------
#include "src/Simulator.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/infra/CommandLine.hpp"
#include "src/infra/JSONReader.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
#include <fstream>
#include <iostream>
#include <memory>
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
struct Options : public CommandLine {
   Option<bool> help;
   Option<bool> execute;
   Option<bool> printPlan;
   Option<std::string> recordingFile;
   Option<std::string> assignmentFile;

   Options() : CommandLine("Partition assignment problem writer command line interface", "[options] <problemfile>") {
      add(help).longName("help").description("show help");
      add(execute).longName("execute").description("execute the simulator on the result");
      add(assignmentFile).longName("assignment file").shortName('a').description("load an existing task assignment from this file instead of planning it");
      add(recordingFile).longName("recording file").shortName('o').description("record the execution and store it to the output file");
      add(printPlan).longName("print plan").shortName('p').description("print the plan in DOT format");
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc, char** argv) {
   Options options;
   if (!options.parse(std::cerr, argc, argv)) return 1;
   if (options.help.get()) {
      options.showHelp(std::cout);
      return 0;
   }

   AssignmentProblem problem;
   try {
      std::ifstream problemStream(options.getPositional()[0]);
      infra::JSONReader reader;
      reader.parse(problemStream);
      problem.read(reader, reader.getRoot());
   } catch (const std::exception& e) {
      std::cerr << "Error reading problem file: " << e.what() << std::endl;
      return 1;
   }

   TaskAssignment assignment;
   if (!options.assignmentFile) {
      std::unique_ptr<TaskAssignmentPlanner> planner;
      planner = std::make_unique<AlignedRoundRobinPlanner>();

      assignment = planner->assignTasks(problem);
   } else {
      try {
         std::ifstream fileStream(options.assignmentFile.get());
         infra::JSONReader reader;
         reader.parse(fileStream);
         assignment.read(reader, reader.getRoot());
      } catch (const std::exception& e) {
         std::cerr << "Error reading assignment file: " << e.what() << std::endl;
         return 1;
      }
   }
   if (options.printPlan.get()) problem.plan.toDOT(std::cout);
   Simulator sim(&problem);
   auto result = sim.simulate(assignment);
   if (result.done)
      std::cout << result.executionTime << std::endl;
   else
      std::cerr << result.abortMessage << std::endl;
   if (!!options.recordingFile) {
      auto recording = sim.recordSimulation(assignment);
      std::ofstream fileStream(options.recordingFile.get());
      fileStream << recording;
   }
}
//---------------------------------------------------------------------------
