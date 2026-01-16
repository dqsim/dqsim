//---------------------------------------------------------------------------
#include "src/distributedplan/ProblemWriter.hpp"
#include "src/infra/CommandLine.hpp"
#include "src/infra/JSONReader.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
#include <fstream>
#include <iostream>
#include <memory>
//---------------------------------------------------------------------------
using namespace dqsim;
using namespace dqsim::infra;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
struct Options : public CommandLine {
   Option<bool> help;
   Option<bool> execute;
   Option<std::string> planner;

   Options() : CommandLine("Partition assignment problem writer command line interface", "[options] <problemfile> <outfile>") {
      add(help).longName("help").description("show help");
      add(execute).longName("execute").description("execute the simulator on the result");
      add(planner).longName("planner").description("which planner to use (defaults to round robin)");
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
   {
      std::ifstream problemStream(options.getPositional()[0]);
      JSONReader reader;
      reader.parse(problemStream);
      problem.read(reader, reader.getRoot());
   }

   std::unique_ptr<TaskAssignmentPlanner> planner;
   if (!options.planner) {
      planner = std::make_unique<AlignedRoundRobinPlanner>();
   }

   TaskAssignment assignment = planner->assignTasks(problem);

   // Output
   {
      std::ofstream output(options.getPositional()[2]);
      JSONWriter jsonWriter(output);
      assignment.write(jsonWriter);
   }
}
//---------------------------------------------------------------------------
