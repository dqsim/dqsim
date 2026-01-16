#include "src/benchmark/BenchmarkDefinition.hpp"
#include "src/benchmark/BenchmarkRunner.hpp"
#include "src/infra/CommandLine.hpp"
#include "src/infra/JSONReader.hpp"
#include <filesystem>
#include <fstream>
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
struct Options : public CommandLine {
   Option<bool> help;

   Options() : CommandLine("Benchmark runner command line interface", "<benchmarkdefinitionfile> ") {
      add(help).longName("help").description("show help");
   }
};
//---------------------------------------------------------------------------
std::vector<AssignmentProblem> loadProblemsFromDirectory(const std::filesystem::path& dir) {
   if (!std::filesystem::exists(dir)) {
      throw std::runtime_error("Directory does not exist: " + dir.string());
   }
   if (!std::filesystem::is_directory(dir)) {
      throw std::runtime_error("Path is not a directory: " + dir.string());
   }

   std::vector<AssignmentProblem> result;
   for (const auto& entry : std::filesystem::directory_iterator(dir)) {
      if (!entry.is_regular_file()) continue;
      const auto& path = entry.path();
      if (path.extension() != ".json") continue;

      std::ifstream input(path);
      if (!input.is_open()) {
         throw std::runtime_error("Failed to open problem file: " + path.string());
      }
      infra::JSONReader reader;
      reader.parse(input);
      AssignmentProblem p;
      p.read(reader, reader.getRoot());
      result.push_back(std::move(p));
   }

   return result;
}
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
   auto bench = BenchmarkDefinition::loadBenchmarkFromFile(options.getPositional()[0]);
   std::string benchmarkDirectory = std::format("problems/{}", bench.name);
   std::vector<AssignmentProblem> problems = loadProblemsFromDirectory(benchmarkDirectory);
   // Run the benchmarks
   BenchmarkRunner::run(problems, bench);
   return 0;
}
//---------------------------------------------------------------------------
