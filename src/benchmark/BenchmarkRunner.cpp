#include "src/benchmark/BenchmarkRunner.hpp"
#include "src/Simulator.hpp"
#include "src/benchmark/Handcrafted.hpp"
#include "src/planning/ComponentPlanner.hpp"
#include "src/planning/EnumerationPlanner.hpp"
#include "src/planning/GreedyPlanner.hpp"
#include "src/planning/HEFTPlanner.hpp"
#include "src/planning/NetHEFTPlanner.hpp"
#include "src/planning/PolarisPlanner.hpp"
#include "src/planning/TaskAssignmentPlanner.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <numeric>
#include <thread>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
class ThreadPool {
   std::vector<std::thread> workers;
   std::vector<std::function<void()>> tasks;
   std::mutex queueMutex;
   bool stop = false;

   public:
   ThreadPool(size_t nThreads) {
      for (size_t i = 0; i < nThreads; ++i) {
         workers.emplace_back([this]() {
            while (true) {
               std::function<void()> task;
               {
                  std::unique_lock lock(queueMutex);
                  if (tasks.empty()) {
                     if (stop) {
                        return;
                     }
                     std::this_thread::yield();
                     continue;
                  } else {
                     task = tasks.back();
                     tasks.pop_back();
                  }
               }
               task();
            }
         });
      }
   }

   template <typename F>
   void pushTask(F&& func) {
      std::unique_lock lock(queueMutex);
      tasks.emplace_back(std::forward<F>(func));
   }

   ~ThreadPool() {
      {
         std::unique_lock lock(queueMutex);
         stop = true;
      }
      for (auto& worker : workers) {
         worker.join();
      }
   }
};
//---------------------------------------------------------------------------
BenchmarkResult computeScore(std::string assignmentAlgorithm,
                             const std::vector<AssignmentProblem>& problems,
                             const std::vector<TaskAssignment>& assignments,
                             const std::vector<double>& schedulingTimes,
                             double totalSchedulingTime) {
   BenchmarkResult result;
   result.totalExecutionTime = 0;
   result.totalSchedulingTime = totalSchedulingTime;
   result.executionTimes.resize(problems.size());
   result.schedulingTimes.resize(problems.size());
   std::vector<std::string> names;

   {
      ThreadPool pool(std::thread::hardware_concurrency());
      for (size_t i = 0; i < problems.size(); ++i) {
         pool.pushTask([&problems, &assignments, &schedulingTimes, &result, i]() {
            Simulator simulator(&problems[i]);
            auto simulationResult = simulator.simulate(assignments[i]);
            double currentResult = std::numeric_limits<double>::infinity();
            if (simulationResult.done)
               currentResult = simulationResult.executionTime;
            else
               std::cout << problems[i].name << ": " << simulationResult.abortMessage << std::endl;
            result.executionTimes[i] = currentResult;
            result.schedulingTimes[i] = schedulingTimes[i];
         });
         names.push_back(problems[i].name);
      }
   }
   result.totalExecutionTime = std::accumulate(result.executionTimes.begin(), result.executionTimes.end(), 0.);
   result.benchmarkNames = names;
   auto now = std::chrono::system_clock::now();
   result.date = std::format("{:%FT%TZ}", now);
   result.assignmentAlgorithm = std::move(assignmentAlgorithm);
   return result;
}
//---------------------------------------------------------------------------
std::pair<std::vector<TaskAssignment>, std::vector<double>> computeAssignment(const std::vector<AssignmentProblem>& problems, TaskAssignmentPlanner& planner) {
   std::vector<TaskAssignment> result;
   std::vector<double> schedulingTimes;
   auto t1 = std::chrono::steady_clock::now();
   for (const AssignmentProblem& problem : problems) {
      result.push_back(planner.assignTasks(problem));
      auto t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double, std::micro> duration = t2 - t1;
      schedulingTimes.push_back(duration.count() / 1e6);
      t1 = t2;
   }
   return {result, schedulingTimes};
}
//---------------------------------------------------------------------------
[[maybe_unused]] void recordSimulations(const std::vector<AssignmentProblem>& problems, const std::vector<std::string>& names, const std::vector<TaskAssignment>& assignments) {
   assert(problems.size() == names.size());

   {
      ThreadPool pool(std::thread::hardware_concurrency());
      for (size_t i = 0; i < problems.size(); ++i) {
         pool.pushTask([&problems, &assignments, &names, i]() {
            Simulator simulator(&problems[i]);
            auto recording = simulator.recordSimulation(assignments[i]);
            std::ofstream stream(std::format("recordings/{}.json", names[i]));
            stream.write(recording.data(), recording.size());
         });
      }
   }
}
//---------------------------------------------------------------------------
} // namespace
//---------------------------------------------------------------------------
std::unique_ptr<TaskAssignmentPlanner> BenchmarkRunner::getPlanner(PlannerType type) {
   switch (type) {
      case PlannerType::RR:
         return std::make_unique<RoundRobinPlanner>();
      case PlannerType::ARR:
         return std::make_unique<AlignedRoundRobinPlanner>();
      case PlannerType::HEFT:
         return std::make_unique<HEFTPlanner>();
      case PlannerType::NetHEFT:
         return std::make_unique<NetHEFTPlanner>();
      case PlannerType::Greedy1:
         return std::make_unique<GreedyPlanner1>();
      case PlannerType::Greedy2:
         return std::make_unique<GreedyPlanner2>();
      case PlannerType::Greedy3:
         return std::make_unique<GreedyPlanner3>();
      case PlannerType::Greedy4:
         return std::make_unique<GreedyPlanner4>();
      case PlannerType::CEFT:
         return std::make_unique<ComponentPlanner>();
      case PlannerType::BruteForce:
         return std::make_unique<BruteForcePlanner>(true);
      case PlannerType::Rand1k:
         return std::make_unique<RandomizedPlanner>(1000, true);
      case PlannerType::Rand10k:
         return std::make_unique<RandomizedPlanner>(10000, true);
      case PlannerType::Rand100k:
         return std::make_unique<RandomizedPlanner>(100000, true);
      case PlannerType::StateSep:
         return std::make_unique<PolarisPlanner>(false, true, false);
      case PlannerType::StateSepB:
         return std::make_unique<PolarisPlanner>(false, true, true);
      case PlannerType::StateSepP2P:
         return std::make_unique<PolarisPlanner>(true, true, false);
      case PlannerType::StateSepP2PB:
         return std::make_unique<PolarisPlanner>(true, true, true);
      case PlannerType::StateSepNoUp:
         return std::make_unique<PolarisPlanner>(true, false, false);
      case PlannerType::StateSepNoUpB:
         return std::make_unique<PolarisPlanner>(true, false, true);
   }
   unreachable();
   return nullptr;
}
//---------------------------------------------------------------------------
void BenchmarkRunner::run(const std::vector<AssignmentProblem>& problems, const BenchmarkDefinition& bench) {
   for (const auto& plannerType : bench.planners) {
      auto planner = getPlanner(plannerType);
      std::cout << "Running planner " << planner->getName() << std::endl;

      auto t1 = std::chrono::steady_clock::now();
      auto [assignments, scheduling_times] = computeAssignment(problems, *planner);
      auto t2 = std::chrono::steady_clock::now();
      // writeAssignments("assignments", assignments, names);
      std::chrono::duration<double, std::micro> duration = t2 - t1;
      std::cout << " Evaluating " << problems.size() << " assignments" << std::endl;
      BenchmarkResult score = computeScore(planner->getName(), problems, assignments, scheduling_times, duration.count() / 1e6);
      std::cout << " Scheduling Optimization time: " << duration.count() / 1000 << "ms Simulated Execution Time: " << score.totalExecutionTime << "s" << std::endl;

      auto now = std::chrono::system_clock::now();
      std::string path = std::format("{}/{}/", "benchmark_results", bench.name);
      // Create directory
      std::filesystem::create_directories(path);
      std::ofstream stream(std::format("{}/{}-{:%FT%TZ}.json", path, planner->getName(), now));
      infra::JSONWriter writer(stream);
      score.write(writer);
   }
}
//---------------------------------------------------------------------------
} // namespace dqsim
//---------------------------------------------------------------------------
