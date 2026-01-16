#pragma once
//---------------------------------------------------------------------------
#include "src/distributedplan/ProblemWriter.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Collection of methods to build commonly used clusters
class Handcrafted {
   public:
   /// Get all handcrafted problems
   static std::vector<AssignmentProblem> getHandcraftedProblems();
   /// Get problem 1
   static AssignmentProblem getH1();
   /// Get problem 2
   static AssignmentProblem getH2();
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
