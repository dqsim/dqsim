#include "src/distributedplan/Pipeline.hpp"
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
template <typename T>
bool vectorEquivalence(const std::vector<T>& a, const std::vector<T>& b) {
   if (a.size() != b.size()) return false;
   for (size_t i = 0; i < a.size(); ++i) {
      if (a[i] != b[i]) return false;
   }
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::Pipeline>::enumEntries(Context& context, dqsim::Pipeline& value) {
   mapRequired(context, "id", value.id);
   mapRequired(context, "scan", value.scan);
   mapRequired(context, "output", value.output);
   mapRequired(context, "estimatedLoad", value.estimatedLoad);
   mapRequired(context, "requiredData", value.requiredData);
}
//---------------------------------------------------------------------------
void IO<dqsim::ShuffleStage>::enumEntries(Context& context, dqsim::ShuffleStage& value) {
   mapRequired(context, "from", value.from);
   mapRequired(context, "to", value.to);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
bool Pipeline::operator==(const Pipeline& other) const {
   return id == other.id && output == other.output && scan == other.scan && vectorEquivalence(requiredData, other.requiredData);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
