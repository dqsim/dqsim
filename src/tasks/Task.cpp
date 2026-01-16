#include "src/tasks/Task.hpp"
#include "src/distributedplan/ProblemWriter.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
dqsim::Task::Task(
   PipelineId pipelineId, Partition scannedPartition, std::vector<Partition> requiredPartitions, Partition outputPartition, bool broadcast)
   : pipeline(pipelineId), scannedPartition(scannedPartition), requiredPartitions(infra::SmallVector<Partition, 4>(requiredPartitions.begin(), requiredPartitions.end())), outputPartition(outputPartition), broadcast(broadcast) {}
//---------------------------------------------------------------------------
void Task::read(infra::JSONReader& in, infra::JSONValue value) {
   infra::json::IO<Task>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void Task::write(infra::JSONWriter& out) const {
   infra::json::IO<Task>::output(out, *this);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::Task>::enumEntries(Context& context, dqsim::Task& value) {
   mapRequired(context, "pipelineId", value.pipeline);
   mapRequired(context, "scannedPartition", value.scannedPartition);
   mapRequired(context, "requiredPartitions", value.requiredPartitions);
   mapRequired(context, "outputPartition", value.outputPartition);
   mapRequired(context, "broadcast", value.broadcast);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
