#include "src/partitioning/Partition.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void infra::json::IO<Partition>::enumEntries(Context& context, Partition& value) {
   mapRequired(context, "dataUnitId", value.dataUnitId);
   mapRequired(context, "partitionIndex", value.partitionIndex);
}
//---------------------------------------------------------------------------
void infra::json::IO<LocatedPartition>::enumEntries(Context& context, LocatedPartition& value) {
   mapRequired(context, "Partition", value.partition);
   mapRequired(context, "Node", value.node);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
