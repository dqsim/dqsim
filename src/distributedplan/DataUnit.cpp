//---------------------------------------------------------------------------
#include "src/distributedplan/DataUnit.hpp"
#include <string_view>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void DataUnit::read(JSONReader& in, JSONValue value) {
   json::IO<DataUnit>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void DataUnit::write(JSONWriter& out) const {
   json::IO<DataUnit>::output(out, *this);
}
//---------------------------------------------------------------------------
double DataUnit::tupleSize() const {
   double result = 0;
   for (const Attribute& attr : attributes) {
      result += attr.size;
   }
   return result;
}
//---------------------------------------------------------------------------
double DataUnit::partitionSize() const {
   assert(partitionLayout);
   return tupleSize() * estimatedCard / partitionLayout->nPartitions;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::DataUnit>::enumEntries(Context& context, dqsim::DataUnit& value) {
   mapRequired(context, "name", value.name);
   mapRequired(context, "id", value.id);
   mapRequired(context, "estimatedCard", value.estimatedCard);
   mapRequired(context, "isIntermediate", value.isIntermediate);
   mapRequired(context, "attributes", value.attributes);
   mapOptional(context, "partitionLayout", value.partitionLayout);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
