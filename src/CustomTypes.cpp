//---------------------------------------------------------------------------
#include "src/CustomTypes.hpp"
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
// void IO<dqsim::DataUnitId>::enumEntries(Context& context, dqsim::DataUnitId& value) {
//    mapRequired(context, "id", value.t);
// }
//---------------------------------------------------------------------------
void IO<dqsim::DataUnitId>::input(infra::JSONReader&, infra::JSONValue in, dqsim::DataUnitId& value) {
   auto id = in.getUInt();
   value = id;
}
//---------------------------------------------------------------------------
void IO<dqsim::DataUnitId>::output(infra::JSONWriter& out, const dqsim::DataUnitId& value) {
   out.writeUInt(value);
}
//---------------------------------------------------------------------------
void IO<dqsim::NodeId>::input(infra::JSONReader&, infra::JSONValue in, dqsim::NodeId& value) {
   auto id = in.getUInt();
   value = id;
}
//---------------------------------------------------------------------------
void IO<dqsim::NodeId>::output(infra::JSONWriter& out, const dqsim::NodeId& value) {
   out.writeUInt(value);
}
//---------------------------------------------------------------------------
void IO<dqsim::PipelineId>::input(infra::JSONReader&, infra::JSONValue in, dqsim::PipelineId& value) {
   auto id = in.getUInt();
   value = id;
}
//---------------------------------------------------------------------------
void IO<dqsim::PipelineId>::output(infra::JSONWriter& out, const dqsim::PipelineId& value) {
   out.writeUInt(value);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
