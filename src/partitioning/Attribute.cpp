#include "src/partitioning/Attribute.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void Attribute::read(JSONReader& in, JSONValue value) {
   infra::json::IO<Attribute>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void Attribute::write(JSONWriter& out) const {
   infra::json::IO<Attribute>::output(out, *this);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::Attribute>::enumEntries(Context& context, dqsim::Attribute& value) {
   mapRequired(context, "name", value.name);
   mapRequired(context, "dataUnitId", value.dataUnitId);
   mapRequired(context, "attributeIndex", value.attributeIndex);
   mapRequired(context, "size", value.size);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
