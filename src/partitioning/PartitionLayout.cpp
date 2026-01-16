#include "PartitionLayout.hpp"
#include <algorithm>
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::PartitionLayout>::enumEntries(Context& context, structType& value) {
   mapRequired(context, "type", value.type);
   mapRequired(context, "nPartitions", value.nPartitions);
   mapOptional(context, "attributes", value.attributes);
}
//---------------------------------------------------------------------------
void IO<dqsim::BaseRelationLayoutDefinition::Layout>::enumEntries(Context& context, dqsim::BaseRelationLayoutDefinition::Layout& value) {
   mapRequired(context, "name", value.name);
   mapRequired(context, "type", value.type);
   mapRequired(context, "nPartitions", value.nPartitions);
   mapRequired(context, "attributes", value.attributes);
}
//---------------------------------------------------------------------------
void IO<dqsim::BaseRelationLayoutDefinition>::enumEntries(Context& context, dqsim::BaseRelationLayoutDefinition& value) {
   mapRequired(context, "layouts", value.layouts);
}
//--------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void BaseRelationLayoutDefinition::read(infra::JSONReader& in, infra::JSONValue value) {
   infra::json::IO<BaseRelationLayoutDefinition>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void BaseRelationLayoutDefinition::write(JSONWriter& out) const {
   infra::json::IO<BaseRelationLayoutDefinition>::output(out, *this);
}
//---------------------------------------------------------------------------
const BaseRelationLayoutDefinition::Layout* BaseRelationLayoutDefinition::getLayout(std::string_view name) const {
   auto layout = std::find_if(layouts.begin(), layouts.end(), [name](const Layout& elem) { return elem.name == name; });
   if (layout == layouts.end()) return nullptr;
   return &*layout;
}
//---------------------------------------------------------------------------
bool PartitionLayout::satisfies(const PartitionLayout& other) const {
   if (type == Type::Broadcast || other.type == Type::Scattered) return true;
   return *this == other;
}
//---------------------------------------------------------------------------
void PartitionLayout::prettyPrint(std::ostream& out) const {
   switch (type) {
      case Type::Scattered: out << "Scattered"; break;
      case Type::HashPartitioned: out << "HashPartitioned"; break;
      case Type::Broadcast: out << "Broadcast"; break;
      case Type::SingleNode: out << "SingleNode"; break;
   }
   if (type == PartitionLayout::Type::HashPartitioned) {
      for (const auto& attribute : attributes) {
         out << " " << attribute.name;
      }
      out << " (" << nPartitions << ")";
   }
}
//---------------------------------------------------------------------------
PartitionLayout::PartitionLayout(PartitionLayout::Type type, std::size_t nPartitions, std::unordered_set<Attribute> attributes)
   : type(type), nPartitions(nPartitions), attributes(std::move(attributes)) {}
//---------------------------------------------------------------------------
PartitionLayout::PartitionLayout() : type(Type::Scattered), nPartitions(0), attributes() {}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
