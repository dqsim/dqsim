//---------------------------------------------------------------------------
#include "src/Cluster.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
void Node::write(infra::JSONWriter& out) const
// Write the object
{
   infra::json::IO<Node>::output(out, *this);
}
//---------------------------------------------------------------------------
void Node::read(infra::JSONReader& in, infra::JSONValue value)
// Read the object
{
   infra::json::IO<Node>::input(in, value, *this);
}
//---------------------------------------------------------------------------
double Node::byteBandwidth() const {
   return 1024 * 1024 * 1024 * networkSpeed / 8;
}
//---------------------------------------------------------------------------
void Cluster::read(infra::JSONReader& in, infra::JSONValue value) {
   infra::json::IO<Cluster>::input(in, value, *this);
}
//---------------------------------------------------------------------------
void Cluster::write(infra::JSONWriter& out) const {
   infra::json::IO<Cluster>::output(out, *this);
}
//---------------------------------------------------------------------------
std::span<Node const> Cluster::getComputeNodes() const {
   return std::span{nodes.begin(), storageService ? nodes.end() - 1 : nodes.end()};
}
//---------------------------------------------------------------------------
bool Cluster::isComputeNode(NodeId node) const {
   return node < (getComputeNodes().size());
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<dqsim::Node>::enumEntries(Context& context, dqsim::Node& value) {
   mapRequired(context, "nCores", value.nCores);
   mapRequired(context, "memorySize", value.memorySize);
   mapRequired(context, "networkSpeed", value.networkSpeed);
   mapRequired(context, "networkLatency", value.networkLatency);
}
//---------------------------------------------------------------------------
void IO<dqsim::Cluster>::enumEntries(Context& context, dqsim::Cluster& value) {
   mapRequired(context, "nodes", value.nodes);
   mapRequired(context, "storageService", value.storageService);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
