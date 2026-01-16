#include "ClusterBuilder.hpp"
#include "src/Util.hpp"
#include <format>
#include <fstream>
#include <sstream>
#include <string>
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
using namespace dqsim;
//---------------------------------------------------------------------------
template <typename T>
T loadFromJsonFile(std::string file)
/// Create an object and read its contents from a json file
{
   T result;
   std::ifstream stream(file);
   infra::JSONReader reader;
   reader.parse(stream);
   result.read(reader, reader.getRoot());
   return result;
}
//---------------------------------------------------------------------------
template <typename T>
T loadFromJson(std::string_view json)
/// Create an object and read its contents from json text
{
   T result;
   std::istringstream stream((std::string(json)));
   infra::JSONReader reader;
   reader.parse(stream);
   result.read(reader, reader.getRoot());
   return result;
}
//---------------------------------------------------------------------------
std::string c5n18xlarge = "{\"nCores\":72, \"memorySize\": 192, \"networkSpeed\":100, \"networkLatency\":2.5e-4}";
std::string c5n9xlarge = "{\"nCores\":36, \"memorySize\": 96, \"networkSpeed\":50, \"networkLatency\":2.5e-4}";
std::string c5nxlarge = "{\"nCores\":4, \"memorySize\": 10.5, \"networkSpeed\":20, \"networkLatency\":2.5e-4}";
std::string bigSlowNetwork = "{\"nCores\":72, \"memorySize\": 192, \"networkSpeed\":1, \"networkLatency\":2.5e-4}";
std::string smallFastNetwork = "{\"nCores\":4, \"memorySize\": 32, \"networkSpeed\":100, \"networkLatency\":2.5e-4}";
std::string small = "{\"nCores\":4, \"memorySize\": 32, \"networkSpeed\":1, \"networkLatency\":2.5e-4}";
std::string blobstore = "{\"nCores\":0, \"memorySize\": 1000000, \"networkSpeed\":1000000, \"networkLatency\":0.1}";
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
Node ClusterBuilder::get18xlarge() {
   return loadFromJson<Node>(c5n18xlarge);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::get9xlarge() {
   return loadFromJson<Node>(c5n9xlarge);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getxlarge() {
   return loadFromJson<Node>(c5nxlarge);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getBigSlowNetwork() {
   return loadFromJson<Node>(bigSlowNetwork);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getSmallFastNetwork() {
   return loadFromJson<Node>(smallFastNetwork);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getSmall() {
   return loadFromJson<Node>(small);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getStorageNode() {
   return loadFromJson<Node>(blobstore);
}
//---------------------------------------------------------------------------
Node ClusterBuilder::getNodeByName(std::string_view nodeName) {
   // Open the nodeTypes json
   std::string path = "src/benchmark/nodeTypes.json";
   std::ifstream stream(path);
   infra::JSONReader reader;

   if (!reader.parse(stream)) {
      throw std::runtime_error(std::format("Failed to parse nodeTypes.json: {}", path));
   }

   auto root = reader.getRoot();
   auto obj = root.getObject();
   auto nodeValue = obj.getEntry(nodeName, true);

   Node result;
   result.read(reader, nodeValue);
   return result;
}
//---------------------------------------------------------------------------
Cluster ClusterBuilder::getCluster(Node node, size_t n, bool storageService) {
   std::vector<Node> nodes(n, node);
   if (storageService) {
      nodes.emplace_back(getStorageNode());
   }
   return Cluster{{nodes}, storageService};
}
//---------------------------------------------------------------------------
Cluster ClusterBuilder::fromDefinition(ClusterBuilderDefinition definition) {
   std::vector<Node> nodes;
   nodes.reserve(definition.nNodes + (definition.storageService ? 1 : 0));

   size_t nodesPerType = definition.nNodes / definition.nodeTypes.size();

   for (const auto& type : definition.nodeTypes) {
      Node node = getNodeByName(type);
      for (size_t i = 0; i < nodesPerType; ++i) {
         nodes.push_back(node);
      }
   }

   // We fill up with the last node type
   Node node = getNodeByName(definition.nodeTypes.back());
   while (nodes.size() < definition.nNodes) {
      nodes.push_back(node);
   }

   if (definition.storageService) {
      nodes.push_back(getNodeByName("storage"));
   }

   return Cluster{{nodes}, definition.storageService};
}
//---------------------------------------------------------------------------
std::string ClusterBuilderDefinition::getName() const {
   std::string typeName;
   for (const auto& type : nodeTypes) {
      typeName += type;
      typeName += "-";
   }
   typeName.pop_back();
   return std::format("{}x{}", typeName, nNodes);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
void IO<ClusterBuilderDefinition>::enumEntries(Context& context, ClusterBuilderDefinition& value) {
   mapRequired(context, "nNodes", value.nNodes);
   mapRequired(context, "storageService", value.storageService);
   mapRequired(context, "nodeTypes", value.nodeTypes);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
