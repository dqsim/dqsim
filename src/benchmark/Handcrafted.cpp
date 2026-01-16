#include "src/benchmark/Handcrafted.hpp"
#include "src/benchmark/ClusterBuilder.hpp"
#include "src/benchmark/DUBuilder.hpp"
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
std::vector<AssignmentProblem> Handcrafted::getHandcraftedProblems() {
   std::vector<AssignmentProblem> result;
   result.push_back(getH1());
   result.push_back(getH2());
   return result;
}
//---------------------------------------------------------------------------
AssignmentProblem Handcrafted::getH1() {
   Cluster cluster = ClusterBuilder::fromDefinition(ClusterBuilderDefinition{2, true, {"c5n9xlarge", "c5n18xlarge"}});

   DUBuilder duBuilder;
   DataUnit r1 = duBuilder.buildDU("R1", 1e7, 4, false, {{{"x", 8}}}, 0, false);
   DataUnit r2 = duBuilder.buildDU("R2", 1e7, 4, false, {{{"y", 8}}}, 0, false);
   DataUnit i1 = duBuilder.buildDU("I1", 1e7, 4, true, {{{"x", 8}}}, 0, false);
   DataUnit i2 = duBuilder.buildDU("I2", 1e7, 4, true, {{{"x", 8}}}, 0, false);
   DataUnit result = duBuilder.buildDU("result", 1e7, 1, true, {{{"x", 8}}}, 0, true);

   Pipeline p1{PipelineId{0}, r1.id, i1.id, {}, 1e-9, "build hashtable"};
   Pipeline p2{PipelineId{1}, r2.id, i2.id, {i1.id}, 1e-9, "probe hashtable"};

   ShuffleStage s1{i2.id, result.id};

   DistributedQueryPlan plan{duBuilder.getDUs(), {p1, p2}, {s1}, {PipelineId{0}, PipelineId{1}}};

   size_t nPartitions = 4;
   PartitionManager partitions{cluster};
   // R1 is on node 1
   partitions.registerDU(r1.id, nPartitions, NodeId{0});
   // R2 is only on the storage service
   partitions.registerDU(r2.id, nPartitions, NodeId{2});

   ProblemWriter writer{plan, cluster, "H1", NodeId{0}, 10000};
   writer.loadBaseRelationPartitioning(partitions.getPartitioning());
   writer.buildTasks();
   return writer.getProblem();
}
//---------------------------------------------------------------------------
AssignmentProblem Handcrafted::getH2() {
   Cluster cluster = ClusterBuilder::get2Big();

   DUBuilder duBuilder;
   DataUnit r1 = duBuilder.buildDU("R1", 1e7, 4, false, {{{"x", 8}, {"z", 8}}}, 0, false);
   DataUnit r2 = duBuilder.buildDU("R2", 1e7, 4, false, {{{"y", 8}}}, 0, false);
   DataUnit r3 = duBuilder.buildDU("R3", 1e7, 4, false, {{{"z", 8}}}, 0, false);
   DataUnit i1 = duBuilder.buildDU("I1", 1e7, 4, false, {{{"x", 8}, {"z", 8}}}, 0, false);
   DataUnit i2 = duBuilder.buildDU("I2", 4e7, 4, true, {{{"x", 8}, {"z", 8}}}, 0, false);
   DataUnit i3 = duBuilder.buildDU("I3", 4e7, 4, true, {{{"x", 8}, {"z", 8}}}, 1, false);
   DataUnit i4 = duBuilder.buildDU("I4", 1e7, 4, true, {}, std::nullopt, false);
   DataUnit result = duBuilder.buildDU("result", 1e7, 1, true, {}, std::nullopt, true);

   Pipeline p1{PipelineId{0}, r1.id, i1.id, {}, 1e-9, "build R1Hash"};
   Pipeline p2{PipelineId{1}, r2.id, i2.id, {i1.id}, 1e-9, "probe, build R2Hash"};
   ShuffleStage s1{i2.id, i3.id};
   Pipeline p3{PipelineId{2}, r3.id, i4.id, {i3.id}, 1e-9, "probe R3"};
   ShuffleStage s2{i4.id, result.id};

   size_t nPartitions = 4;
   PartitionManager partitions{cluster};
   // R1 is on node 1 except one partition
   partitions.registerDU(r1.id, nPartitions, NodeId{0});
   partitions.registerPartition(Partition{r1.id, 3}, NodeId{1});
   // R2 is on node 1 except one partition
   partitions.registerDU(r2.id, nPartitions, NodeId{0});
   partitions.registerPartition(Partition{r2.id, 3}, NodeId{1});
   // R3 is completely on node 2
   partitions.registerDU(r3.id, nPartitions, NodeId{1});

   // Everything is cached on storage
   for (const auto& du : {r1.id, r2.id, r3.id}) {
      for (unsigned p : {0, 1, 2, 3}) {
         partitions.cachePartition(Partition{du, p}, NodeId{2});
      }
   }

   DistributedQueryPlan plan{duBuilder.getDUs(), {p1, p2, p3}, {s1, s2}, {PipelineId{0}, PipelineId{1}, PipelineId{2}}};

   ProblemWriter writer{plan, cluster, "H2", NodeId{0}, 10000};
   writer.loadBaseRelationPartitioning(partitions.getPartitioning());
   writer.buildTasks();
   return writer.getProblem();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
