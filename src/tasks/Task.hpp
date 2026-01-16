#pragma once
//---------------------------------------------------------------------------
#include "src/CustomTypes.hpp"
#include "src/infra/JSONMapping.hpp"
#include "src/infra/SmallVector.hpp"
#include "src/partitioning/Partition.hpp"
#include <unordered_set>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// A task consists of the executed pipeline and the scanned partition
struct Task {
   /// The pipeline that is executed in this task
   PipelineId pipeline;
   /// The scanned partition the pipeline is executed on
   Partition scannedPartition;
   /// Partitions which need to be present to execute the task
   infra::SmallVector<Partition, 4> requiredPartitions;
   /// The result of the task
   Partition outputPartition;
   /// Flag for identical tasks that need to be started on all nodes
   bool broadcast;

   /// Constructor
   Task(PipelineId pipelineId, Partition scannedPartition, std::vector<Partition> requiredPartitions, Partition outputPartition, bool broadcast);
   /// Constructor
   Task() = default;
   /// Equality
   bool operator==(const Task& other) const = default;

   /// Read the object
   void read(infra::JSONReader& in, infra::JSONValue value);
   /// Write the object to json
   void write(infra::JSONWriter& out) const;

   /// Iterator for all input partitions
   class PartitionIterator {
      public:
      const Task* t;
      size_t i;
      PartitionIterator(const Task* t, size_t i) : t(t), i(i) {}
      Partition operator*() const {
         return (i == 0) ? t->outputPartition : (i == 1) ? t->scannedPartition :
                                                           t->requiredPartitions[i - 2];
      }
      PartitionIterator& operator++() {
         ++i;
         return *this;
      }
      bool operator!=(const PartitionIterator& other) const { return i != other.i; }
   };

   class PartitionRange {
      public:
      const Task* t;
      size_t iStart;
      PartitionRange(const Task* t, size_t iStart) : t(t), iStart(iStart) {}
      PartitionIterator begin() const { return PartitionIterator(t, iStart); }
      PartitionIterator end() const { return PartitionIterator(t, 2 + t->requiredPartitions.size()); }
   };

   // Returns an iterable range for use with range-based for loops.
   PartitionRange getPartitions() const { return PartitionRange(this, 0); }
   PartitionRange getInputPartitions() const { return PartitionRange(this, 1); }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
//---------------------------------------------------------------------------
template <>
struct IO<dqsim::Task> : public StructMapper<dqsim::Task> {
   static void enumEntries(Context& context, structType& value);
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace std {
//---------------------------------------------------------------------------
template <>
struct hash<dqsim::Task> {
   size_t operator()(dqsim::Task const& task) const noexcept {
      size_t seed = 0;
      size_t h1 = hash<dqsim::PipelineId>{}(task.pipeline);
      size_t h2 = hash<dqsim::Partition>{}(task.scannedPartition);
      boost::hash_combine(seed, h1);
      boost::hash_combine(seed, h2);
      return seed;
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
