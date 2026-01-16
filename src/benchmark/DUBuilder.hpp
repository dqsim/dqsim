#pragma once
//---------------------------------------------------------------------------
#include "src/distributedplan/DataUnit.hpp"
#include <span>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
/// Class to easily build data units
class DUBuilder {
   public:
   /// An attribute definition
   struct Attribute {
      /// The name
      std::string name;
      /// The size of a value in bytes
      double size;
   };

   private:
   /// Previously created DUs
   std::vector<DataUnit> dus;

   public:
   /// Create a data unit
   DataUnit buildDU(std::string name, double size, std::size_t nPartition, bool intermediate, std::span<Attribute const> attributes, std::optional<size_t> partitionAttributeIndex, bool makeSingleNode);
   /// Get all data units
   const std::vector<DataUnit>& getDUs() const;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
