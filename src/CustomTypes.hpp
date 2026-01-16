#pragma once
//---------------------------------------------------------------------------
#include "src/infra/JSONMapping.hpp"
#include <boost/functional/hash.hpp>
#include <boost/serialization/strong_typedef.hpp>
#include <cstdint>
//---------------------------------------------------------------------------
namespace dqsim {
//---------------------------------------------------------------------------
// using DataUnitId = uint64_t;
BOOST_STRONG_TYPEDEF(uint64_t, DataUnitId);
// using NodeId = uint64_t;
BOOST_STRONG_TYPEDEF(uint64_t, NodeId);
// using PipelineId = uint64_t;
BOOST_STRONG_TYPEDEF(uint64_t, PipelineId);
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
namespace dqsim::infra::json {
};
//---------------------------------------------------------------------------
template <>
struct dqsim::infra::json::IO<dqsim::DataUnitId> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, dqsim::DataUnitId& value);
   static void output(infra::JSONWriter& out, const dqsim::DataUnitId& value);
};
//---------------------------------------------------------------------------
template <>
struct dqsim::infra::json::IO<dqsim::NodeId> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, dqsim::NodeId& value);
   static void output(infra::JSONWriter& out, const dqsim::NodeId& value);
};
//---------------------------------------------------------------------------
template <>
struct dqsim::infra::json::IO<dqsim::PipelineId> {
   static void input(infra::JSONReader& reader, infra::JSONValue in, dqsim::PipelineId& value);
   static void output(infra::JSONWriter& out, const dqsim::PipelineId& value);
};
//---------------------------------------------------------------------------
namespace std {
//---------------------------------------------------------------------------
template <>
struct hash<dqsim::DataUnitId> {
   size_t operator()(dqsim::DataUnitId const& x) const noexcept {
      return hash<uint64_t>{}(x);
   }
};
//---------------------------------------------------------------------------
template <>
struct hash<dqsim::NodeId> {
   size_t operator()(dqsim::NodeId const& x) const noexcept {
      return hash<uint64_t>{}(x);
   }
};
//---------------------------------------------------------------------------
template <>
struct hash<dqsim::PipelineId> {
   size_t operator()(dqsim::PipelineId const& dataUnitId) const noexcept {
      return hash<uint64_t>{}(dataUnitId);
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
