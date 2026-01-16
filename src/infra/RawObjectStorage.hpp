#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Config.hpp"
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
/// An aligned storage array that can be used to create uninitialized objects
template <class T>
struct RawObjectStorage {
   /// The backing storage
   alignas(T) std::byte buffer[sizeof(T)];

   /// Access
   T* get() { return reinterpret_cast<T*>(buffer); }
   /// Access
   T* operator->() { return reinterpret_cast<T*>(buffer); }
   /// Access
   T& operator*() { return *reinterpret_cast<T*>(buffer); }
};
//---------------------------------------------------------------------------
/// A RawObjectStorage variant that can store additional data appended to the object
template <class T, uintptr_t extraSize>
struct RawObjectStorageFlex {
   /// The backing storage
   alignas(T) char buffer[sizeof(T)];
   /// Additional storage
   std::byte extra[extraSize];

   /// Access
   T* get() { return reinterpret_cast<T*>(buffer); }
   /// Access
   T* operator->() { return reinterpret_cast<T*>(buffer); }
   /// Access
   T& operator*() { return *reinterpret_cast<T*>(buffer); }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
