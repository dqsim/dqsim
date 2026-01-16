#pragma once
//---------------------------------------------------------------------------
#include "src/infra/RawObjectStorage.hpp"
#include <algorithm>
#include <compare>
#include <concepts>
#include <memory>
#include <type_traits>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
//---------------------------------------------------------------------------
namespace smallvectorimpl {
//---------------------------------------------------------------------------
/// Report a failed allocation
[[noreturn]] void reportBadAlloc();
/// Report an out-of-range access
[[noreturn]] void reportOutOfRange();
//---------------------------------------------------------------------------
/// Fixed sized buffer
template <class T, unsigned fixedCapacity>
struct SmallVectorBuffer {
   /// The buffer
   RawObjectStorage<T> buffer[fixedCapacity];
};
//---------------------------------------------------------------------------
/// Fixed sized buffer. Specialization for capacity 0
template <class T>
struct SmallVectorBuffer<T, 0> {
};
//---------------------------------------------------------------------------
/// A pointer/allocator pair
template <class T, class Allocator>
struct PointerAllocatorPair {
   /// The pointer
   T* ptr;
   /// The allocator (if any)
   [[no_unique_address]] Allocator alloc;

   /// Constructor
   PointerAllocatorPair(T* ptr, const Allocator& alloc) : ptr(ptr), alloc(alloc) {}

   /// Get the allocator
   Allocator& getAlloc() noexcept { return alloc; }
   /// Get the allocator
   const Allocator& getAlloc() const noexcept { return alloc; }
   /// Set the allocator
   void setAlloc(const Allocator& alloc) { getAlloc() = alloc; }
};
//---------------------------------------------------------------------------
/// Base class that just contains the members. We need that separate here in order to get a complete class for computations
template <class T, class Allocator>
class SmallVectorMembers {
   protected:
   /// The data pointer
   PointerAllocatorPair<T, Allocator> mem;
   /// Size and capacity
   unsigned size, capacity;

   /// Constructor
   SmallVectorMembers(T* inlineStorage, const Allocator& alloc, unsigned fixedCapacity) : mem(inlineStorage, alloc), size(0), capacity(fixedCapacity) {}
};
//---------------------------------------------------------------------------
/// Declaration of helper class
template <class T, bool trivial>
struct ObjectMovementImpl {
};
//---------------------------------------------------------------------------
/// Object movement for trivial objects
template <class T>
struct ObjectMovementImpl<T, true> {
   /// Move
   static void uninitializedMove(T* target, const T* source, uintptr_t count) noexcept { __builtin_memcpy(reinterpret_cast<char*>(target), source, sizeof(T) * count); }
   /// Move
   static void initializedMove(T* target, const T* source, uintptr_t count) noexcept { __builtin_memmove(reinterpret_cast<char*>(target), source, sizeof(T) * count); }
};
//---------------------------------------------------------------------------
/// Object movement for non-trivial objects
template <class T>
struct ObjectMovementImpl<T, false> {
   /// Move
   static void uninitializedMove(T* target, T* source, uintptr_t count) noexcept { std::uninitialized_move_n(source, count, target); }
   /// Move
   static void initializedMove(T* target, T* source, uintptr_t count) {
      if (target < source) {
         for (auto limit = source + count; source != limit;)
            *(target++) = std::move(*source++);
      } else if (target > source) {
         auto limit = source;
         source += count;
         target += count;
         while (source != limit)
            *(--target) = std::move(*(--source));
      }
   }
};
//---------------------------------------------------------------------------
/// Dispatcher for object movement
template <class T>
struct ObjectMovement : public ObjectMovementImpl<T, std::is_trivially_copyable<T>::value> {
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
/// A small vector implementation. This is the capacity-independent logic, the real class is defined in SmallVector
template <class T, class Allocator = std::allocator<T>>
class SmallVectorBase : private smallvectorimpl::SmallVectorMembers<T, Allocator> {
   private:
   using SmallVectorMembers = smallvectorimpl::SmallVectorMembers<T, Allocator>;

   /// The size of the fixed header
   static constexpr unsigned fixedHeaderSize = ((sizeof(SmallVectorMembers) + (alignof(T) - 1)) / alignof(T)) * alignof(T);

   /// Get the inline content
   T* getInlineContent() noexcept { return reinterpret_cast<T*>(reinterpret_cast<char*>(this) + fixedHeaderSize); }
   /// Get the inline content
   const T* getInlineContent() const noexcept { return reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) + fixedHeaderSize); }
   /// Do we currently use the inline storage?
   bool isInline() const noexcept { return SmallVectorMembers::mem.ptr == getInlineContent(); }

   public:
   using value_type = T;
   using allocator_type = Allocator;
   using size_type = unsigned;
   using difference_type = int;
   using reference = value_type&;
   using const_reference = const value_type&;
   using pointer = T*;
   using const_pointer = const T*;
   using iterator = pointer;
   using const_iterator = const_pointer;
   using reverse_iterator = std::reverse_iterator<iterator>;
   using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   protected:
   /// Constructor
   SmallVectorBase(const Allocator& alloc, unsigned fixedCapacity) : SmallVectorMembers(getInlineContent(), alloc, fixedCapacity) {}
   /// Destructor
   ~SmallVectorBase();

   /// Increase the capacity
   void growCapacity(uint64_t newCapacity);
   /// Shrink to fit
   void shrinkToFit(unsigned fixedCapacity);
   /// Prepare for emplace
   T* emplaceImpl(const_iterator pos);
   /// The the allocator
   void setAlloc(const Allocator& alloc) { SmallVectorMembers::mem.setAlloc(alloc); }
   /// Swap two vectors. The inline capacity must be identical
   void swapImpl(SmallVectorBase& other) noexcept;
   /// Implement a move. That target must be empty and inline
   void moveSame(SmallVectorBase&& other) noexcept;
   /// Implement a move. That target must be empty and inline
   void moveOther(SmallVectorBase&& other);

   public:
   /// Assignment
   void assign(size_type count, const T& value);
   /// Assignment
   template <class InputIt>
   void assign(InputIt first, InputIt last);
   /// Assignment
   void assign(std::initializer_list<T> v) { assign(v.begin(), v.end()); }
   /// Get the associate allocator
   allocator_type get_allocator() const noexcept { return SmallVectorMembers::mem.getAlloc(); }

   /// Access element with bounds check
   reference at(size_type pos) {
      if (pos >= SmallVectorMembers::size) smallvectorimpl::reportOutOfRange();
      return data()[pos];
   }
   /// Access element with bounds check
   const_reference at(size_type pos) const {
      if (pos >= SmallVectorMembers::size) smallvectorimpl::reportOutOfRange();
      return data()[pos];
   }
   /// Access element
   reference operator[](size_type pos) noexcept {
      assert(pos < SmallVectorMembers::size);
      return data()[pos];
   }
   /// Access element
   const_reference operator[](size_type pos) const noexcept {
      assert(pos < SmallVectorMembers::size);
      return data()[pos];
   }
   /// The first element
   reference front() noexcept {
      assert(0 < SmallVectorMembers::size);
      return data()[0];
   }
   /// The first element
   const_reference front() const noexcept {
      assert(0 < SmallVectorMembers::size);
      return data()[0];
   }
   /// The last element
   reference back() noexcept {
      assert(0 < SmallVectorMembers::size);
      return data()[SmallVectorMembers::size - 1];
   }
   /// The last element
   const_reference back() const noexcept {
      assert(0 < SmallVectorMembers::size);
      return data()[SmallVectorMembers::size - 1];
   }
   /// The content
   T* data() noexcept { return SmallVectorMembers::mem.ptr; }
   /// The content
   const T* data() const noexcept { return SmallVectorMembers::mem.ptr; }

   /// Iterator on the first element
   iterator begin() noexcept { return data(); }
   /// Iterator on the first element
   const_iterator begin() const noexcept { return data(); }
   /// Iterator on the first element
   const_iterator cbegin() const noexcept { return data(); }
   /// Iterator behind the last element
   iterator end() noexcept { return data() + size(); }
   /// Iterator behind the last element
   const_iterator end() const noexcept { return data() + size(); }
   /// Iterator behind the last element
   const_iterator cend() const noexcept { return data() + size(); }
   /// Iterator on the first element in reverse
   reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }
   /// Iterator on the first element in reverse
   const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }
   /// Iterator on the first element in reverse
   const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator(cend()); }
   /// Iterator behind the last element in reverse
   reverse_iterator rend() noexcept { return reverse_iterator(begin()); }
   /// Iterator behind the last element in reverse
   const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }
   /// Iterator behind the last element in reverse
   const_reverse_iterator crend() const noexcept { return const_reverse_iterator(cbegin()); }

   /// Is the vector empty?
   bool empty() const noexcept { return !size(); }
   /// The size of the vector
   size_type size() const noexcept { return SmallVectorMembers::size; }
   /// The maximum size
   size_type max_size() const noexcept { return ~static_cast<size_type>(0); }
   /// Reserve space
   void reserve(size_type new_cap) {
      if (new_cap > SmallVectorMembers::capacity) growCapacity(new_cap);
   }
   /// Get the capacity
   size_type capacity() const noexcept { return SmallVectorMembers::capacity; }

   /// Remove all elements
   CLANG_REINITIALIZES void clear() noexcept;
   /// Insert at a certain position
   iterator insert(const_iterator pos, const T& value);
   /// Insert at a certain position
   iterator insert(const_iterator pos, T&& value);
   /// Insert multiple values at a certain position
   iterator insert(const_iterator pos, size_type count, const T& value);
   /// Insert values at a certain position
   template <class InputIt>
   iterator insert(const_iterator pos, InputIt first, InputIt last);
   /// Insert values at a certain position
   iterator insert(const_iterator pos, std::initializer_list<T> ilist) { return insert(pos, ilist.begin(), ilist.end()); }
   /// Insert and construct value at a certain position
   template <class... Args>
   iterator emplace(const_iterator pos, Args&&... args) { std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), emplaceImpl(pos), std::forward(args)...); }
   /// Erase an element
   iterator erase(const_iterator pos);
   /// Erase a range
   iterator erase(const_iterator first, const_iterator last);
   /// Append
   inline void push_back(const T& value);
   /// Append
   inline void push_back(T&& value);
   /// Construct at the end
   template <class... Args>
   inline reference emplace_back(Args&&... args);
   /// Construct at the end when we know that the vector will not grow
   template <class... Args>
   inline reference emplace_back_no_alloc(Args&&... args);
   /// Remove the last element
   inline void pop_back();
   /// Resize the vector
   void resize(size_t count);
   /// Resize the vector
   void resize(size_t count, const value_type& value);

   /// Equality operator
   bool operator==(const SmallVectorBase& other) const
      requires std::equality_comparable<T>
   {
      return std::equal(begin(), end(), other.begin(), other.end());
   }

   /// Three-way comparison
   auto operator<=>(const SmallVectorBase& other) const
      requires std::three_way_comparable<T>
   {
      return std::lexicographical_compare_three_way(begin(), end(), other.begin(), other.end());
   }
};
//---------------------------------------------------------------------------
template <class T, class Allocator>
SmallVectorBase<T, Allocator>::~SmallVectorBase()
// Destructor
{
   std::destroy_n(data(), size());
   if (!isInline()) SmallVectorMembers::mem.getAlloc().deallocate(SmallVectorMembers::mem.ptr, SmallVectorMembers::capacity);
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::growCapacity(uint64_t newCapacity)
// Grow the capacity
{
   newCapacity = std::max(newCapacity, std::max<uint64_t>(static_cast<uint64_t>(SmallVectorMembers::capacity) + 2, static_cast<uint64_t>(SmallVectorMembers::capacity) + (SmallVectorMembers::capacity / 4)));
   if (newCapacity >> 32) smallvectorimpl::reportBadAlloc();

   T* newBuffer = SmallVectorMembers::mem.getAlloc().allocate(newCapacity);
   smallvectorimpl::ObjectMovement<T>::uninitializedMove(newBuffer, SmallVectorMembers::mem.ptr, SmallVectorMembers::size);
   if (!isInline())
      SmallVectorMembers::mem.getAlloc().deallocate(SmallVectorMembers::mem.ptr, SmallVectorMembers::capacity);
   SmallVectorMembers::mem.ptr = newBuffer;
   SmallVectorMembers::capacity = static_cast<size_type>(newCapacity);
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::swapImpl(SmallVectorBase& other) noexcept
// Swap two vectors. The inline capacity must be identical
{
   if (&other == this) return;

   unsigned s1 = SmallVectorBase::size(), s2 = other.SmallVectorBase::size(), common = std::min(s1, s2);
   auto d1 = SmallVectorMembers::mem.ptr, d2 = other.SmallVectorMembers::mem.ptr;
   if (this->isInline()) {
      if (other.isInline()) {
         for (unsigned index = 0; index != common; ++index)
            std::swap(d1[index], d2[index]);
         smallvectorimpl::ObjectMovement<T>::uninitializedMove(d2 + common, d1 + common, s1 - common);
         smallvectorimpl::ObjectMovement<T>::uninitializedMove(d1 + common, d2 + common, s2 - common);
      } else {
         smallvectorimpl::ObjectMovement<T>::uninitializedMove(other.getInlineContent(), d1, s1);
         SmallVectorMembers::mem.ptr = other.SmallVectorMembers::mem.ptr;
         other.SmallVectorMembers::mem.ptr = other.getInlineContent();
      }
   } else {
      if (other.isInline()) {
         smallvectorimpl::ObjectMovement<T>::uninitializedMove(this->getInlineContent(), d2, s2);
         other.SmallVectorMembers::mem.ptr = SmallVectorMembers::mem.ptr;
         SmallVectorMembers::mem.ptr = this->getInlineContent();
      } else {
         std::swap(SmallVectorMembers::mem.ptr, other.SmallVectorMembers::mem.ptr);
      }
   }
   std::swap(SmallVectorMembers::size, other.SmallVectorMembers::size);
   std::swap(SmallVectorMembers::capacity, other.SmallVectorMembers::capacity);
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::moveSame(SmallVectorBase&& other) noexcept
// Implement a move. The target must be empty and inline
{
   if (&other == this) return;

   if (other.isInline()) {
      smallvectorimpl::ObjectMovement<T>::uninitializedMove(data(), other.data(), other.SmallVectorMembers::size);
      SmallVectorMembers::size = other.SmallVectorMembers::size;
      other.SmallVectorMembers::size = 0;
   } else {
      SmallVectorMembers::mem.ptr = other.SmallVectorMembers::mem.ptr;
      other.SmallVectorMembers::mem.ptr = other.getInlineContent();
      SmallVectorMembers::size = other.SmallVectorMembers::size;
      other.SmallVectorMembers::size = 0;
      std::swap(SmallVectorMembers::capacity, other.SmallVectorMembers::capacity);
   }
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::moveOther(SmallVectorBase&& other)
// Implement a move. The target must be empty and inline
{
   if (&other == this) return;

   if (other.size() > SmallVectorMembers::capacity)
      growCapacity(other.size());
   smallvectorimpl::ObjectMovement<T>::uninitializedMove(data(), other.data(), other.SmallVectorMembers::size);
   SmallVectorMembers::size = other.SmallVectorMembers::size;
   other.SmallVectorMembers::size = 0;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::assign(size_type count, const T& value)
// Assignment
{
   if (count > SmallVectorMembers::capacity)
      growCapacity(count);

   for (auto iter = this->data(), limit = iter + std::min(SmallVectorMembers::size, count); iter != limit; ++iter)
      *iter = value;
   if (count > SmallVectorMembers::size)
      std::uninitialized_fill(this->data() + SmallVectorMembers::size, this->data() + count, value);
   else
      std::destroy(this->data() + count, this->data() + SmallVectorMembers::size);

   SmallVectorMembers::size = count;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
template <class InputIt>
void SmallVectorBase<T, Allocator>::assign(InputIt first, InputIt last)
// Assignment
{
   uint64_t count = std::distance(first, last);
   if (count > SmallVectorMembers::capacity)
      growCapacity(count);

   auto writer = this->data();
   for (auto remaining = std::min<unsigned>(SmallVectorMembers::size, count); remaining; --remaining)
      *(writer++) = *(first++);
   if (count > SmallVectorMembers::size)
      std::uninitialized_copy(first, last, writer);
   else
      std::destroy(writer, this->data() + SmallVectorMembers::size);

   SmallVectorMembers::size = count;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::shrinkToFit(unsigned fixedCapacity)
// Shrink to fit
{
   // Do we have to do something?
   if ((SmallVectorMembers::capacity > SmallVectorMembers::size) && (SmallVectorMembers::capacity > fixedCapacity)) {
      // Allocate or use the target space
      T* newBuffer = (SmallVectorMembers::size > fixedCapacity) ? SmallVectorMembers::mem.getAlloc().allocate(SmallVectorMembers::size) : getInlineContent();

      // Move elements
      smallvectorimpl::ObjectMovement<T>::uninitializedMove(newBuffer, SmallVectorMembers::mem.ptr, SmallVectorMembers::size);

      /// Release current memory
      SmallVectorMembers::mem.getAlloc().deallocate(SmallVectorMembers::mem.ptr, SmallVectorMembers::capacity);
      SmallVectorMembers::mem.ptr = newBuffer;
      SmallVectorMembers::capacity = (SmallVectorMembers::size > fixedCapacity) ? SmallVectorMembers::size : fixedCapacity;
   }
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::clear() noexcept
// Remove all elements
{
   std::destroy_n(data(), size());
   SmallVectorMembers::size = 0;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::insert(const_iterator pos, const T& value)
// Insert at a certain position
{
   uint64_t ofs = pos - begin();
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   if (ofs < s) {
      std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::move(d[s - 1]));
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs + 1, d + ofs, s - ofs - 1);
      d[ofs] = value;
   } else {
      assert(ofs == s);
      std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, value);
   }
   SmallVectorMembers::size++;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::insert(const_iterator pos, T&& value)
// Insert at a certain position
{
   uint64_t ofs = pos - begin();
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   if (ofs < s) {
      std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::move(d[s - 1]));
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs + 1, d + ofs, s - ofs - 1);
      d[ofs] = std::move(value);
   } else {
      assert(ofs == s);
      std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::move(value));
   }
   SmallVectorMembers::size++;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::insert(const_iterator pos, size_type count, const T& value) {
   uint64_t ofs = pos - begin();
   if (!count) {
      assert(ofs <= size());
      return begin() + ofs;
   }

   uint64_t s = size(), c = capacity();
   if (s + count > c)
      growCapacity(s + count);

   auto d = data();
   if (ofs < s) {
      uint64_t n = std::min<uint64_t>(count, s - ofs);
      smallvectorimpl::ObjectMovement<T>::uninitializedMove(d + s + count - n, d + s - n, n);
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs + count, d + ofs, s - ofs - n);
      for (unsigned index = 0; index != count; ++index)
         d[ofs + index] = value;
   } else {
      assert(ofs == s);
      std::uninitialized_fill(d + s, d + s + count, value);
   }
   SmallVectorMembers::size += count;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
template <class InputIt>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::insert(const_iterator pos, InputIt first, InputIt last)
// Insert values at a certain position
{
   uint64_t count = last - first;
   if (count >> 32) smallvectorimpl::reportBadAlloc();

   uint64_t ofs = pos - begin();
   uint64_t s = size(), c = capacity();
   if (s + count > c)
      growCapacity(s + count);

   auto d = data();
   if (ofs < s) {
      uint64_t n = std::min<uint64_t>(count, s - ofs);
      smallvectorimpl::ObjectMovement<T>::uninitializedMove(d + s + count - n, d + s - n, n);
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs + count, d + ofs, s - ofs - n);
      for (unsigned index = 0; index != count; ++index)
         d[ofs + index] = *(first++);
   } else {
      assert(ofs == s);
      std::uninitialized_copy_n(first, count, d + s);
   }
   SmallVectorMembers::size += count;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
T* SmallVectorBase<T, Allocator>::emplaceImpl(const_iterator pos)
// Prepare for emplace
{
   uint64_t ofs = pos - begin();
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   if (ofs < s) {
      std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::move(d[s - 1]));
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs + 1, d + ofs, s - ofs - 1);
   } else {
      assert(ofs == s);
   }
   SmallVectorMembers::size++;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::erase(const_iterator pos)
// Erase an element
{
   uint64_t ofs = pos - begin();
   uint64_t s = size();
   assert(ofs < s);

   auto d = data();
   if (ofs + 1 < s)
      smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs, d + ofs + 1, s - ofs - 1);
   std::destroy_at(d + s - 1);
   SmallVectorMembers::size--;

   return d + ofs;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
typename SmallVectorBase<T, Allocator>::iterator SmallVectorBase<T, Allocator>::erase(const_iterator first, const_iterator last)
// Erase a range
{
   uint64_t ofs = first - begin();
   if (first != last) {
      uint64_t count = last - first;
      uint64_t s = size();
      assert(ofs + count <= s);

      auto d = data();
      if (ofs + count < s) {
         smallvectorimpl::ObjectMovement<T>::initializedMove(d + ofs, d + ofs + count, s - ofs - count);
      }
      std::destroy_n(d + s - count, count);
      SmallVectorMembers::size -= count;

      return d + ofs;
   } else {
      return begin() + ofs;
   }
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::push_back(const T& value)
// Append
{
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, value);
   SmallVectorMembers::size++;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::push_back(T&& value)
// Append
{
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::move(value));
   SmallVectorMembers::size++;
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
template <class... Args>
typename SmallVectorBase<T, Allocator>::reference SmallVectorBase<T, Allocator>::emplace_back(Args&&... value)
// Construct at the end
{
   uint64_t s = size(), c = capacity();
   if (s == c)
      growCapacity(s + 1);

   auto d = data();
   std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::forward<Args>(value)...);
   return d[SmallVectorMembers::size++];
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
template <class... Args>
typename SmallVectorBase<T, Allocator>::reference SmallVectorBase<T, Allocator>::emplace_back_no_alloc(Args&&... value)
// Construct at the end when we know that the vector will not allocate
{
   uint64_t s = size(), c = capacity();
   if (s == c)
      smallvectorimpl::reportOutOfRange();

   auto d = data();
   std::allocator_traits<Allocator>::construct(SmallVectorMembers::mem.getAlloc(), d + s, std::forward<Args>(value)...);
   return d[SmallVectorMembers::size++];
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::pop_back()
// Remove the last element
{
   assert(size());
   SmallVectorMembers::size--;
   std::destroy_at(data() + size());
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::resize(size_t count)
// Resize the vector
{
   if (count > capacity())
      growCapacity(count);

   auto s = size();
   auto d = data();
   if (count < s) {
      std::destroy_n(d + count, s - count);
   } else if (count > s) {
      std::uninitialized_value_construct_n(d + s, count - s);
   }
   SmallVectorMembers::size = static_cast<size_type>(count);
}
//---------------------------------------------------------------------------
template <class T, class Allocator>
void SmallVectorBase<T, Allocator>::resize(size_t count, const value_type& value)
// Resize the vector
{
   if (count > capacity())
      growCapacity(count);

   auto s = size();
   auto d = data();
   if (count < s) {
      std::destroy_n(d + count, s - count);
   } else if (count > s) {
      std::uninitialized_fill_n(d + s, count - s, value);
   }
   SmallVectorMembers::size = count;
}
//---------------------------------------------------------------------------
/// A vector implementation that uses a fixed sized buffer if possible, and falls back to heap if necessary.
template <class T, unsigned fixedCapacity, class Allocator = std::allocator<T>>
class SmallVector : public SmallVectorBase<T, Allocator>, smallvectorimpl::SmallVectorBuffer<T, fixedCapacity> {
   public:
   static constexpr unsigned inlineCapacity = fixedCapacity;
   using SmallVectorBase = dqsim::infra::SmallVectorBase<T, Allocator>;
   using size_type = typename SmallVectorBase::size_type;

   /// Constructor
   SmallVector() noexcept(noexcept(Allocator())) : SmallVectorBase(Allocator(), fixedCapacity) {}
   /// Constructor
   explicit SmallVector(const Allocator& alloc) noexcept : SmallVectorBase(alloc, fixedCapacity) {}
   /// Constructor
   explicit SmallVector(size_type count, const T& value, const Allocator& alloc = Allocator()) : SmallVectorBase(alloc, fixedCapacity) { this->assign(count, value); }
   /// Constructor
   explicit SmallVector(size_type count, const Allocator& alloc = Allocator()) : SmallVectorBase(alloc, fixedCapacity) { this->assign(count, T()); }
   /// Constructor
   template <class InputIt>
   SmallVector(InputIt first, InputIt last, const Allocator& alloc = Allocator()) : SmallVectorBase(alloc, fixedCapacity) { this->assign(first, last); }
   /// Constructor
   SmallVector(const SmallVector& other) : SmallVectorBase(other.get_allocator(), fixedCapacity) { this->assign(other.begin(), other.end()); }
   /// Constructor
   SmallVector(const SmallVectorBase& other) : SmallVectorBase(other.get_allocator(), fixedCapacity) { this->assign(other.begin(), other.end()); }
   /// Constructor
   SmallVector(const SmallVectorBase& other, const Allocator& alloc) : SmallVectorBase(alloc, fixedCapacity) { this->assign(other.begin(), other.end()); }
   /// Constructor
   SmallVector(SmallVector&& other) noexcept : SmallVectorBase(other.get_allocator(), fixedCapacity) { this->moveSame(std::move(other)); }
   /// Constructor
   SmallVector(SmallVectorBase&& other) : SmallVectorBase(other.get_allocator(), fixedCapacity) { this->moveOther(std::move(other)); }
   /// Constructor
   SmallVector(SmallVectorBase&& other, const Allocator& alloc) : SmallVectorBase(alloc, fixedCapacity) { this->moveOther(std::move(other)); }
   /// Constructor
   SmallVector(std::initializer_list<T> init, const Allocator& alloc = Allocator()) : SmallVectorBase(alloc, fixedCapacity) { this->assign(init); }
   /// Destructor
   ~SmallVector() = default;

   /// Assignment
   SmallVector& operator=(const SmallVector& other) {
      if (this != &other) this->assign(other.begin(), other.end());
      return *this;
   }
   /// Assignment
   SmallVector& operator=(const SmallVectorBase& other) {
      if (this != &other) this->assign(other.begin(), other.end());
      return *this;
   }
   /// Assignment
   SmallVector& operator=(std::initializer_list<T> init) {
      this->assign(init);
      return *this;
   }
   /// Assignment
   SmallVector& operator=(SmallVector&& other) noexcept {
      this->clear();
      shrink_to_fit();
      this->setAlloc(other.get_allocator());
      this->moveSame(std::move(other));
      return *this;
   }
   /// Assignment
   SmallVector& operator=(SmallVectorBase&& other) {
      this->clear();
      shrink_to_fit();
      this->setAlloc(other.get_allocator());
      this->moveOther(std::move(other));
      return *this;
   }

   /// Shrink to minimal size
   void shrink_to_fit() { SmallVectorBase::shrinkToFit(fixedCapacity); }
   /// Swap two vectors
   void swap(SmallVector& other) noexcept { SmallVectorBase::swapImpl(other); }
   /// Swap two vectors
   friend void swap(SmallVector& a, SmallVector& b) noexcept { a.swap(b); }
};
//---------------------------------------------------------------------------
/// Utility function to efficiently (move-)construct a SmallVector from elements
template <class T, unsigned fixedCapacity, class... Ts>
auto makeSmallVectorExplicit(Ts&&... args) {
   auto res = SmallVector<T, fixedCapacity>();
   res.reserve(sizeof...(Ts));
   // Parameter pack fold over the SmallVector::operator,() emplaces all args
   (res.emplace_back(std::forward<Ts>(args)), ...);
   return res;
}
//---------------------------------------------------------------------------
/// Utility function to efficiently (move-)construct a SmallVector from elements
template <class... Ts, unsigned fixedCapacity = sizeof...(Ts)>
auto makeSmallVector(Ts&&... args) {
   using T = std::common_type_t<Ts...>;
   return makeSmallVectorExplicit<T, fixedCapacity, Ts...>(std::forward<Ts>(args)...);
}
//---------------------------------------------------------------------------
// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
