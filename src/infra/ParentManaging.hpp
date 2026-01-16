#pragma once
//---------------------------------------------------------------------------
namespace dqsim::infra {
namespace parentptrvectorimpl {
//---------------------------------------------------------------------------
/// Helper to generically extract the managed type
template <class T>
struct ManagedTypeImpl;
template <class T>
using ManagedType = typename ManagedTypeImpl<T>::Type;
//---------------------------------------------------------------------------
/// ParentManaging types automatically take care of their parent reference to
/// guarantee that the child-parent relationship stays intact. Examples in
/// our code are OperatorPtr and IUPtr.
/// The ParentManaging concept here is a generalization upon containers and
/// nested structures.
///
/// Smartpointers, similar to unique_ptr that automatically manage their parent
/// They need typedefs for ParentType and WrappedType to check the requirements
/// And allow managing through assign() / reparent() / release()
template <class T>
concept ParentPtr = requires(T ptr, typename std::remove_reference_t<T>::ParentType containingParent, typename std::remove_reference_t<T>::WrappedType owner) {
   ptr.assign(containingParent, std::move(owner));
   ptr.reparent(containingParent);
   // clang-format off
   { ptr.release() } -> std::same_as<typename std::remove_reference_t<T>::WrappedType>;
   // clang-format on
};
template <ParentPtr T>
struct ManagedTypeImpl<T> {
   using Type = T;
};
/// Containers with a ParentPtr
/// Exposed via an accessor method as in the following example:
/// struct Container {
///    OperatorPtr ptr;
///    auto& getContainedParentPtr() { return ptr; }
/// }
template <class T>
concept ParentPtrContainer = requires(T ptrContainer) {
   // clang-format off
   { ptrContainer.getContainedParentPtr() } -> ParentPtr;
   // clang-format on
};
template <ParentPtrContainer T>
struct ManagedTypeImpl<T> {
   using Type = std::remove_cvref_t<decltype(std::declval<T>().getContainedParentPtr())>;
};
// clang-format off
/// Helper to check, if a tuple consists of ParentPtrs
template <class>
constexpr bool ParentPtrTuple = false;
template <template <class...> class Tuple, class... Types>requires(ParentPtr<Types>&&...)
constexpr bool ParentPtrTuple<Tuple<Types...>> = true;
/// Helper to check, if a tuple of ParentPtrs have compatible parents
template <class>
constexpr bool CompatibleParentTypeTuple = false;
template <template <class...> class Tuple, ParentPtr T, ParentPtr... Ts> requires(std::conjunction_v<std::is_convertible<typename ManagedType<std::remove_reference_t<T>>::ParentType, typename ManagedType<std::remove_reference_t<Ts>>::ParentType>...>)
constexpr bool CompatibleParentTypeTuple<Tuple<T, Ts...>> = true;
// clang-format on
/// Concept of a tuple of ParentPtrs that share the same parent
template <class T>
concept ParentManagingTuple = ParentPtrTuple<T> && CompatibleParentTypeTuple<T>;
//---------------------------------------------------------------------------
/// Containers with multiple ParentPtr that have the same parent
/// Exposed via an accessor method as in the following example:
/// struct Container {
///    OperatorPtr ptr1, ptr2;
///    auto getContainedParentPtr() { return std::tie(ptr1, ptr2); }
/// }
/// NOTE: If the parent types are heterogenous, make sure the parent types
/// are convertible and the most generic type is tie()'d first
template <class T>
concept MultipleParentPtrContainer = requires(T ptrContainer) {
   // clang-format off
   { ptrContainer.getContainedParentPtr() } -> ParentManagingTuple;
   // clang-format on
};
template <MultipleParentPtrContainer T>
struct ManagedTypeImpl<T> {
   using Type = std::remove_cvref_t<std::tuple_element_t<0, std::remove_cvref_t<decltype(std::declval<T>().getContainedParentPtr())>>>;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
/// Generic interface. Either a Smartpointer to a container of smartpointers
template <class T>
concept ParentManaging = parentptrvectorimpl::ParentPtr<T> || parentptrvectorimpl::ParentPtrContainer<T> || parentptrvectorimpl::MultipleParentPtrContainer<T>;
//---------------------------------------------------------------------------
/// A vector of owning pointers. Sets parent pointers etc. automatically
/// NOTE: ParentPtrVector is a ParentPtr itself and can be nested!
template <ParentManaging T>
class ParentPtrVector;
//---------------------------------------------------------------------------
/// Helper functions for parent management
template <ParentManaging T>
struct ParentManagement {
   /// The parent type
   using ParentType = typename parentptrvectorimpl::ManagedType<T>::ParentType;

   /// Set a new parent for the managed type
   static void reparent(T& ptr, ParentType parent) noexcept {
      if constexpr (parentptrvectorimpl::ParentPtr<T>)
         ptr.reparent(parent);
      else if constexpr (parentptrvectorimpl::ParentPtrContainer<T>)
         ptr.getContainedParentPtr().reparent(parent);
      else if constexpr (parentptrvectorimpl::MultipleParentPtrContainer<T>)
         std::apply([&](auto&... x) { (x.reparent(parent), ...); }, ptr.getContainedParentPtr());
      else
         static_assert(!ParentManaging<T>, "unhandled case in reparentImpl"); // unreachable
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
