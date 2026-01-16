cmake_minimum_required(VERSION 3.30)

project(dqsim LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DPERFTRACER")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(DISTSIM_TOPLEVEL TRUE)
else()
    set(DISTSIM_TOPLEVEL FALSE)
endif()

if (DISTSIM_TOPLEVEL)
    if(APPLE)
        if(EXISTS "/opt/homebrew/opt/boost")
            set(BOOST_ROOT "/opt/homebrew/opt/boost" CACHE PATH "Homebrew Boost root" FORCE)
            set(Boost_NO_SYSTEM_PATHS ON)
        endif()
    endif()

    find_package(Boost REQUIRED COMPONENTS context thread HINTS /opt/homebrew/opt)
    message(STATUS "Boost version: ${Boost_VERSION}")
    message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARIES}")

    find_library(GLPK_LIBRARY NAMES glpk HINTS /opt/homebrew/opt)
    find_path(GLPK_INCLUDE_DIR
        NAMES glpk.h
        HINTS /opt/homebrew/opt
        PATH_SUFFIXES include
    )
    if(NOT GLPK_LIBRARY OR NOT GLPK_INCLUDE_DIR)
        message(FATAL_ERROR "Could not find GLPK library or headers")
    endif()
endif()
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

#---------------------------------------------------------------------------
# Debugging info
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")

# Only new compilers work
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
        message(FATAL_ERROR "GCC ≥ 14 required.")
    endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # libc++ version is not directly exposed by CMake,
    # but std::ranges::to requires libc++ ≥ 16.
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 16)
        message(WARNING "Clang detected — ensure libc++ ≥ 16 or std::ranges::to will not be available.")
    endif()
endif()

#---------------------------------------------------------------------------
# dqsim infra

add_library(dqsim_infra
  src/infra/JSONMapping.cpp
  src/infra/JSONReader.cpp
  src/infra/JSONWriter.cpp
  src/infra/CommandLine.cpp
  src/infra/SmallVector.cpp
)
target_include_directories(dqsim_infra PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${GLPK_INCLUDE_DIR})

#---------------------------------------------------------------------------
# distributed simulation

add_library(dqsim_core
  src/benchmark/BenchmarkDefinition.cpp
  src/benchmark/BenchmarkRunner.cpp
  src/benchmark/ClusterBuilder.cpp
  src/benchmark/DUBuilder.cpp
  src/benchmark/Handcrafted.cpp
  src/distributedplan/DataUnit.cpp
  src/distributedplan/DistributedQueryPlan.cpp
  src/distributedplan/Pipeline.cpp
  src/distributedplan/ProblemWriter.cpp
  src/network/StreamSimulator.cpp
  src/network/NetworkSimulator.cpp
  src/partitioning/Attribute.cpp
  src/partitioning/Partitioner.cpp
  src/partitioning/PartitionLayout.cpp
  src/partitioning/Partition.cpp
  src/partitioning/PartitionManager.cpp
  src/planning/AssignmentPlannerState.cpp
  src/planning/ComponentPlanner.cpp
  src/planning/EnumerationPlanner.cpp
  src/planning/GreedyPlanner.cpp
  src/planning/GreedyInfra.cpp
  src/planning/HEFTPlanner.cpp
  src/planning/HEFTScheduler.cpp
  src/planning/NetHEFTPlanner.cpp
  src/planning/NetHEFTScheduler.cpp
  src/planning/PolarisPlanner.cpp
  src/planning/TaskAssignmentPlanner.cpp
  src/tasks/Task.cpp
  src/MemorySimulator.cpp
  src/Simulator.cpp
  src/SimulatorImpl.cpp
  src/Cluster.cpp
  src/CustomTypes.cpp
  src/TempFile.cpp
)
target_include_directories(dqsim_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${GLPK_INCLUDE_DIR})
target_link_libraries(dqsim_core PUBLIC dqsim_infra ${GLPK_LIBRARY} Boost::boost)

#---------------------------------------------------------------------------
# distributed localization problem solver

add_executable(ds_task_planner src/planning/TaskPlanner.cpp)
target_link_libraries(ds_task_planner PUBLIC dqsim_infra dqsim_core ${GLPK_LIBRARY} Boost::boost)
target_include_directories(ds_task_planner PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${GLPK_INCLUDE_DIR})

#---------------------------------------------------------------------------
# distributed simulation for a fixed file

add_executable(ds_sim src/sim.cpp)
target_link_libraries(ds_sim PUBLIC dqsim_infra dqsim_core ${GLPK_LIBRARY} Boost::boost)
target_include_directories(ds_sim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${GLPK_INCLUDE_DIR})

#---------------------------------------------------------------------------
# distributed localization benchmark

add_executable(ds_bench_s src/benchmarkStandalone.cpp)
target_link_libraries(ds_bench_s PUBLIC dqsim_infra dqsim_core ${GLPK_LIBRARY} Boost::boost)
target_include_directories(ds_bench_s PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${GLPK_INCLUDE_DIR})
