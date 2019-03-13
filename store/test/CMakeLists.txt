project(storeTest)
cmake_minimum_required(VERSION 2.6)

SET(CMAKE_C_FLAGS "$ENV{CFLAGS} -g -fPIC -Wall --std=gnu99")
SET(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -g -fPIC -Wall  --std=c++11")
include_directories( ../../util/  ../../replicator/  ../..  ../../message/)
ADD_EXECUTABLE(testAppendingIndex testAppendingIndex.cpp ../../util/arena.cc ../appendingIndex.cpp)

