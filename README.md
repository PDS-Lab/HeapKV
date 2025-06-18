<!-- ## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/main/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Questions and discussions are welcome on the [RocksDB Developers Public](https://www.facebook.com/groups/rocksdb.dev/) Facebook group and [email list](https://groups.google.com/g/rocksdb) on Google Groups.

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses. -->

## HeapKV: Enabling Efficient Garbage Collection for KV-Separated LSM Stores on Modern SSDs

### Introduction

Key-value (KV) separation has emerged as a pivotal solution to tackle write amplification in LSM-tree-based KV stores (LSM stores). However, the garbage collection (GC) mechanism essential for reclaiming obsolete values introduces substantial overheads: (i) Additional LSM-tree I/O operations degrade foreground performance and cause data inconsistency issues; (ii) Exacerbated write amplification arises from excessive valid data migration in update-intensive workloads. Moreover, existing GC optimization schemes fundamentally struggle to balance space overhead, write amplification, and system performance. We propose HeapKV, a high-performance KV separated LSM store that improves GC efficiency through three key technologies: (i) A lightweight two-level index and a global garbage view decouple GC operations of value storage from the LSM-tree, eliminating additional I/O operations; (ii) A novel valid data migration scheme mitigates write amplification during space reclamation by in-place overwrites and logical data copying; (iii) SSD-conscious I/O optimizations featuring asynchronous value flushing, fast read paths and concurrent prefetching for range queries.

### Overview

This repository contains a naive prototype of HeapKV that written in C++ based on [RocksDB](https://github.com/facebook/rocksdb) v8.11.3 . The core implementation of HeapKV is under `db/heap` directory.

## Build

### Requirement

- Linux Kernal 6.x (recommended)
- g++ 13.0+
- [CMake](https://gitlab.kitware.com/cmake/cmake) 3.10.0+
- [liburing](https://github.com/axboe/liburing) 2.6+
- filesystem that support reflink feature (e.g. XFS, Btrfs, ZFS...)

### Clone repository


```
$ git clone https://github.com/PDS-Lab/HeapKV.git
```

### Build

```
# use CC=gcc-13 CXX=g++-13 if the default compiler of your system is not g++ 13
# ninja librocksdb.so if you need shared lib

$ mkdir build && cd build
$ cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Release .. -GNinja
$ ninja librocksdb.a
```

### Usage

```cmake
target_include_directories(${target} ${PATH_TO_HEAPKV}/include)
target_link_libraries(${target} ${PATH_TO_HEAPKV}/build/librocksdb.so uring)
```

## HeapKV Specific Configuration

```
enable_heapkv: whether use heapkv key-value separation

min_heap_value_size: value ≥ this setting while be separated during flush operation

heap_extent_relocate_threshold: the GC ratio of HeapKV. (i.e. 0.3 means GC will be triggered if 30% of space are garbage or unavailable fragmentation)

enable_fast_path_update: whether update fast path info during compaction

heap_value_cache: the cache object to use for value and value index in extent file. It is recommended to create a new cache instance instead of using block cache of rocksdb.
```