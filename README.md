# MapReduce Framework (C++, Multi-Threaded)

A generic, reusable **MapReduce execution engine** implemented from scratch in C++ with `std::thread`, without any external parallelism libraries. A client supplies only a `map` and a `reduce` function; the framework handles thread creation, synchronization, shuffling, and progress reporting.

## Overview

The framework runs a MapReduce job over an input vector using a fixed number of worker threads, each of which:

1. **Maps** elements from the input, emitting intermediate `(K2, V2)` pairs via `emit2`.
2. **Sorts** its own intermediate pairs by key.
3. **Shuffles** (one designated thread merges and groups all threads' sorted intermediate pairs by equal key, across all threads) once every thread has finished mapping.
4. **Reduces** each key's group of values, emitting final `(K3, V3)` pairs via `emit3`.

All four phases run under the same pool of threads; no new threads are spawned per phase.

## API

### `MapReduceFramework.h`

- `JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)` — starts the job on `multiThreadLevel` threads and returns a handle immediately (asynchronous).
- `void waitForJob(JobHandle job)` — blocks until the job finishes. Safe to call multiple times or never.
- `void getJobState(JobHandle job, JobState* state)` — reports the current `stage_t` (`UNDEFINED_STAGE` / `MAP_STAGE` / `SHUFFLE_STAGE` / `REDUCE_STAGE`) and completion percentage of that stage.
- `void closeJobHandle(JobHandle job)` — waits for the job if needed, then releases all resources associated with it.
- `void emit2(K2* key, V2* value, void* context)` / `void emit3(K3* key, V3* value, void* context)` — called by the client's `map`/`reduce` implementations to emit intermediate and final pairs, respectively.

### `MapReduceClient.h`

- `K1`/`K2`/`K3` are abstract key classes exposing only `operator<`, so the framework can sort and compare keys without knowing their concrete type; `V1`/`V2`/`V3` are opaque value base classes.
- `InputPair`/`IntermediatePair`/`OutputPair` are `(K*, V*)` pairs, collected into `InputVec`/`IntermediateVec`/`OutputVec`.
- `MapReduceClient` is the interface a user implements: `map(const K1*, const V1*, void* context)` calls `emit2` any number of times, and `reduce(const IntermediateVec*, void* context)` — given one key and all its values — calls `emit3`, usually once.

A client subclasses `MapReduceClient`, implements `map`/`reduce` against its own concrete `K1`/`V1`/.../`V3` types, and passes an instance of it to `startMapReduceJob`; the framework drives everything else.

## Design & Synchronization

- **`Barrier`**: a from-scratch generation-counting barrier (mutex + condition variable) used to synchronize all worker threads between the map and shuffle phases, and again between shuffle and reduce.
- **Per-thread intermediate vectors**: each thread maps into its own `IntermediateVec` (no locking needed during mapping), then sorts it locally before the barrier — avoiding contention during the most parallel phase.
- **Shuffle**: performed by a single thread (`Tid == 0`) after the barrier. It repeatedly scans every thread's (now-sorted) intermediate vector, finds the current maximum key across all of them, pops every pair matching that key from the back of its vector (O(1) per pop, since vectors are sorted ascending), and pushes the resulting group onto a shared queue.
- **Reduce phase**: all threads pull groups off the shared shuffled-queue (`reduceMutex`-protected) and call the client's `reduce` on each, so reduce work is load-balanced across threads rather than statically assigned.
- **Atomic counters**: `std::atomic<int>` counters (`mapInit`, `mapFinish`, `shuffleCounter`, `reduceCounter`, `emit3Counter`) drive both work distribution (e.g. `mapInit.fetch_add(1)` hands each thread the next input index with no lock) and the progress percentage reported by `getJobState`.
- **`emit3` is the only mutex-protected emit path**, since all threads append directly to the single shared `outputVec` during reduce; `emit2` needs no lock because each thread owns its own intermediate slot.

## Files

```
.
├── MapReduceFramework.cpp   # Framework implementation (threads, barrier, shuffle, reduce)
├── MapReduceFramework.h     # Public API: JobHandle, JobState, stage_t, function declarations
└── MapReduceClient.h        # MapReduceClient interface and K1/V1/K2/V2/K3/V3 pair typedefs
```

`MapReduceFramework.h` and `MapReduceClient.h` define the framework/client contract; a client implementation (its own `map`/`reduce` logic and concrete key/value types) is not included here and must be supplied separately.

## Building

```bash
g++ -std=c++11 -pthread -c MapReduceFramework.cpp -o MapReduceFramework.o
```

Link the resulting object file together with a client implementation and `-pthread`.

## Usage Sketch

```cpp
MyClient client;
InputVec input = /* ... */;
OutputVec output;

JobHandle job = startMapReduceJob(client, input, output, /*multiThreadLevel=*/4);

JobState state;
getJobState(job, &state);   // poll progress at any time

waitForJob(job);            // block until done
closeJobHandle(job);        // release resources
```

## Tech Stack

C++11, `std::thread`, `std::atomic`, `std::mutex`/`std::condition_variable`

