#include "MapReduceFramework.h"

#include <condition_variable>
#include <algorithm>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>


class Barrier {
public:
    explicit Barrier(int numThreads);
    ~Barrier() = default;
    void barrier();

private:
    std::mutex mutex;
    std::condition_variable cv;
    int count;
    int generation;
    const int numThreads;
};

Barrier::Barrier(int numThreads)
        : count(0)
        , generation(0)
        , numThreads(numThreads)
{ }


void Barrier::barrier() {
    std::unique_lock<std::mutex> lock(mutex);
    int gen = generation;

    if (++count < numThreads) {
        cv.wait(lock, [this, gen] { return gen != generation; });
    } else {
        count = 0;
        generation++;
        cv.notify_all();
    }
}


struct JobContext;

struct ThreadContext{
    int Tid;
    JobContext* ThreadJobContext;

};

void runThread(ThreadContext* threadContext);
//int countIntermediatePairs(std::vector<IntermediateVec>* intermediate);
int countIntermediatePairs(const std::vector<IntermediateVec>* intermediate);


struct JobContext{
    const MapReduceClient& MRclient;
    const InputVec& inputVector;
    OutputVec& outputVector;
    int multiThreadLevel;

//    std::atomic<int>* emit2Counter;
    std::atomic<int>* emit3Counter;
    std::atomic<int>* mapInit;
    std::atomic<int>* mapFinish;
    std::atomic<int>* shuffleCounter;
    std::atomic<int>* reduceCounter;
    std::atomic<int> totalKeys;
    std::atomic<int> totalSize;



    std::atomic<bool> waitingFlag;
    stage_t stage;

	std::vector<std::pair<std::thread, ThreadContext*>>* threads;
    std::vector<IntermediateVec>* intermediate;
    std::vector<IntermediateVec>* shuffledQueue;


    Barrier* barrier;
    std::mutex emit3_mutex;
    std::mutex waitForJob_mutex;
    std::mutex getJobState_mutex;
    std::mutex shuffleMutex;
    std::mutex reduceMutex;



    JobContext(const MapReduceClient& client,
               const InputVec& inputVec,
               OutputVec& outputVecRef,
               int mtLevel)
        : MRclient(client), inputVector(inputVec), outputVector(outputVecRef), multiThreadLevel(mtLevel),
//          emit2Counter(nullptr),
         emit3Counter(nullptr),mapInit(nullptr), mapFinish(nullptr),
          shuffleCounter(nullptr), reduceCounter(nullptr),waitingFlag(false),
          stage(UNDEFINED_STAGE), threads(nullptr), intermediate(nullptr),shuffledQueue(nullptr),barrier(nullptr)
    {}
};


void emit2 (K2* key, V2* value, void* context){
    auto* threadContext = static_cast<ThreadContext*>(context);
    IntermediatePair pair(key, value);
    (*threadContext->ThreadJobContext->intermediate)[threadContext->Tid].push_back(pair);
//    threadContext->ThreadJobContext->emit2Counter->fetch_add(1);
}

void emit3(K3* key, V3* value, void* context) {
    auto* threadContext = static_cast<ThreadContext*>(context);
    std::unique_lock<std::mutex> lock(threadContext->ThreadJobContext->emit3_mutex);
    OutputPair pair(key, value);
    threadContext->ThreadJobContext->outputVector.push_back(pair);
    threadContext->ThreadJobContext->emit3Counter->fetch_add(1);
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto* jobContext = new(std::nothrow) JobContext(client, inputVec, outputVec, multiThreadLevel);
    if (!jobContext) {
        fprintf(stderr, "Error: Failed to allocate JobContext\n");
        exit(1); // Or return nullptr or throw
    }
    try {
        jobContext->emit3Counter = new std::atomic<int>(0);
        jobContext->mapInit = new std::atomic<int>(0);
        jobContext->mapFinish = new std::atomic<int>(0);
        jobContext->shuffleCounter = new std::atomic<int>(0);
        jobContext->reduceCounter = new std::atomic<int>(0);

        jobContext->threads = new std::vector<std::pair<std::thread, ThreadContext*>>();
        jobContext->intermediate = new std::vector<IntermediateVec>(multiThreadLevel);
        jobContext->barrier = new Barrier(multiThreadLevel);
        jobContext->shuffledQueue = new std::vector<IntermediateVec>();

        for (int i = 0; i < multiThreadLevel; ++i) {
            auto* threadContext = new ThreadContext();
            threadContext->Tid = i;
            threadContext->ThreadJobContext = jobContext;
            jobContext->threads->emplace_back(std::thread(runThread, threadContext), threadContext);
        }

    } catch (const std::bad_alloc& e) {
        fprintf(stderr, "Error: Memory allocation failed: %s\n", e.what());
        // Clean up anything that was already allocated
        delete jobContext;
        exit(1); // Or return nullptr / throw
    }

    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job){
	auto* jobContext = static_cast<JobContext*>(job);
    std::unique_lock<std::mutex> lock(jobContext->waitForJob_mutex);
    if (jobContext->waitingFlag.load()){
      return;
    }
    else{
        jobContext->waitingFlag.store(true);
    for (auto& thread_pair : *(jobContext->threads)) {
        if (thread_pair.first.joinable()) {
            thread_pair.first.join();
        }
    }
    }
}

void getJobState(JobHandle job, JobState* state){
    auto* jobContext = static_cast<JobContext*>(job);
    std::unique_lock<std::mutex> lock(jobContext->getJobState_mutex);
    state->stage = jobContext->stage;
    if (jobContext->stage == UNDEFINED_STAGE){
      state->percentage = 0;
    }
    else if(jobContext->stage == MAP_STAGE){
        state->percentage = (static_cast<float>(jobContext->mapFinish->load()) / static_cast<float>(jobContext->inputVector.size())) * 100;
    }
    else if (jobContext->stage == SHUFFLE_STAGE){
        state->percentage = (static_cast<float>(jobContext->shuffleCounter->load()) /
                     static_cast<float>(jobContext->totalSize)) * 100;
    }
    else if (jobContext->stage == REDUCE_STAGE){
        state->percentage = (static_cast<float>(jobContext->reduceCounter->load()) /
                     static_cast<float>(jobContext->totalKeys)) * 100;
    }
}

void closeJobHandle(JobHandle job){
    auto* jobContext = static_cast<JobContext*>(job);
    waitForJob(job);
//    delete jobContext->emit2Counter;
    delete jobContext->emit3Counter;
    delete jobContext->mapFinish;
    delete jobContext->mapInit;
    delete jobContext->shuffleCounter;
    delete jobContext->reduceCounter;
    for (auto& thread_pair : *(jobContext->threads)) {
        delete thread_pair.second;  // Free the ThreadContext*
    }
    delete jobContext->threads;
    delete jobContext->intermediate;
    delete jobContext->barrier;
    delete jobContext->shuffledQueue;
    delete jobContext;
}

bool compareKeys(const IntermediatePair& a, const IntermediatePair& b) {
    return *(a.first) < *(b.first);
}

void shuffle(JobContext* jobContext) {
    jobContext->totalSize = countIntermediatePairs(jobContext->intermediate);
    jobContext->stage = SHUFFLE_STAGE;

    auto& intermediates = *(jobContext->intermediate);
    auto& queue = *(jobContext->shuffledQueue);

    while (true) {
        bool allEmpty = true;
        IntermediatePair maxPair = {nullptr, nullptr};

        // 1. Find the max key (using compareKeys)
        for (auto& vec : intermediates) {
            if (!vec.empty()) {
                allEmpty = false;
                IntermediatePair candidate = vec.back();
                if (!maxPair.first || compareKeys(maxPair, candidate)) {
                    maxPair = candidate;
                }
            }
        }

        if (allEmpty) break;

        // 2. Group all (k2,v2) with max key
        IntermediateVec group;
        for (auto& vec : intermediates) {
            while (!vec.empty() && !compareKeys(vec.back(), maxPair) && !compareKeys(maxPair, vec.back())) {
                // Not less than and not greater than => equal
                group.push_back(vec.back());
                vec.pop_back();
            }
        }

        {
            std::lock_guard<std::mutex> lock(jobContext->shuffleMutex);
            queue.push_back(std::move(group));
            (*(jobContext->shuffleCounter))++;
        }
    }
    jobContext->totalKeys = jobContext->shuffledQueue->size();
}



void map(ThreadContext *threadContext, JobContext *jobContext) {
    jobContext->stage = MAP_STAGE;
    size_t counter = jobContext->mapInit->fetch_add(1);
    while(counter < jobContext->inputVector.size()){
        jobContext->MRclient.map(jobContext->inputVector[counter].first,jobContext->inputVector[counter].second,threadContext);
        counter = jobContext->mapInit->fetch_add(1);
        jobContext->mapFinish->fetch_add(1);
    }
}

void reducePhase(ThreadContext* threadContext) {
    JobContext* jobContext = threadContext->ThreadJobContext;
    auto& queue = *(jobContext->shuffledQueue);

    while (true) {
        IntermediateVec group;

        // Safely pop from the queue
        {
            std::unique_lock<std::mutex> lock(jobContext->reduceMutex);
            if (queue.empty()) {
                break;
            }
            group = std::move(queue.back());
            queue.pop_back();
        }

        // Call client reduce on the group
        jobContext->MRclient.reduce(&group, threadContext);

        // Count completed reduce jobs
        jobContext->reduceCounter->fetch_add(1);
    }
}

void runThread(ThreadContext* threadContext){
    JobContext* jobContext = threadContext->ThreadJobContext;
    // map phase
    map(threadContext, jobContext);
    // sort
    std::sort((*jobContext->intermediate)[threadContext->Tid].begin(),
              (*jobContext->intermediate)[threadContext->Tid].end(),
              compareKeys);
    //barrier
    jobContext->barrier->barrier();
    //shuffle
    if (threadContext->Tid == 0) {
        jobContext->stage = SHUFFLE_STAGE;
        shuffle(jobContext);
    }
    // reduce
    jobContext->barrier->barrier();
    jobContext->stage = REDUCE_STAGE;
    reducePhase(threadContext);
}

int countIntermediatePairs(const std::vector<IntermediateVec>* intermediate) {
    int totalIntermediatePairs = 0;
    for (const auto& vec : *intermediate) {
        totalIntermediatePairs += static_cast<int>(vec.size());
    }
    return totalIntermediatePairs;
}
