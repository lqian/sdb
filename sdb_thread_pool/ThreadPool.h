/*
 * ThreadPool.h
 *
 *  Created on: Mar 23, 2015
 *      Author: linkqian
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <iostream>
#include <functional>
#include <deque>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <condition_variable>

#define DEFAULT_QUEUE 1024;

namespace sdb {
namespace common {

class ThreadPool;
/*
 * represent a action object to be put thread pool
 */
class Runnable {
public:
	virtual void run() {
	}
};

/*
 * a task queue contains Runnable object's to be execute. ThreadPool put/insert Runnable object to it.
 * Workers in ThreadPool consumer this object and delegate to execute object's run method.
 */
class TaskQueue {
private:
	friend class ThreadPool;

	std::mutex mutex;
	std::condition_variable_any condition;
	std::deque<std::function<void()> > task_queue;
	bool stopping_waiting;
	int max_task_num;
	void stopping();

public:
	TaskQueue(int s=DEFAULT_QUEUE):max_task_num(s), stopping_waiting(false) {
	}

	// insert a function to the first of deque until success or throw exception
	void insert( std::function<void()> &f);

	// insert a function to the first of deque within a time limit specified with millisecond
	bool insert( std::function<void()> &f, std::chrono::milliseconds ms);

	// push a function to the end of deque until success or throw exception
	void push_back( std::function<void()> &f);

	// push a function to the end of deque within a time limit specified with millisecond
	bool push_back( std::function<void()> &f,
			std::chrono::milliseconds ms);

	// take the first element from deque until has take a element from the queue
	bool take_to(std::function<void()> & f);

	// take the first element from deque within a time limit specified with millisecond
	 bool take_to(std::function<void()> & f, std::chrono::milliseconds ms);

	 int task_num();

	 bool not_full() ;

	 bool not_empty();

	 bool is_empty();

	 bool is_full();

	 void wait_empty();
	 void wait_empty(long ms);

};

class ThreadPool {

private:
	friend class Worker;
	int coreSize;

	std::vector<std::thread> threads;
	TaskQueue taskQueue;

	bool stopping;
	bool terminated;
	long lockTimeout;


	void execute();

public:
	ThreadPool(){}
	ThreadPool(int __coreSize, int __maxTask);
	ThreadPool(int __coreSize, int __maxTask, long __timeout);
	~ThreadPool();
	void init_thread_workers();
	void push_back(Runnable & r);
	bool push_back(Runnable & r, const long milliseconds);
	void insert(Runnable & r);
	bool insert(Runnable & r, const long milliseconds);

	// convenience method for awaitTerminate(false)
	void await_terminate();

	// await to terminate thread pool, if forced, it terminates after executing task is completed,
	// else it terminate until all task is completed.
	void await_terminate(bool forced);

	inline bool is_empty() {
		return taskQueue.is_empty();
	}
	inline bool is_terminated() {
		return terminated;
	}
};

} /* namespace common */
} /* namespace sdb */

#endif /* THREADPOOL_H_ */
