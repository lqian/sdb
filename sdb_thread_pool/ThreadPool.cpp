/*
 * ThreadPool.cpp
 *
 *  Created on: Mar 23, 2015
 *      Author: linkqian
 */

#include "ThreadPool.h"

namespace sdb {
namespace common {

using namespace std;

void ThreadPool::await_terminate() {
	await_terminate(false);
}
void ThreadPool::await_terminate(bool force) {
	if (!force) {
		//waiting for all task to be executed
		taskQueue.wait_empty();
	}

	//set the stopping flag
	stopping = true;

	// set the flag for taskQueue stopping signal
	taskQueue.stopping();

	//waiting for task thread which is executing current task!
	for (size_t i = 0; i < threads.size(); i++) {
		if (force) {
			threads[i].detach();
		}
		else if (threads[i].joinable()) {
			threads[i].join();
		}
	}

	terminated = true;
}

void ThreadPool::insert(Runnable & r) {
	function<void()> f = bind(&Runnable::run, &r);
	taskQueue.insert(f);
}

bool ThreadPool::insert(Runnable &r, long ms) {
	function<void()> f = bind(&Runnable::run, &r);
	return taskQueue.insert(f, chrono::milliseconds(ms));
}

void ThreadPool::push_back(Runnable &r) {
	function<void()> f = bind(&Runnable::run, &r);
	taskQueue.push_back(f);
}

bool ThreadPool::push_back(Runnable &r, long ms) {
	function<void()> f = bind(&Runnable::run, &r);
	return taskQueue.push_back(f, chrono::milliseconds(ms));
}

// take the first element from dueue
bool TaskQueue::take_to(function<void()> & f) {
	bool taked = false;
	mutex.lock();
	condition.wait(mutex, [this]() {
		return this->stopping_waiting || this->not_empty();});
	if (stopping_waiting) {
		taked = false;
	} else {
		f = task_queue.front();
		task_queue.pop_front();
		taked = true;
		condition.notify_one();
	}
	mutex.unlock();
	return taked;
}

// take the first element from deque within a time limit specified with millisecond
bool TaskQueue::take_to(function<void()> &f, chrono::milliseconds ms) {
	bool taked = false;
	mutex.lock();
	if (condition.wait_for(mutex, ms,
			[this]() {return stopping_waiting || not_empty();})) {
		if (stopping_waiting) {
			taked = false;
		} else {
			f = task_queue.front();
			task_queue.pop_front();
			taked = true;
			condition.notify_one();
		}
	}
	mutex.unlock();
	return taked;
}

void TaskQueue::stopping() {
	mutex.lock();
	stopping_waiting = true;
	condition.notify_all();
	mutex.unlock();
}

int TaskQueue::task_num() {
	return task_queue.size();
}

bool TaskQueue::not_empty() {
	return task_queue.size() > 0;
}

bool TaskQueue::not_full() {
	return task_queue.size() < max_task_num;
}

bool TaskQueue::is_empty() {
	return task_queue.size() == 0;
}

bool TaskQueue::is_full() {
	return task_queue.size() == max_task_num;
}

void TaskQueue::wait_empty() {
	mutex.lock();
	condition.wait(mutex, [this]() {return is_empty();});
	mutex.unlock();
}

void TaskQueue::wait_empty(long duration) {
	mutex.lock();
	condition.wait_for(mutex, std::chrono::milliseconds(duration),
			[this]() {return is_empty();});
	mutex.unlock();
}

bool TaskQueue::insert(function<void()> &f, chrono::milliseconds ms) {
	bool success = false;
	mutex.lock();
	if (condition.wait_for(mutex, ms, [this]() {return not_full();})) {
		task_queue.push_front(f);
		condition.notify_one();
		mutex.unlock();
		success = true;
	}

	return success;
}

void TaskQueue::insert(function<void()> &f) {
	mutex.lock();
	condition.wait(mutex, [this]() {return not_full();});
	task_queue.push_front(f);
	condition.notify_one();
	mutex.unlock();
}

bool TaskQueue::push_back(function<void()> &f, chrono::milliseconds ms) {
	bool success = false;
	mutex.lock();
	if (condition.wait_for(mutex, ms, [this]() {return not_full();})) {
		task_queue.push_back(f);
		condition.notify_one();
		success = true;
	}
	mutex.unlock();

	return success;
}

void TaskQueue::push_back(function<void()> &f) {
	mutex.lock();
	condition.wait_for(mutex, std::chrono::milliseconds(0),
			[this]() {return this->not_full();});
	task_queue.push_back(f);
	condition.notify_one();
	mutex.unlock();
}

void ThreadPool::init_thread_workers() {
	for (int i = 0; i < coreSize; i++) {
		threads.push_back(std::thread(&ThreadPool::execute, this));
	}
}

ThreadPool::ThreadPool(int __coreSize, int __maxTask) :
		terminated(false), stopping(false), taskQueue(__maxTask), threads(
				__coreSize), lockTimeout(0L) {
	coreSize = __coreSize;
	init_thread_workers();
}

ThreadPool::ThreadPool(int __coreSize, int __maxTask, long __timeout) :
		terminated(false), stopping(false), taskQueue(__maxTask), lockTimeout(
				__timeout) {
	coreSize = __coreSize;

	init_thread_workers();
}

void ThreadPool::execute() {
	while (!stopping) {
		function<void()> exec_runnable;
		if (lockTimeout <= 0) {
			if (taskQueue.take_to(exec_runnable)) {
				exec_runnable();
			}
		} else {
			chrono::milliseconds ms(lockTimeout);
			if (taskQueue.take_to(exec_runnable, ms)) {
				exec_runnable();
			}
		}
	}
}

ThreadPool::~ThreadPool() {
	if (!terminated) {
		await_terminate();
	}
}

}  // namespace common

} /* sdb namespace ending */

