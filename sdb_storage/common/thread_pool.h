/*
 * ThreadPool.h
 *
 *  Created on: Mar 23, 2015
 *      Author: linkqian
 */

#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <iostream>
#include <functional>
#include <deque>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <condition_variable>
#include <type_traits>

#define DEFAULT_QUEUE 1024

namespace sdb {
namespace common {
using namespace std;
using namespace std::chrono;

template<class T> class thread_pool;

/*
 * represent a action object to be put thread pool
 */
class runnable {
public:
	virtual ~runnable(){};

	virtual void run() {
	}

	virtual int status() {
		return -1;
	}

	/*
	 * when run method execution is not successful, get some hint via this method
	 */
	virtual string msg() {
		return "no msg";
	}
};

template<typename T>
class condition_queue {
private:
	friend class thread_pool<T> ;
	std::mutex mutex;
	std::condition_variable_any condition;
	std::deque<T> deque;
	bool stopping_waiting;
	int max_task_num;

	void stopping();

	condition_queue(int s = DEFAULT_QUEUE) :
			max_task_num(s), stopping_waiting(false) {
	}

	void insert(T & t) {
		mutex.lock();
		condition.wait(mutex, [this]() {return not_full();});
		deque.push_front(t);
		condition.notify_one();
		mutex.unlock();
	}

	bool insert(T & t, long ms) {
		bool success = false;
		mutex.lock();
		if (condition.wait_for(mutex, milliseconds(ms),
				[this]() {return not_full();})) {
			deque.push_front(t);
			condition.notify_one();
			success = true;
		}
		mutex.unlock();

		return success;
	}

	void push_back(T &t) {
		mutex.lock();
		condition.wait(mutex, [this]() {return not_full();});
		deque.push_back(t);
		condition.notify_one();
		mutex.unlock();
	}
	bool push_back(T &t, long ms) {
		bool success = false;
		mutex.lock();
		if (condition.wait_for(mutex, milliseconds(ms),
				[this]() {return not_full();})) {
			deque.push_back(t);
			condition.notify_one();
			success = true;
		}
		mutex.unlock();

		return success;
	}

	bool take_to(T &t) {
		bool taked = false;
		mutex.lock();
		condition.wait(mutex, [this]() {
			return this->stopping_waiting || this->not_empty();});
		if (stopping_waiting) {
			taked = false;
		} else {
			t = deque.front();
			deque.pop_front();
			taked = true;
			condition.notify_one();
		}
		mutex.unlock();
		return taked;
	}

	bool take_to(T &t, long ms) {
		bool taked = false;
		mutex.lock();
		if (condition.wait_for(mutex, milliseconds(ms),
				[this]() {return stopping_waiting || not_empty();})) {
			if (stopping_waiting) {
				taked = false;
			} else {
				t = deque.front();
				deque.pop_front();
				taked = true;
				condition.notify_one();
			}
		}
		mutex.unlock();
		return taked;
	}

	int task_num() {
		return deque.size();
	}

	bool not_full() {
		return deque.size() < max_task_num;
	}

	bool not_empty() {
		return deque.size() > 0;
	}

	bool is_empty() {
		return deque.size() == 0;
	}

	bool is_full() {
		return deque.size() == max_task_num;
	}

	void wait_empty() {
		mutex.lock();
		condition.wait(mutex, [this]() {return is_empty();});
		mutex.unlock();
	}
	void wait_empty(long ms) {
		mutex.lock();
		condition.wait_for(mutex, milliseconds(ms),
				[this]() {return is_empty();});
		mutex.unlock();

	}

};

template<typename T>
class thread_pool {

private:
	friend class Worker;
	int core_size;

	std::vector<std::thread> threads;
	condition_queue<T> inque;
	condition_queue<T> outque;

	bool stopping;
	bool terminated;
	long lock_timeout;

	void execute() {
		while (!stopping) {
			function<void()> exec_runnable;
			T t;
			if (lock_timeout <= 0) {
				if (inque.take_to(t)) {
					exec_runnable();
					outque.push_back(t);
				}
			} else {
				if (inque.take_to(t, lock_timeout)) {
					//static_assert(std::is_base_of<runnable, T>::value, true);
					exec_runnable = std::bind(&runnable::run, &t);
					exec_runnable();
					outque.push_back(t, lock_timeout);
				}
			}
		}
	}

public:
	thread_pool(int __coreSize = 4, int __maxTask = 100, long __timeout = 0) :
			terminated(false), stopping(false), lock_timeout(__timeout) {
		this->core_size = __coreSize;
		inque.max_task_num = __maxTask;
		outque.max_task_num = __maxTask;
		init_thread_workers();
	}
	~thread_pool() {
		if (!terminated) {
			await_terminate();
		}
	}
	void init_thread_workers() {
		for (int i = 0; i < core_size; i++) {
			threads.push_back(std::thread(&thread_pool::execute, this));
		}
	}
	bool push_back(T & t) {
		bool r = false;
		if (std::is_base_of<runnable, T>::value) {
			inque.push_back(t);
			r = true;
		}
		return r;
	}
	bool push_back(T &t, const long ms) {
		bool r = false;
		if (std::is_base_of<runnable, T>::value) {
			r = inque.push_back(t, ms);
		}
		return r;
	}

	bool insert(T & t) {
		bool r = false;
		if (std::is_base_of<runnable, T>::value) {
			inque.insert(t);
			r = true;
		}
		return r;
	}
	bool insert(T & t, const long ms) {
		bool r = false;
		if (std::is_base_of<runnable, T>::value) {
			r = inque.insert(t, ms);
		}
		return r;
	}

	// convenience method for awaitTerminate(false)
	void await_terminate() {
		await_terminate(false);
	}

	// await to terminate thread pool, if forced, it terminates after executing task is completed,
	// else it terminate until all task is completed.
	void await_terminate(bool forced) {
		if (!forced) {
			//waiting for all task to be executed
			inque.wait_empty();
		}

		//set the stopping flag
		stopping = true;

		// set the flag for taskQueue stopping signal
		inque.stopping();

		//waiting for task thread which is executing current task!
		for (size_t i = 0; i < threads.size(); i++) {
			if (forced) {
				threads[i].detach();
			} else if (threads[i].joinable()) {
				threads[i].join();
			}
		}

		terminated = true;
	}

	inline bool is_empty() {
		return inque.is_empty();
	}
	inline bool is_terminated() {
		return terminated;
	}
	inline void set_core_size(int cs) {
		core_size = cs;
	}

	inline void set_max_task(int mt) {
		inque.max_task_num = mt;
		outque.max_task_num = mt;
	}
};

} /* namespace common */
} /* namespace sdb */

#endif /* THREAD_POOL_H_ */
