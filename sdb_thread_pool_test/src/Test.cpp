#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"
#include <cstdlib>
#include "ThreadPool.h"
#include <functional>
#include <atomic>
#include <map>

using namespace std;
using namespace sdb::common;

std::atomic<int> counter(0);

class EchoRunner: public sdb::common::Runnable {
private:
	long duration;
public:
	EchoRunner(long d) :
			duration(d) {
	}
	virtual void run() {
		std::this_thread::sleep_for(std::chrono::milliseconds(duration));
	}
};

class CounterRunner: public sdb::common::Runnable {
public:
	virtual void run() {
		counter++;
	}
};

void test_task_queue() {
	sdb::common::TaskQueue taskQueue(100);
	EchoRunner r1(rand() % 100);
	std::function<void()> f = std::bind(&EchoRunner::run, &r1);
	taskQueue.insert(f);
	ASSERT(taskQueue.task_num() == 1);

	std::function<void()> f2 = std::bind(&EchoRunner::run, &r1);
	ASSERT(taskQueue.insert(f2, std::chrono::milliseconds(30)));

	std::function<void()> t1;
	taskQueue.take_to(t1);
	t1();

	std::function<void()> t2;
	ASSERT(taskQueue.take_to(t2, std::chrono::milliseconds(30)));
	t2();

	cout << taskQueue.task_num() << endl;
	ASSERT(taskQueue.task_num() == 0);

	while (taskQueue.not_full()) {
		EchoRunner r(rand() % 100);
		std::function<void()> fe = std::bind(&EchoRunner::run, &r);
		taskQueue.insert(f);
	}

	ASSERT(taskQueue.is_full());
}

void test_thread_pool() {
	sdb::common::ThreadPool tp(4, 100);

	for (int i = 0; i < 1200; i++) {
		CounterRunner runner;
		tp.push_back(runner);
	}

	ASSERT(tp.is_terminated() == false);

	std::cout << "await terminated gracefully" << endl;
	tp.await_terminate();
	ASSERT(tp.is_empty());
	ASSERT(tp.is_terminated() == true);

	ASSERT(counter.load(std::memory_order_relaxed) == 1200);
}
void newTestFunction(){
	std::map<char, int> map1;
	map1.insert(std::pair<char,int>('a', 1));
	ASSERT(map1.find('a') != map1.end());

	ASSERT(map1.find('a')->second == 1);
}


void runSuite() {
	cute::suite s;

	s.push_back(CUTE(test_task_queue));
	s.push_back(CUTE(test_thread_pool));
	s.push_back(CUTE(newTestFunction));

	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The Suite");
}

int main() {
	runSuite();
	return 0;
}

