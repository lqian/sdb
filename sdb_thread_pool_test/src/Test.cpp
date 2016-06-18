#include <thread_pool.h>
#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"
#include <cstdlib>
#include <functional>
#include <atomic>
#include <map>
#include <iostream>

using namespace std;
using namespace sdb::common;

std::atomic<int> counter(0);

class echo_runner: public sdb::common::runnable {
private:
	long duration;
public:
	echo_runner(long d) :
			duration(d) {
	}
	virtual void run() {
		std::this_thread::sleep_for(std::chrono::milliseconds(duration));
		cout << "echo runner" << this << std::endl;

	}
};

class counter_runner: public sdb::common::runnable {
public:
	virtual void run() {
		counter++;
	}
};


void test_thread_pool() {
//	sdb::common::thread_pool<echo_runner> tp(4, 100);
}
void newTestFunction(){
	std::map<char, int> map1;
	map1.insert(std::pair<char,int>('a', 1));
	ASSERT(map1.find('a') != map1.end());
	ASSERT(map1.find('a')->second == 1);
}

void runSuite() {
	cute::suite s;

	s.push_back(CUTE(test_thread_pool));
	s.push_back(CUTE(newTestFunction));

	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The Suite");
}

int main() {
	runSuite();
	return 0;
}

