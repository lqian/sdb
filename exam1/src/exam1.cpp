//============================================================================
// Name        : exam1.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <thread>
#include <functional>
#include "Worker.h"

using namespace std;
using namespace sdb::common;

struct C {
	void foo() {
		cout << "foo" << endl;
	}
};

int main() {
	Worker w;
	std::function<void()> void_fun = std::bind<void()>(&Worker::execute);
	C bar;
	std::function<void()> func_exec = std::bind(&C::foo, &bar);
	func_exec();
	std::thread w_thread(w);
	w_thread.join();
	return 0;
}
