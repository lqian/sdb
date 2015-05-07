//============================================================================
// Name        : test1.cpp
// Author      : link qian
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <sys/mman.h>
#include <vector>
using namespace std;

class C1 {
private:
	int i;
public:
	void set(int i) {
		this->i = i;
	}

	int get() {
		return i;
	}
};

void foo(C1 &c) {
	C1 cp;
	cp.set(200);
	c = cp;
}

void bar(C1 c) {
	C1 cp;
	cp.set(400);
	c = cp;
}

struct s_parent {
	int off;
};

struct s_child: s_parent {
	int size;

	void add() {
		size++;
	}
};

int main() {

	enum block_size_type {
		K_4 = 4, K_8 = 8, K_16 = 16, K_32 = 32, K_64 = 64
	} bst;

	cout << "!!!Hello World!!!" << endl;

	C1 c;
	foo(c);
	cout << "reference of c:" << c.get() << endl;

	bar(c);
	cout << "reference of c:" << c.get() << endl;

	bst = K_16;
	cout << bst << endl;

	s_child s1;
	s1.size = 0;
	s1.add();
	cout << "s1.size:" << s1.size << endl << true << endl;

	unsigned short d = 1 << 15;
	cout << "unsigned short: "<< d << endl;


	std::vector<int> v1;

	std::vector<int> v2;

	v2.push_back(2);
	v2.push_back(1);

	v1.insert(v1.begin(), v2.begin(), v2.end());

	for (auto it = v1.begin(); it!=v1.end(); it++) {
		cout << (*it) << endl;
	}

	return 0;
}
