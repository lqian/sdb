//============================================================================
// Name        : test1.cpp
// Author      : link qian
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <list>
#include <sys/mman.h>
#include <vector>

#include "./common/char_buffer.h"

using namespace std;

class C1 {
protected:
	int i;
public:
	C1(int _i = 0) :
			i(_i) {
	}
	C1(const C1 & o) {
		i = o.i;
	}
	void set(int i) {
		this->i = i;
	}

	int get() {
		return i;
	}

	C1 & operator=(C1 &other) {
		if (this != &other) {
			i = other.i;
		}
		return *this;
	}
};

class C2: public C1 {
public:
	C2(const C1 & c1) :
			C1(c1) {

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

class T {
private:
	char * buff = nullptr;
	int len = 0;

public:
	T(char *buff, int len) {
		this->buff = buff;
		this->len = len;
	}

	T(T && t) {
		this->len = t.len;
		this->buff = t.buff;
		t.buff = nullptr;
		std::cout << "move...." << std::endl;
	}
};

int main() {

	C1 c1(20);
	C2 c2 = c1;
	C2 c3(c1);

	std::cout << "c2.i=" << c2.get() << std::endl;

	std::cout << "c3.i=" << c3.get() << std::endl;

	char *buff = new char[10];
	T t1(buff, 10);
	T t2 = std::move(t1);

	struct str {
		int a;
		int b;

		void foo() {
		}
		;
	};

	str str1, str2;
	str1.a = 0x34353637;
	str1.b = 0x30313233;
	str2 = str1;
	std::cout << "size of struct: " << sizeof(str2) << std::endl << " str:"
			<< str2.a << " " << str2.b << std::endl;


	char * pc = new char[sizeof(str1)];
	memcpy(pc, (char *) &str1, sizeof(str1));

	str * p_str = (str *) pc;
	std::cout << "p str: " << p_str->a << " " << p_str->b << std::endl;

	std::list<int> int_list;
	for (int i = 0; i < 10; i++) {
		int_list.push_back(i);
	}
	auto it = int_list.begin();
	std::cout << "size: " << int_list.size() << std::endl;

	for (; it != int_list.end(); it++) {
		if (*it == 6) {
			int_list.erase(it);
		}
	}

	int_list.remove(2);
	std::cout << "size: " << int_list.size() << std::endl;

	for (it = int_list.begin(); it != int_list.end(); it++) {
		std::cout << "ele: " << *it << std::endl;
	}

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

	s_parent p1;
	s_child s1;
	s1.size = 0;
	s1.add();
	cout << "s1.size:" << s1.size << endl << true << endl;

	p1.off = 200;

	unsigned short d = 1 << 15;
	cout << "unsigned short: " << d << endl;

	std::vector<int> v1;

	std::vector<int> v2;

	v2.push_back(2);
	v2.push_back(1);

	v1.insert(v1.begin(), v2.begin(), v2.end());

	for (auto it = v1.begin(); it != v1.end(); it++) {
		cout << (*it) << endl;
	}

	cout << "move 0: " << (1 >> 0 & 1 == 1) << endl;
	cout << "move 1: " << (2 >> 1 & 1 == 1) << endl;
	return 0;
}
