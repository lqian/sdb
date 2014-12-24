/*
 * author linkqian
 *
 * To change this generated comment edit the template variable "comment":
 * Window > Preferences > C/C++ > Editor > Templates.
 */

#ifndef STORE_VECTOR_H_
#define STORE_VECTOR_H_

#include <string>
#include <iostream>
#include "encoding.h"

/**
 * a dynamic char buffer that push/pop bool, int, long, float, double, string, char array
 */
namespace common {
using namespace std;
class char_buffer {

private:
	char * buffer;
	int push_pos = 0;
	int pop_pos = 0;
	int s = 0;
	int cap = 0;
	float grow_factor = 2;

public:
	char_buffer(int capacity) {
		this->cap = capacity;
		buffer = new char[capacity];
	}

	char_buffer(int capacity, float grow_factor) {
		this->cap = capacity;
		this->grow_factor = grow_factor;
		buffer = new char[capacity];
	}

	~char_buffer() {
		delete[] buffer;
	}

	int capacity() {
		return cap;
	}

	int header() {
		return pop_pos;
	}

	int tail() {
		return push_pos;
	}

	/**
	 * size of push chars has'nt been pop
	 */
	int remain() {
		return push_pos - pop_pos;
	}

	/**
	 * actual char size of current buffer
	 */
	int size() {
		return s;
	}

	void shrink() {
		char * new_buffer = new char[s];
		strncpy(new_buffer, buffer, s);
		cap = s;
		delete[] buffer;
		buffer = new_buffer;
	}

	char_buffer * rewind() {
		push_pos = 0;
		pop_pos = 0;
		return this;
	}
	char_buffer * push_back(const int & val) {
		ensure_capacity(INT_CHARS);
		push_chars(to_chars(val), INT_CHARS);
		return this;
	}

	char_buffer * push_back(const long & val) {
		ensure_capacity(LONG_CHARS);
		push_chars(to_chars(val), LONG_CHARS);
		return this;
	}

	char_buffer * push_back(const bool &val) {
		ensure_capacity(BOOL_CHARS);
		push_chars(to_chars(val), BOOL_CHARS);
		return this;
	}

	char_buffer * push_back(const float& val) {
		ensure_capacity(FLOAT_CHARS);
		push_chars(to_chars(val), FLOAT_CHARS);
		return this;
	}

	char_buffer * push_back(const double& val) {
		ensure_capacity(DOUBLE_CHARS);
		push_chars(to_chars(val), DOUBLE_CHARS);
		return this;
	}

	char_buffer * push_back(const std::string & str) {
		int size = str.size();
		push_back(str.c_str(), size);
		return this;
	}

	char_buffer * push_back(const char * chars, int len) {
		ensure_capacity(len + INT_CHARS);
		push_chars(to_chars(len), INT_CHARS);
		push_chars(chars, len);
		return this;
	}

	bool pop_bool() {
		return buffer[pop_pos++] == 1;
	}
	const int & pop_int() {
		int * p = (int *) (buffer + pop_pos);
		pop_pos += INT_CHARS;
		return (*p);
	}

	std::string pop_string() {
		return std::string(pop_cstr());
	}

	const char * pop_cstr() {
		int len = pop_int();
		char *p = new char[len];
		strncpy(p, buffer + pop_pos, len);
		pop_pos += len;
		return p;
	}

	float pop_float() {
		float * p = (float *) (buffer + pop_pos);
		pop_pos += FLOAT_CHARS;
		return (*p);
	}

	long pop_long() {
		long * p = (long *) (buffer + pop_pos);
		pop_pos += LONG_CHARS;
		return (*p);
	}

	char * data() {
		return buffer;
	}

	friend char_buffer& operator<<(char_buffer & buff, bool & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, int & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, long & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, float & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, double & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, std::string & val) {
		buff.push_back(val);
		return buff;
	}

protected:
	void ensure_capacity(int len) {
		while (s + len >= cap) {
			cap *= grow_factor;
		}
		char * new_buffer = new char[cap];
		strcpy(new_buffer, buffer);
		delete[] buffer;
		buffer = new_buffer;
	}

	void push_chars(const char* chars, int len) {
		for (int i = 0; i < len; i++) {
			buffer[push_pos++] = chars[i];
		}

		s += len;
	}
}
;
}

#endif
