/*
 * author linkqian
 *
 * To change this generated comment edit the template variable "comment":
 * Window > Preferences > C/C++ > Editor > Templates.
 */

#ifndef CHAR_BUFFER_H
#define CHAR_BUFFER_H

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
	int b_size = 0;
	int cap = 0;
	float grow_factor = 2;

public:

	char_buffer(char * buff, int size) {
		cap = size;
		buffer = new char[cap];
		memcpy(buffer, buff, cap);
	}

	char_buffer(int capacity) {
		this->cap = capacity;
		buffer = new char[capacity];
	}

	char_buffer(int capacity, float grow_factor) {
		this->cap = capacity;
		this->grow_factor = grow_factor;
		buffer = new char[capacity];
	}

	char_buffer(const char_buffer & other) :
			cap(other.cap), b_size(other.b_size), grow_factor(other.grow_factor) {
		buffer = new char[cap];
		memcpy(buffer, other.buffer, b_size);
	}

	char_buffer(const char_buffer & other, const bool shrinking) :
			cap(other.cap), b_size(other.b_size), grow_factor(other.grow_factor) {
		if (shrinking) {
			buffer = new char[b_size];
		} else {
			buffer = new char[cap];
		}
		memcpy(buffer, other.buffer, b_size);
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
		return b_size;
	}

	void shrink() {
		char * new_buffer = new char[b_size];
		memcpy(new_buffer, buffer, b_size);
		cap = b_size;
		delete[] buffer;
		buffer = new_buffer;
	}

	char_buffer * rewind() {
		push_pos = 0;
		pop_pos = 0;
		return this;
	}

	/**
	 * skip off size of chars from current pop position
	 */
	char_buffer * skip(int off) {
		pop_pos += off;
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

	char_buffer * push_back(const char_buffer & other) {
		ensure_capacity(other.b_size + INT_CHARS);
		push_chars(to_chars(other.b_size), INT_CHARS);
		push_chars(other.buffer, other.b_size);
		return this;
	}

	const short pop_short() {
		short *p = (short *) (buffer + pop_pos);
		pop_pos += SHORT_CHARS;
		return (*p);
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

	char * pop_cstr() {
		int len = pop_int();
		char *p = new char[len];
		memcpy(p, buffer + pop_pos, len);
		if (strlen(p) > len) {
			p[len] = '\0';
		}
		pop_pos += len;
		return p;
	}

	float pop_float() {
		float * p = (float *) (buffer + pop_pos);
		pop_pos += FLOAT_CHARS;
		return (*p);
	}

	double pop_double() {
		double * p = (double *) (buffer + pop_pos);
		pop_pos += DOUBLE_CHARS;
		return (*p);
	}

	long pop_long() {
		long * p = (long *) (buffer + pop_pos);
		pop_pos += LONG_CHARS;
		return (*p);
	}

	bool pop_char_buffer(char_buffer* p_buffer) {
		int len = pop_int();
		if (len <= 0)
			return false;
		char *p = new char[len];
		memcpy(p, buffer + pop_pos, len);
		pop_pos += len;
		p_buffer->set_buffer(p);
		p_buffer->rewind();
		p_buffer->b_size = len;
		return true;
	}

	const char * data() {
		return buffer;
	}

	char * copy_slice(int start, int size) {
		char * slice = new char[size];
		memcpy(slice, buffer + start, size);
		return slice;
	}

	void set_buffer(char * buff) {
		buffer = buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const bool & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const int & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const long & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const float & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const double & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff,
			const std::string & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff,
			const char_buffer & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, std::string & val) {
		val = buff.pop_string();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, float & val) {
		val = buff.pop_float();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, double & val) {
		val = buff.pop_double();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, int & val) {
		val = buff.pop_int();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, long & val) {
		val = buff.pop_long();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, bool & val) {
		val = buff.pop_bool();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, char * val) {
		val = buff.pop_cstr();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, short & val) {
		val = buff.pop_short();
		return buff;
	}

	char_buffer & operator=(const char_buffer & other) {
		cap = other.cap;
		b_size = other.b_size;
		pop_pos = other.pop_pos;
		push_pos = other.push_pos;
		buffer = new char[b_size];
		std::memcpy(buffer, other.buffer, b_size);
		return *this;
	}

protected:
	void ensure_capacity(int len) {
		while (b_size + len >= cap) {
			cap *= grow_factor;
		}
		char * new_buffer = new char[cap];
		memcpy(new_buffer, buffer, b_size);
		delete[] buffer;
		buffer = new_buffer;
	}

	void push_chars(const char* chars, int len) {
		if (len == BOOL_CHARS) {
			buffer[push_pos++] = chars[0];
			if (chars[0] == 1) {
				cout << "this is a c++ bug" << endl;
			}
//			buffer[push_pos++] = *chars;

		} else {
			for (int i = 0; i < len;) {
				buffer[push_pos++] = chars[i++];
			}
		}

		b_size += len;
	}
}
;
}

#endif
