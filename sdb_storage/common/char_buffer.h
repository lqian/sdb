/*
 * author linkqian
 *
 * To change this generated comment edit the template variable "comment":
 * Window > Preferences > C/C++ > Editor > Templates.
 */

#ifndef CHAR_BUFFER_H
#define CHAR_BUFFER_H

#include <string>
#include <cstring>
#include <ctime>
#include "encoding.h"

/**
 * a dynamic char buffer that push/pop bool, int, long, float, double, string, char array
 */
namespace sdb {
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

	bool ref = false;

public:

	char_buffer() {
	}

	char_buffer(const char * buff, int size) {
		cap = size;
		buffer = new char[cap];
		memcpy(buffer, buff, cap);
	}

	char_buffer(char * buff, int size, bool ref = false) {
		if (ref) {
			this->ref = ref;
			cap = size;
			buffer = buff;
		} else {
			cap = size;
			buffer = new char[cap];
			memcpy(buffer, buff, cap);
		}
	}

	explicit char_buffer(int capacity) {
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

	char_buffer(char_buffer && other) :
			cap(other.cap), b_size(other.b_size), grow_factor(other.grow_factor) {
		this->buffer = other.buffer;
		other.buffer = nullptr;
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

	void ref_buff(char * buff, int size) {
		this->ref = true;
		cap = size;
		this->b_size = size;
		buffer = buff;
	}

	~char_buffer() {
		if (!ref) {
			delete[] buffer;
		}
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
	const int remain() const {
		return b_size - pop_pos;
	}

	/*
	 * size of chars has'nt push or assign value
	 */
	const int free() const {
		return cap - push_pos;
	}

	/**
	 * actual char size of current buffer
	 */
	int size() const {
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

	char_buffer * reset() {
		push_pos = 0;
		pop_pos = 0;
		b_size = 0;
		return this;
	}

	/**
	 * skip off size of chars from current pop position
	 */
	char_buffer * skip(int off) {
		pop_pos += off;
		return this;
	}

	char_buffer * push_back(const char & val) {
		if (!ref) {
			ensure_capacity(CHAR_LEN);
		}
		push_chars(&val, CHAR_LEN);
		return this;
	}

	char_buffer * push_back(const unsigned char & val) {
		char c = (char) val;
		return push_back(c);
	}

	char_buffer * push_back(const short & val) {
		if (!ref) {
			ensure_capacity(SHORT_CHARS);
		}
		push_chars(to_chars(val), SHORT_CHARS);
		return this;
	}

	char_buffer * push_back(const unsigned short & val) {
		if (!ref) {
			ensure_capacity(SHORT_CHARS);
		}
		push_chars(to_chars(val), SHORT_CHARS);
		return this;
	}
	char_buffer * push_back(const int & val) {
		if (!ref) {
			ensure_capacity(INT_CHARS);
		}
		push_chars(to_chars(val), INT_CHARS);
		return this;
	}

	char_buffer * push_back(const unsigned int & val) {
		if (!ref) {
			ensure_capacity(INT_CHARS);
		}
		push_chars(to_chars(val), INT_CHARS);
		return this;
	}

	char_buffer * push_back(const long & val) {
		if (!ref) {
			ensure_capacity(LONG_CHARS);
		}
		push_chars(to_chars(val), LONG_CHARS);
		return this;
	}

	char_buffer * push_back(const unsigned long & val) {
		if (!ref) {
			ensure_capacity(UNSIGNED_LONG_CHARS);
		}
		push_chars(to_chars(val), UNSIGNED_LONG_CHARS);
		return this;
	}

	char_buffer * push_back(const bool &val) {
		if (!ref) {
			ensure_capacity(BOOL_CHARS);
		}
		buffer[push_pos++] = val ? 1 : 0;
		++b_size;
		return this;
	}

	char_buffer * push_back(const float& val) {
		if (!ref) {
			ensure_capacity(FLOAT_CHARS);
		}
		push_chars(to_chars(val), FLOAT_CHARS);
		return this;
	}

	char_buffer * push_back(const double& val) {
		if (!ref) {
			ensure_capacity(DOUBLE_CHARS);
		}
		push_chars(to_chars(val), DOUBLE_CHARS);
		return this;
	}

	char_buffer * push_back(const std::string & str) {
		int size = str.size();
		push_back(str.c_str(), size);
		return this;
	}

	char_buffer * push_back(const char * chars, int len, bool with_len = true) {
		if (with_len) {
			if (!ref) {
				ensure_capacity(len + INT_CHARS);
			}
			push_chars(to_chars(len), INT_CHARS);
		} else {
			if (!ref) {
				ensure_capacity(len);
			}
		}
		push_chars(chars, len);
		return this;
	}

	char_buffer * push_back(const char_buffer & other) {
		if (!ref) {
			ensure_capacity(other.b_size + INT_CHARS);
		}
		push_chars(to_chars(other.b_size), INT_CHARS);
		push_chars(other.buffer, other.b_size);
		return this;
	}

	const short pop_short() {
		short *p = (short *) (buffer + pop_pos);
		pop_pos += SHORT_CHARS;
		return (*p);
	}

	const unsigned short pop_unsigned_short() {
		unsigned short *p = (unsigned short *) (buffer + pop_pos);
		pop_pos += SHORT_CHARS;
		return (*p);
	}

	const char pop_char() {
		char *p = (buffer + pop_pos);
		pop_pos += CHAR_LEN;
		return (*p);
	}

	const unsigned char pop_uchar() {
		return (unsigned char) pop_char();
	}

	bool pop_bool() {
		return buffer[pop_pos++] == 1;
	}
	const int pop_int() {
		int * p = (int *) (buffer + pop_pos);
		pop_pos += INT_CHARS;
		return (*p);
	}

	const unsigned int pop_uint() {
		unsigned * p = (unsigned *) (buffer + pop_pos);
		pop_pos += INT_CHARS;
		return (*p);
	}

	std::string pop_string() {
		int len = pop_int();
		std::string str(buffer + pop_pos, len);
		pop_pos += len;
		return str;
	}

	void pop_cstr(char * p) {
		int len = pop_int();
		p = new char[len];
		memcpy(p, buffer + pop_pos, len);
		pop_pos += len;
	}

	void pop_cstr(char * p, int len, bool with_alloc = true) {
		if (with_alloc) {
			p = new char[len];
		}
		memcpy(p, buffer + pop_pos, len);
		pop_pos += len;
	}

	/**
	 * pop int to len, and next length of chars
	 */
	char * pop_chars(int & len) {
		len = pop_int();
		if (len > 0) {
			char *p = new char[len];
			memcpy(p, buffer + pop_pos, len);
			pop_pos += len;
			return p;
		} else
			return NULL;
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

	unsigned long pop_unsigned_long() {
		unsigned long * p = (unsigned long *) (buffer + pop_pos);
		pop_pos += UNSIGNED_LONG_CHARS;
		return (*p);
	}

	bool pop_char_buffer(char_buffer* p_buffer) {
		int len = pop_int();
		if (len <= 0)
			return false;
		memcpy(p_buffer->buffer, buffer + pop_pos, len);
		pop_pos += len;
		p_buffer->rewind();
		p_buffer->b_size = len;
		return true;
	}

	char * data() {
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

	friend char_buffer& operator<<(char_buffer & buff,
			const unsigned char & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const short & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff,
			const unsigned short & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff, const int & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff,
			const unsigned int & val) {
		buff.push_back(val);
		return buff;
	}

	friend char_buffer& operator<<(char_buffer & buff,
			const unsigned long & val) {
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

	friend char_buffer& operator>>(char_buffer & buff, unsigned char & val) {
		val = buff.pop_uchar();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, unsigned short & val) {
		val = buff.pop_unsigned_short();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, short & val) {
		val = buff.pop_short();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, int & val) {
		val = buff.pop_int();
		return buff;
	}

	friend char_buffer & operator>>(char_buffer & buff, unsigned & val) {
		val = buff.pop_uint();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, long & val) {
		val = buff.pop_long();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, unsigned long & val) {
		val = buff.pop_unsigned_long();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, bool & val) {
		val = buff.pop_bool();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, char * val) {
		int len = buff.pop_int();
		buff.pop_cstr(val);
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, char & val) {
		val = buff.pop_char();
		return buff;
	}

	friend char_buffer& operator>>(char_buffer & buff, char_buffer & val) {
		buff.pop_char_buffer(&val);
		return buff;
	}

	char_buffer & operator=(const char_buffer & other) {
		cap = other.cap;
		b_size = other.b_size;
		pop_pos = other.pop_pos;
		push_pos = other.push_pos;
		buffer = new char[b_size];
		memcpy(buffer, other.buffer, b_size);
		return *this;
	}

	void push_chars(const char* chars, int len) {
		if (len == BOOL_CHARS) {
			buffer[push_pos++] = chars[0];
//			if (chars[0] == 1) {
//				cout << "this is a c++ bug" << endl;
//			}
		} else {
			memcpy(buffer + push_pos, chars, len);
			push_pos += len;
		}

		b_size += len;
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
}
;
}
}

#endif
