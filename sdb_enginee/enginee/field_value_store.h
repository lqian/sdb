/*
 * field_value_store.h
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#ifndef FIELD_VALUE_STORE_H_
#define FIELD_VALUE_STORE_H_

#include <cstring>
#include <ctime>

#include "sdb.h"

namespace enginee {
using namespace std;

class field_value_store {
private:
	sdb_table_field_type type;
	char * value;
	int len;
public:

	void set_bool(const bool & val);
	void set_short(const short & val);
	void set_int(const int &val);
	void set_long(const long &val);
	void set_unsigned_short(const unsigned short & val);
	void set_unsigned_int(const unsigned int & val);
	void set_unsigned_long(const unsigned long & val);
	void set_float(const float & val);
	void set_double(const float &val);
	void set_cstr(const char * p, int len);
	void set_string(const std::string & val);
	void set_time(const time_t & val);

	bool to_bool();
	short to_short();
	int to_int();
	long to_long();
	unsigned short to_unsigned_short();
	unsigned int to_unsigned_int();
	unsigned long to_unsigned_long();
	float to_float();
	double to_double();
	char * to_cstr();
	std::string to_string();
	time_t to_time();

	bool is_variant() const {
		return (type == char_type  || type == varchar_type) || type > varchar_type;
	}

	explicit field_value_store() {}
	explicit field_value_store(const sdb_table_field_type & _type):type(_type) {

	}
	explicit field_value_store(const sdb_table_field_type & _type, const char * _value, const int & _len) :
			type(_type), len(_len) {
		value = new char[len];
		memcpy(value, _value, len);
	}

	explicit field_value_store(const field_value_store & other) :
			type(other.type), len(other.len) {
		value = new char[len];
		memcpy(value, other.value, len);
	}
	virtual ~field_value_store() {
		delete[] value;
	}

	int get_len() const {
		return len;
	}

	void set_len(int len) {
		this->len = len;
	}

	sdb_table_field_type get_type() const {
		return type;
	}

	void set_type(sdb_table_field_type type) {
		this->type = type;
	}

	char* get_value() const {
		return value;
	}

	void set_value(char* value) {
		this->value = value;
	}
};

} /* namespace enginee */

#endif /* FIELD_VALUE_STORE_H_ */
