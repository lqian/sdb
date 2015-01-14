/*
 * field_value_store.h
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#ifndef FIELD_VALUE_STORE_H_
#define FIELD_VALUE_STORE_H_

#include <string.h>
#include <ctime>
#include "sdb.h"
#include "./../common/char_buffer.h"

namespace enginee {
using namespace std;

class FieldValue {
private:
	sdb_table_field_type type;
	char * value;
	int len = 0;
public:

	FieldValue() {
	}

	FieldValue(const FieldValue & other) :
			type(other.type), len(other.len) {
		value = new char[len];
		memcpy(value, other.value, len);
	}

	explicit FieldValue(const sdb_table_field_type & _type) :
			type(_type) {
	}

	explicit FieldValue(const sdb_table_field_type & _type, const char * _value, const int & _len) :
			type(_type), len(_len) {
		value = new char[len];
		memcpy(value, _value, _len);
	}

	void operator=(const FieldValue & other) {
		type = other.type;
		len = other.len;
		value = new char[len];
		memcpy(value, other.value, len);
	}

	void set_bool(const bool & val);
	void set_short(const short & val);
	void set_int(const int &val);
	void set_long(const long &val);
//	void set_unsigned_short(const unsigned short & val);
//	void set_unsigned_int(const unsigned int & val);
//	void set_unsigned_long(const unsigned long & val);
	void set_float(const float & val);
	void set_double(const double &val);
	void set_cstr(const char * p, int len);
	void set_string(const std::string & val);
	void set_time(const time_t & val);

	bool bool_val();
	short short_val();
	int int_val();
	long long_val();
//	unsigned short to_unsigned_short();
//	unsigned int to_unsigned_int();
//	unsigned long to_unsigned_long();
	float float_val();
	double double_val();
	const char * cstr_val();
	std::string string_val();
	time_t time_val();

	bool is_variant() const {
		return (type == char_type || type == varchar_type) || type > varchar_type;
	}

	bool read_buffer(common::char_buffer * buff);

	void fill_buffer(common::char_buffer * buff);

	virtual ~FieldValue() {
		if (len > 0) {
			delete[] value;
		}
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

	const char* get_value() const {
		return value;
	}

	void set_value(char* value) {
		this->value = value;
	}
};

} /* namespace enginee */

#endif /* FIELD_VALUE_STORE_H_ */
