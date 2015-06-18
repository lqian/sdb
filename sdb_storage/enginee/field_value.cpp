/*
 * field_value_store.cpp
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#include "field_value.h"
#include "../common/encoding.h"

namespace sdb {
namespace enginee {

void field_value::set_bool(const bool & val) {
	type = bool_type;
	len = BOOL_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_short(const short & val) {
	type = short_type;
	len = SHORT_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_ushort(const unsigned short & val) {
	type = unsigned_short_type;
	len = SHORT_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}
void field_value::set_uint(const unsigned int & val) {
	type = unsigned_int_type;
	len = INT_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}
void field_value::set_ulong(const unsigned long & val) {
	type = unsigned_long_type;
	len = LONG_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}
void field_value::set_int(const int & val) {
	type = int_type;
	len = INT_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_long(const long & val) {
	type = long_type;
	len = LONG_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_time(const time_t & val) {
	type = time_type;
	len = LONG_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_float(const float & val) {
	type = float_type;
	len = FLOAT_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_double(const double & val) {
	type = double_type;
	len = DOUBLE_CHARS;
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

void field_value::set_cstr(const char *p, int l) {
	if (type == unknow_type) {
		type = varchar_type;
	}
	len = l;
	value = new char[len];
	memcpy(value, p, len);
}

void field_value::set_string(const string & val) {
	if (type == unknow_type) {
		type = varchar_type;
	}
	len = val.size();
	value = new char[len];
	memcpy(value, (char*) &val, len);
}

bool field_value::read_buffer(common::char_buffer * buff) {
	value = buff->pop_chars(len);
	return len > 0;
}

void field_value::fill_buffer(common::char_buffer * buff) {
	buff->push_back(value, len);
}

bool field_value::bool_val() {
	return to_bool(value);
}

short field_value::short_val() {
	return to_short(value);
}
int field_value::int_val() {
	return to_int(value);
}
long field_value::long_val() {
	return to_long(value);
}

float field_value::float_val() {
	return to_float(value);
}

double field_value::double_val() {
	return to_double(value);
}

const char* field_value::cstr_val() {
	return value;
}

std::string field_value::string_val() {
	return std::string(value);
}

time_t field_value::time_val() {
	return to_long(value);
}

unsigned short field_value::ushort_val() {
	return to_ushort(value);
}
unsigned int field_value::uint_val() {
	return to_uint(value);
}
unsigned long field_value::ulong_val() {
	return to_ulong(value);
}

} /* namespace enginee */
} /* namespace sdb */
