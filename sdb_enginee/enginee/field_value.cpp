/*
 * field_value_store.cpp
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#include "field_value.h"
#include "../common/encoding.h"

namespace enginee {

void FieldValue::set_bool(const bool & val) {
	len = BOOL_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_short(const short & val) {
	len = SHORT_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_int(const int & val) {
	len = INT_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_long(const long & val) {
	len = LONG_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_time(const time_t & val) {
	len = LONG_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_float(const float & val) {
	len = FLOAT_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_double(const double & val) {
	len = DOUBLE_CHARS;
	value = new char[len];
	memcpy(value, to_chars(val), len);
}

void FieldValue::set_cstr(const char *p, int l) {
	len = l;
	value = new char[len];
	memcpy(value, p, len);
}

void FieldValue::set_string(const string & val) {
	len = val.size();
	value = new char[len];
	memcpy(value, val.c_str(), len);
}

bool FieldValue::read_buffer(common::char_buffer * buff) {
	value = buff->pop_chars(len);
	return len > 0;
}

void FieldValue::fill_buffer(common::char_buffer * buff) {
	buff->push_back(value, len);
}

bool FieldValue::bool_val() {
	return to_bool(value);
}

short FieldValue::short_val() {
	return to_short(value);
}
int FieldValue::int_val() {
	return to_int(value);
}
long FieldValue::long_val() {
	return to_long(value);
}

float FieldValue::float_val() {
	return to_float(value);
}

double FieldValue::double_val() {
	return to_double(value);
}

const char* FieldValue::cstr_val() {
	return value;
}

std::string FieldValue::string_val() {
	return std::string(value);
}

time_t FieldValue::time_val() {
	return to_long(value);
}
} /* namespace enginee */
