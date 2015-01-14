/*
 * row_store.cpp
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#include "row_store.h"
#include "./../common/char_buffer.h"

namespace enginee {

using namespace common;

bool RowStore::exist_filed(const FieldDescription & desc) {
	std::map<short, FieldValue>::const_iterator it = field_value_map.find(desc.get_inner_key());
	return it != field_value_map.end();
}

bool RowStore::exist_filed(const short & key) {
	std::map<short, FieldValue>::const_iterator it = field_value_map.find(key);
	return it != field_value_map.end();
}

void RowStore::add_field_value_store(const FieldDescription & desc, const FieldValue & val) {
	field_value_map.insert(std::pair<short, FieldValue>(desc.get_inner_key(), val));
}

void RowStore::add_field_value_store(const short & key, const FieldValue & val) {
	field_value_map.insert(std::pair<short, FieldValue>(key, val));
}

void RowStore::delete_field_value_store(const short & key) {
	field_value_map.erase(key);
}

void RowStore::delete_field_value_store(const FieldDescription & desc) {
	field_value_map.erase(desc.get_inner_key());
}

void RowStore::set_field_value_store(const FieldDescription & desc, const FieldValue & val) {
	field_value_map.insert(std::pair<short, FieldValue>(desc.get_inner_key(), val));
}

void RowStore::fill_char_buffer(char_buffer * p_buffer) {
	char_buffer h_buff(512), v_buff(512);
	int fs = field_value_map.size();
	h_buff << fs;

	std::map<short, FieldValue>::iterator it;
	for (it = field_value_map.begin(); it != field_value_map.end(); it++) {
		h_buff.push_back(it->first);
		v_buff.push_back(it->second.get_value(), it->second.get_len());
	}

	content_size = h_buff.size() + (INT_CHARS * 2) + v_buff.size();

	p_buffer->push_back(status)->push_back(content_size)->push_back(extended);
	p_buffer->push_back(h_buff)->push_back(v_buff);
}

void RowStore::read_char_buffer(char_buffer *p_buff) {
	common::char_buffer h_head(512), v_buff(1024);
	status = p_buff->pop_int();
	content_size = p_buff->pop_int();
	extended = p_buff->pop_int();
	p_buff->pop_char_buffer(&h_head);
	p_buff->pop_char_buffer(&v_buff);

	field_value_map.clear();
	int field_size = h_head.pop_int();
	for (int i = 0; i < field_size; i++) {
		FieldValue v;
		if (v.read_buffer(&v_buff) > 0) {
			short key = h_head.pop_short();
			field_value_map.insert(std::pair<short, FieldValue>(key, v));
		}
	}
}

void RowStore::read_char_buffer(const char * p, int len) {
	common::char_buffer row_buff(p, len);
	read_char_buffer(&row_buff);
}

RowStore::~RowStore() {
}

} /* namespace enginee */

