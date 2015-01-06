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

bool row_store::exist_filed(const field_description & desc) {
	std::map<short, field_value_store>::const_iterator it = field_value_map.find(desc.get_inner_key());
	return it != field_value_map.end();
}

bool row_store::exist_filed(const short & key) {
	std::map<short, field_value_store>::const_iterator it = field_value_map.find(key);
	return it != field_value_map.end();
}

void row_store::add_field_value_store(const field_description & desc, const field_value_store & val) {
	field_value_map.insert(std::pair<short, field_value_store>(desc.get_inner_key(), val));
}

void row_store::add_field_value_store(const short & key, const field_value_store & val) {
	field_value_map.insert(std::pair<short, field_value_store>(key, val));
}

void row_store::delete_field_value_store(const short & key) {
	field_value_map.erase(key);
}

void row_store::delete_field_value_store(const field_description & desc) {
	field_value_map.erase(desc.get_inner_key());
}

void row_store::set_field_value_store(const field_description & desc, const field_value_store & val) {
	field_value_map.insert(std::pair<short, field_value_store>(desc.get_inner_key(), val));
}

void row_store::fill_char_buffer(char_buffer * p_buffer) const {
	char_buffer h_buff(512), v_buff(size);
	h_buff << start_pos << end_pos << size << status;
	for (std::map<short, field_value_store>::const_iterator it = field_value_map.begin(); it != field_value_map.end(); it++) {
		h_buff.push_back(it->first);
		field_value_store val(it->second);
		if (val.is_variant()) {
			v_buff.push_back(val.get_value(), val.get_len());
		} else {
			v_buff.push_chars(val.get_value(), val.get_len());
		}
	}

	int fs = field_value_map.size();
	int i_tail = h_buff.size() + (INT_CHARS * 3) + v_buff.size();
	h_buff << i_tail << extended << fs;

	p_buffer->push_back(h_buff)->push_back(v_buff);
}

row_store::~row_store() {
}

} /* namespace enginee */

