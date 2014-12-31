/*
 * field_description_store.h
 *
 *  Created on: Dec 16, 2014
 *      Author: linkqian
 */

#ifndef FIELD_DESCRIPTION_STORE_H_
#define FIELD_DESCRIPTION_STORE_H_

#include <vector>
#include "field_description.h"
#include <iostream>
#include "./../common/char_buffer.h"

namespace enginee {
class field_description_store: public enginee::field_description {
	static const int field_store_block_size = 128;

public:

	field_description_store() {
	}
	explicit field_description_store(const std::string & _field_name,
			const sdb_table_field_type _field_type,
			const std::string & _comment, const bool _deleted) :
			field_description(_field_name, _field_type, _comment, _deleted) {
	}

	explicit field_description_store(const field_description & fd) :
			field_description(fd) {
	}

	const int block_size() {
		return store.size();
	}

	void fill_store() {
		store << pre_store_block_start_pos << next_store_block_start_pos << inner_key
				<< field_name << deleted;
		int t = field_type;
		store << t << comment;
	}

	/**
	 * read members from store
	 */
	void read_store() {
		store >> pre_store_block_start_pos >> next_store_block_start_pos >> inner_key
				>> field_name >> deleted;
		int t(0);
//		comment = store.pop_string();
		store >> t >> comment;
		field_type = (sdb_table_field_type) t;
	}

	void set_pre_store_block_pos(int pos) {
		pre_store_block_start_pos = pos;
	}

	void set_next_store_block_pos(int pos) {
		next_store_block_start_pos = pos;
	}

	const int evaluate_block_size() {
		return INT_CHARS * 5 + field_name.size() + comment.size() + BOOL_CHARS + SHORT_CHARS;
	}

	common::char_buffer& char_buffer() {
		return store;
	}

	void set_store(common::char_buffer & buff) {
		store = buff;
	}

private:
	common::char_buffer store =  common::char_buffer(field_store_block_size);
	int pre_store_block_start_pos = 0;
	int next_store_block_start_pos = 0;
};
}

#endif /* FIELD_DESCRIPTION_STORE_H_ */
