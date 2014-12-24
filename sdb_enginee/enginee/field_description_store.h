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
#include "./../common/store_vector.h"

namespace enginee {
class field_description_store: protected enginee::field_description {
	static const int field_store_block_size = 512;

public:

	explicit field_description_store( const std::string & _field_name, const sdb_table_field_type _field_type,
			const std::string & _comment, const bool _deleted): field_description(_field_name, _field_type, _comment, _deleted) {
		}

	explicit field_description_store(const field_description & fd): field_description(fd) {
//		this->table_description = fd.table_description;
//		this->field_name = fd.field_name;
//		this->field_type = fd.field_type;
//		this->deleted = fd.deleted;
//		this->comment = fd.comment;
	}

	const int block_size() {
		return store.size();
	}

	void fill_store() {
		store << pre_store_block_start_pos << next_store_block_start_pos << field_name;
		int t = field_type;
		store << deleted << t << comment;

	}

	void set_pre_store_block_pos(int pos) {
		pre_store_block_start_pos = pos;
	}

	void set_next_store_block_pos(int pos) {
		next_store_block_start_pos = pos;
	}

	const int evaluate_block_size() {
		return INT_CHARS * 5 + field_name.size() + comment.size() + BOOL_CHARS;
	}

	common::char_buffer&  char_buffer() {
		return store;
	}

private:

	common::char_buffer store = common::char_buffer(field_store_block_size);
	int pre_store_block_start_pos = 0;
	int next_store_block_start_pos = 0;
};
}

#endif /* FIELD_DESCRIPTION_STORE_H_ */
