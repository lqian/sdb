/*
 * row_store.h
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#ifndef ROW_STORE_H_
#define ROW_STORE_H_

#include <map>
#include <string.h>
#include "field_value_store.h"
#include "field_description.h"
#include "./../common/char_buffer.h"

namespace enginee {
using namespace std;

class row_store {

private:

	long block_start_pos;
	long block_end_pos;

	/*
	 * position offset the block_store start position
	 */
	int start_pos;
	int end_pos;

	int size;  // the row_store size

	int status;

	/*
	 * position offset the row-store-data-end-position,
	 * if current row store has extend or the it is full,
	 * its value SHOULD be end_pos - 4;
	 */
	int tail = 0;

	/*
	 * if a row value's bytes exceed a row store, the row value data spans two or more row_store,
	 * besides the last row_store, they have extend that points start_pos of the next row store.
	 */
	int extended = 0;

	map<short, field_value_store> field_value_map;

public:

	const static int un_usage = 0;
	const static int usage = 1;
	const static int deleted = 2;

	explicit row_store(const long & bsp, const long & bep, const int & start) :
			block_start_pos(bsp), block_end_pos(bep), start_pos(start) {
		size = block_end_pos - block_start_pos;
	}

	explicit row_store(const row_store & other) {
		this->block_start_pos = other.block_start_pos;
		this->block_end_pos = other.block_end_pos;
		this->start_pos = other.start_pos;
		this->status = other.status;
		this->end_pos = other.end_pos;
		this->size = other.size;
		this->tail = other.tail;
		this->extended = other.extended;
		this->field_value_map = other.field_value_map;
	}
	virtual ~row_store();

	bool exist_filed(const field_description & desc);
	bool exist_filed(const short & key);
	void add_field_value_store(const field_description & desc, const field_value_store & val);
	void add_field_value_store(const short & key, const field_value_store & val);
	void delete_field_value_store(const field_description & desc);
	void delete_field_value_store(const short & inner_key);
	void set_field_value_store(const field_description & desc, const field_value_store & val);

	void fill_char_buffer(common::char_buffer * p_buffer) const;

	int get_end_pos() const {
		return end_pos;
	}

	void set_end_pos(int endpos) {
		end_pos = endpos;
	}

	int get_extended() const {
		return extended;
	}

	void set_extended(int extended) {
		this->extended = extended;
	}

	int get_row_head_count() const {
		return field_value_map.size();
	}

	int get_size() const {
		return size;
	}

	void set_size(int size) {
		this->size = size;
	}

	int get_start_pos() const {
		return start_pos;
	}

	void set_start_pos(int startpos) {
		start_pos = startpos;
	}

	int get_status() const {
		return status;
	}

	void set_status(int status) {
		this->status = status;
	}

	int get_tail() const {
		return tail;
	}

	void set_tail(int tail) {
		this->tail = tail;
	}

	void operator=(const row_store & other) {
		this->block_start_pos = other.block_start_pos;
		this->block_end_pos = other.block_end_pos;
		this->start_pos = other.start_pos;
		this->status = other.status;
		this->end_pos = other.end_pos;
		this->size = other.size;
		this->tail = other.tail;
		this->extended = other.extended;
		this->field_value_map = other.field_value_map;
	}

	long get_block_end_pos() const {
		return block_end_pos;
	}

	void set_block_end_pos(long blockendpos) {
		block_end_pos = blockendpos;
	}

	long get_block_start_pos() const {
		return block_start_pos;
	}

	void set_block_start_pos(long blockstartpos) {
		block_start_pos = blockstartpos;
	}

	const map<short, field_value_store>& get_field_value_map() const {
		return field_value_map;
	}

	void set_field_value_map(const map<short, field_value_store>& fieldvaluemap) {
		field_value_map = fieldvaluemap;
	}
};

} /* namespace enginee */

#endif /* ROW_STORE_H_ */
