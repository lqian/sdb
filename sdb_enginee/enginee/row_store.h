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
#include "field_value.h"
#include "field_description.h"
#include "./../common/char_buffer.h"

namespace enginee {
using namespace std;

const static int ROW_STORE_IN_USAGE = 1;
const static int ROW_STORE_DELETED = -1;
const static int ROW_STORE_HEAD = 12;

class RowStore {

private:

	int status;

	int content_size;  // the row_store content size, excludes ROW_STORE_HEAD, maybe exceeds a row-store-size

	/*
	 * position offset the block_store start position, include BLOCK_HEAD_LENGTH
	 */
	int start_pos;
	int end_pos;

	/*
	 * for index
	 */
	long block_start_pos;
	long block_end_pos;
	/*
	 * if a row value's bytes exceed a row store, the row value data spans two or more row_store,
	 * besides the last row_store, they have extend that points start_pos of the next row store.
	 */
	int extended = -1;

	map<short, FieldValue> field_value_map;

public:

	RowStore() {

	}
	explicit RowStore(const long & bsp, const long & bep, const int & start) :
			block_start_pos(bsp), block_end_pos(bep), start_pos(start) {
		content_size = block_end_pos - block_start_pos;
	}

	explicit RowStore(const RowStore & other) {
		this->block_start_pos = other.block_start_pos;
		this->block_end_pos = other.block_end_pos;
		this->start_pos = other.start_pos;
		this->status = other.status;
		this->end_pos = other.end_pos;
		this->content_size = other.content_size;
		this->extended = other.extended;
		this->field_value_map = other.field_value_map;
	}
	virtual ~RowStore();

	bool exist_filed(const FieldDescription & desc);
	bool exist_filed(const short & key);
	void add_field_value_store(const FieldDescription & desc, const FieldValue & val);
	void add_field_value_store(const short & key, const FieldValue & val);
	void delete_field_value_store(const FieldDescription & desc);
	void delete_field_value_store(const short & inner_key);
	void set_field_value_store(const FieldDescription & desc, const FieldValue & val);

	void fill_char_buffer(common::char_buffer * p_buffer);

	void read_char_buffer(common::char_buffer * p_buffer);
	void read_char_buffer(const char * p, int len);



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

	FieldValue & get_field_value(short key)  {
		return field_value_map.at(key);
	}

	int get_row_head_count() const {
		return field_value_map.size();
	}

	int get_content_size() const {
		return content_size;
	}

	void set_size(int size) {
		this->content_size = size;
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

	long get_stream_pos() {
		return block_start_pos + start_pos;
	}

	void set_status(int status) {
		this->status = status;
	}

	bool has_extended() {
		return extended != -1;
	}

	const bool can_search() {
		return block_start_pos > 0 && start_pos > 0;
	}

	void operator=(const RowStore & other) {
		this->block_start_pos = other.block_start_pos;
		this->block_end_pos = other.block_end_pos;
		this->start_pos = other.start_pos;
		this->status = other.status;
		this->end_pos = other.end_pos;
		this->content_size = other.content_size;
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

	 map<short, FieldValue>& get_field_value_map() {
		return field_value_map;
	}

	void set_field_value_map(const map<short, FieldValue>& fieldvaluemap) {
		field_value_map = fieldvaluemap;
	}
};

} /* namespace enginee */

#endif /* ROW_STORE_H_ */
