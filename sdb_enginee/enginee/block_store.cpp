/*
 * block_store.cpp
 *
 *  Created on: Jan 8, 2015
 *      Author: linkqian
 */

#include "block_store.h"

namespace enginee {

bool BlockStore::append_to_buff(RowStore * rs) {
	bool append = false;

	if (block_buff == NULL || block_buff_cap == 0)
		return append;

	rs->set_status(enginee::ROW_STORE_IN_USAGE);
	char_buffer row_buff(row_store_size);  // TODO multiple row

	rs->fill_char_buffer(&row_buff);

	int block_remain = block_size - tail - row_buff.size();
	if (!is_full() && block_remain >= row_store_size) {
		if (block_buff->free() > row_store_size) {
			block_buff->push_back(row_buff);
			bufferred_rows++;
			append = true;
			usage_percent = (tail + block_buff->size()) * 100 / block_size;
		}
	}

	return append;
}

void BlockStore::fill_head() {
	block_store_head->rewind();
	block_store_head->push_back(block_store_id);
	block_store_head->push_back(name_space);
	block_store_head->push_back(row_store_size);
	block_store_head->push_back(status);
	block_store_head->push_back(start_pos);
	block_store_head->push_back(end_pos);
	block_store_head->push_back(pre_block_store_pos);
	block_store_head->push_back(next_block_store_pos);
	block_store_head->push_back(block_size);
	block_store_head->push_back(tail);
	block_store_head->push_back(usage_percent);
	block_store_head->push_back(create_time);
	block_store_head->push_back(update_time);
	block_store_head->push_back(assign_time);
}

void BlockStore::read_head() {
	block_store_head->rewind();
	block_store_id = block_store_head->pop_long();
	name_space = block_store_head->pop_string();
	row_store_size = block_store_head->pop_int();
	status = block_store_head->pop_short();
	start_pos = block_store_head->pop_long();
	end_pos = block_store_head->pop_long();
	pre_block_store_pos = block_store_head->pop_long();
	next_block_store_pos = block_store_head->pop_long();
	block_size = block_store_head->pop_int();
	tail = block_store_head->pop_int();
	usage_percent = block_store_head->pop_int();
	create_time = block_store_head->pop_long();
	update_time = block_store_head->pop_long();
	assign_time = block_store_head->pop_long();
}

} /* namespace enginee */
