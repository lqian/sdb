/*
 * block_store.cpp
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#include "block_store.h"

namespace enginee {

void block_store::fill_block_head(common::char_buffer & buff) {
	common::char_buffer tmp(512);
	tmp << block_store_id << row_store_size << status << name_space << start_pos << end_pos;
	tmp << pre_block_store_pos << next_block_store_pos << block_size << tail << usage_percent ;
	tmp << create_time << update_time << assign_time ;

	//push the tmp buffer
	buff << tmp;

}

block_store::~block_store() {
}

} /* namespace enginee */
