/*
 * table_store.h
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#ifndef TABLE_STORE_H_
#define TABLE_STORE_H_

#include <string.h>
#include <time.h>
#include "row_store.h"
#include "block_store.h"
#include "table_description.h"
#include "../common/char_buffer.h"
#include <fstream>

namespace enginee {

using namespace std;
using namespace common;

const std::string table_store_file_extension(".data");
const std::string table_lock_file_extension(".lock");
const int table_head_length(1024);


class table_store {

private:
	table_description tbl_desc;
	std::string full_path; // the full path of file stores table data
	std::string lock_path;
	sdb_table_status status;

	time_t create_time;
	time_t update_time;

	long block_store_off;  // offset includes last block length, meanwhile it's the next new block start off

	long head_block_store_pos;
	long tail_block_store_pos;
	long block_count;


	fstream table_stream;

	char_buffer head_buffer;

	block_store head_block_store;

	block_store tail_block_store;

public:
	table_store(const table_description & _desc) :
			tbl_desc(_desc), status(sdb_table_ready), head_buffer(char_buffer(table_head_length)){

		full_path.append(_desc.get_sdb().get_full_path()).append("/").append(_desc.get_table_name()).append(table_store_file_extension);

		lock_path.append(_desc.get_sdb().get_full_path()).append("/").append(_desc.get_table_name()).append(table_lock_file_extension);

		time(&create_time);
		time(&update_time);
	}
	virtual ~table_store();

	/**
	 * open the table data file and indicate it's status
	 */
	bool open();

	bool is_open();

	bool init_store(bool refresh);

	bool is_initialized();

	bool close();

	bool is_closed();

	bool read_header();
	bool sync_header();

	bool assign_block_store(block_store & _bs);

	const block_store & head();

	const block_store & tail();

	/**
	 * add a row_store to the table store end-block-store
	 */
	bool append_row_store(const row_store & _rs);

	bool append_block_store(const block_store & _bs);

	long get_block_store_off() const {
		return block_store_off;
	}

	void set_block_store_off(long blockstoreoff) {
		block_store_off = blockstoreoff;
	}

	time_t get_create_time() const {
		return create_time;
	}

	void set_create_time(time_t createtime) {
		create_time = createtime;
	}

	const std::string& get_full_path() const {
		return full_path;
	}

	void set_full_path(const std::string& fullpath) {
		full_path = fullpath;
	}

	long get_head_block_store_pos() const {
		return head_block_store_pos;
	}

	void set_head_block_store_pos(long headblockstorepos) {
		head_block_store_pos = headblockstorepos;
	}

	const char_buffer& get_head_buffer() const {
		return head_buffer;
	}

	void set_head_buffer(const char_buffer& headbuffer) {
		head_buffer = headbuffer;
	}

	const std::string& get_lock_path() const {
		return lock_path;
	}

	void set_lock_path(const std::string& lockpath) {
		lock_path = lockpath;
	}

	sdb_table_status get_status() const {
		return status;
	}

	void set_status(sdb_table_status status) {
		this->status = status;
	}

	long get_tail_block_store_pos() const {
		return tail_block_store_pos;
	}

	void set_tail_block_store_pos(long tailblockstorepos) {
		tail_block_store_pos = tailblockstorepos;
	}

	const table_description& get_tbl_desc() const {
		return tbl_desc;
	}

	void set_tbl_desc(const table_description& tbldesc) {
		tbl_desc = tbldesc;
	}
};

} /* namespace enginee */

#endif /* TABLE_STORE_H_ */
