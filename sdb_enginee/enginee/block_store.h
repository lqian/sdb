/*
 * block_store.h
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#ifndef BLOCK_STORE_H_
#define BLOCK_STORE_H_

#include <time.h>
#include "row_store.h"
#include "table_description.h"
#include "../common/char_buffer.h"

namespace enginee {

const static std::string DEFAULT_NAMESPACE("_default");
const static int ROW_STORE_SIZE_4K = 4096;
const static int ROW_STORE_SIZE_8K = 8192;
const static int ROW_STORE_SIZE_16K = 16384;
const static int ROW_STORE_SIZE_32K = 32768;
const static int ROW_STORE_SIZE_64K = 65536;
const static int ROW_STORE_SIZE_128K = 131072;

const static int BLOCK_SIZE_64M = 64 * 1024 * 1024;
const static int BLOCK_SIZE_128M = 128 * 1024 * 1024;
const static int BLOCK_SIZE_256M = 256 * 1024 * 1024;
const static int BLOCK_SIZE_512M = 512 * 1024 * 1024;

using namespace std;

class block_store {

private:
	table_description tbl_desc;
	long block_store_id;
	int row_store_size;
	short status;
	string name_space;
	long start_pos;  // start position in table store data
	long end_pos;
	long pre_block_store_pos;
	long next_block_store_pos;
	int block_size;
	int tail; // end position of the last row store

	int usage_percent;
	time_t create_time;
	time_t update_time;
	time_t assign_time;

public:

	block_store() :
			row_store_size(ROW_STORE_SIZE_4K), block_size(BLOCK_SIZE_64M), name_space(DEFAULT_NAMESPACE) {
		time(&create_time);
		time(&update_time);
	}
	explicit block_store(const table_description &_tbl_desc) :
			tbl_desc(_tbl_desc), row_store_size(ROW_STORE_SIZE_4K), block_size(BLOCK_SIZE_64M), name_space(
					DEFAULT_NAMESPACE) {
		time(&create_time);
		time(&update_time);
	}

	explicit block_store(const table_description &_tbl_desc, long & start, long& end) :
			tbl_desc(_tbl_desc), start_pos(start), end_pos(end), name_space(DEFAULT_NAMESPACE) {
		this->block_size = end - start;
	}

	explicit block_store(const block_store & other) :
			tbl_desc(other.tbl_desc) {
		this->block_store_id = other.block_store_id;
		this->row_store_size = other.row_store_size;
		this->status = other.status;
		this->start_pos = other.start_pos;
		this->end_pos = other.end_pos;
		this->pre_block_store_pos = other.pre_block_store_pos;
		this->next_block_store_pos = other.next_block_store_pos;
		this->block_size = other.block_size;
		this->tail = other.tail;
		this->usage_percent = other.usage_percent;
		this->create_time = other.create_time;
		this->update_time = other.update_time;
		this->assign_time = other.assign_time;
	}

	virtual ~block_store();

	bool is_empty();
	bool sync_to_file();
	bool sync_header();
	bool load_from_file();

	void fill_block_head(common::char_buffer & buff);

	bool append_row_store(const row_store & _rs);
	bool delete_row_store(const row_store & _rs); // without index
	bool update_row_store(const row_store & _rs);
	bool delete_row_store(const row_store & _rs, const int & off);  // with index
	bool update_row_store(const row_store & _rs, const int & off);

	const block_store & next_block_store();
	const block_store & pre_block_store();
	const block_store defragment();

	bool has_next() {
		return next_block_store_pos != 0;
	}
	bool has_previous() {
		return pre_block_store_pos != 0;
	}

	void operator=(const block_store & other) {
		this->tbl_desc = other.tbl_desc;
		this->block_store_id = other.block_store_id;
		this->row_store_size = other.row_store_size;
		this->status = other.status;
		this->start_pos = other.start_pos;
		this->end_pos = other.end_pos;
		this->pre_block_store_pos = other.pre_block_store_pos;
		this->next_block_store_pos = other.next_block_store_pos;
		this->block_size = other.block_size;
		this->tail = other.tail;
		this->usage_percent = other.usage_percent;
		this->create_time = other.create_time;
		this->update_time = other.update_time;
		this->assign_time = other.assign_time;
	}

	bool operator==(const block_store & other) {
		return this->tbl_desc == other.tbl_desc && this->block_store_id == other.block_store_id
				&& this->row_store_size == other.row_store_size && this->status == other.status
				&& this->start_pos == other.start_pos && this->end_pos == other.end_pos
				&& this->pre_block_store_pos == other.pre_block_store_pos
				&& this->next_block_store_pos == other.next_block_store_pos && this->block_size == other.block_size
				&& this->tail == other.tail && this->usage_percent == other.usage_percent
				&& this->create_time == other.create_time && this->update_time == other.update_time
				&& this->assign_time == other.assign_time;
	}

	int get_block_size() const {
		return block_size;
	}

	void set_block_size(int blocksize) {
		block_size = blocksize;
	}

	long get_block_store_id() const {
		return block_store_id;
	}

	void set_block_store_id(long blockstoreid) {
		block_store_id = blockstoreid;
	}

	time_t get_create_time() const {
		return create_time;
	}

	void set_create_time(time_t createtime) {
		create_time = createtime;
	}

	long get_end_pos() const {
		return end_pos;
	}

	void set_end_pos(long endpos) {
		end_pos = endpos;
	}

	const string& get_name_space() const {
		return name_space;
	}

	void set_name_space(const string& _namespace) {
		name_space = _namespace;
	}

	int get_row_store_size() const {
		return row_store_size;
	}

	void set_row_store_size(int rowstoresize) {
		row_store_size = rowstoresize;
	}

	long get_start_pos() const {
		return start_pos;
	}

	void set_start_pos(long startpos) {
		start_pos = startpos;
	}

	short get_status() const {
		return status;
	}

	void set_status(short status) {
		this->status = status;
	}

	int get_tail() const {
		return tail;
	}

	void set_tail(int tail) {
		this->tail = tail;
	}

	const table_description& get_tbl_desc() const {
		return tbl_desc;
	}

	void set_tbl_desc(const table_description& tbldesc) {
		tbl_desc = tbldesc;
	}

	time_t get_update_time() const {
		return update_time;
	}

	void set_update_time(time_t updatetime) {
		update_time = updatetime;
	}

	int get_usage_percent() const {
		return usage_percent;
	}

	void set_usage_percent(int usagepercent) {
		usage_percent = usagepercent;
	}

	long get_next_block_store_pos() const {
		return next_block_store_pos;
	}

	void set_next_block_store_pos(long nextblockstorepos) {
		next_block_store_pos = nextblockstorepos;
	}

	long get_pre_block_store_pos() const {
		return pre_block_store_pos;
	}

	void set_pre_block_store_pos(long preblockstorepos) {
		pre_block_store_pos = preblockstorepos;
	}

	time_t get_assign_time() const {
		return assign_time;
	}

	void set_assign_time(const time_t & assigntime) {
		assign_time = assigntime;
	}
};

} /* namespace enginee */

#endif /* BLOCK_STORE_H_ */
