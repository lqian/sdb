/*
 * block_store.h
 *
 *  Created on: Jan 8, 2015
 *      Author: linkqian
 */

#ifndef BLOCK_STORE_H_
#define BLOCK_STORE_H_

#include <string>
#include "../common/char_buffer.h"
#include "table_description.h"
#include "row_store.h"

namespace enginee {

using namespace std;
using namespace common;

const static int BLOCK_STORE_HEAD_SIZE = 1024;

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

const static short DISK_SPACE_ASSIGNED = 1;
const static short DISK_SPACE_NOT_ASSIGNED = -1;

const static short SUCCESS_APPEND_BUFFER = 2;
const static short FAILURE_APPEND_BUFFER = 4;

class BlockStore {

private:
	long block_store_id;
	int row_store_size;
	short status;
	string name_space;
	long start_pos = 0;  // start position in its table store data
	long end_pos = 0;    // end position in its table store data
	long pre_block_store_pos = -1;
	long next_block_store_pos = -1;
	int block_size;

	int tail = 0; // end position of the last row store

	int usage_percent = 0;
	time_t create_time;
	time_t update_time;
	time_t assign_time;

	int block_buff_cap = 0 ;

	char_buffer* block_store_head;
	char * block_data;
	char_buffer * block_buff;
	TableDescription * table_desc;

	int bufferred_rows;

public:

	explicit BlockStore() :
			row_store_size(ROW_STORE_SIZE_4K), block_size(BLOCK_SIZE_64M), name_space(DEFAULT_NAMESPACE) {
		time(&create_time);
		time(&update_time);
		block_store_head = new char_buffer(BLOCK_STORE_HEAD_SIZE);
	}

	explicit BlockStore(long & start, long& end) :
			start_pos(start), end_pos(end), name_space(DEFAULT_NAMESPACE) {
		this->block_size = end - start;
		block_store_head = new char_buffer(BLOCK_STORE_HEAD_SIZE);
	}

	explicit BlockStore(const enginee::BlockStore & other) {
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
		this->block_store_head = other.block_store_head;
		this->block_buff_cap = other.block_buff_cap;
		this->block_buff = other.block_buff;
	}

	virtual ~BlockStore() {
		delete block_store_head;
	}

	const bool is_empty() {
		return usage_percent == 0 || tail == 0;
	}

	const bool has_sync_buffer() {
		return block_buff->size() > 0;
	}

	const bool is_full_buffer() {
		return block_buff->remain() > 0;
	}

	const bool is_full() {
		return usage_percent >= 99 || tail + start_pos == end_pos;
	}

	void new_tail() {
		tail += (row_store_size * bufferred_rows);
	}
	void clean_buffer() {
		bufferred_rows = 0;
		block_buff->rewind();
	}

	const char * head_buffer() {
		return block_store_head->data();
	}

	void push_head_chars(const char *p, int len) {
		block_store_head->push_chars(p, len);
	}

	void fill_head();

	void read_head();

	/*
	 * only append the RowStore to block_buff, return true if append RowStore data to block_buff,
	 * return false when block_store is full or block_buff is full;
	 */
	bool append_to_buff(RowStore * _rs);

	void defragment();

	bool has_next() {
		return next_block_store_pos != 0;
	}
	bool has_previous() {
		return pre_block_store_pos != 0;
	}

	void operator=(const BlockStore & other) {
		this->table_desc = other.table_desc;
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

	bool operator==(const BlockStore & other) {
		return this->table_desc == other.table_desc && this->block_store_id == other.block_store_id
				&& this->row_store_size == other.row_store_size && this->status == other.status
				&& this->start_pos == other.start_pos && this->end_pos == other.end_pos;

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

	void set_start_pos(const long startpos) {
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

	char_buffer* get_block_store_head() {
		return block_store_head;
	}

	void set_block_store_head(char_buffer* blockstorehead) {
		block_store_head = blockstorehead;
	}

	const TableDescription* get_table_desc() const {
		return table_desc;
	}

	void set_table_desc(TableDescription* tabledesc) {
		table_desc = tabledesc;
	}

	char* get_block_data() const {
		return block_data;
	}

	void set_block_data(char* blockdata) {
		block_data = blockdata;
	}

	char_buffer* get_block_buff() const {
		return block_buff;
	}

	void set_block_buff(char_buffer* blockbuff) {
		block_buff = blockbuff;
		block_buff_cap = block_buff->capacity();
	}

	int get_block_buff_cap() const {
		return block_buff_cap;
	}

	int get_bufferred_rows() const {
		return bufferred_rows;
	}
};

} /* namespace enginee */

#endif /* BLOCK_STORE_H_ */
