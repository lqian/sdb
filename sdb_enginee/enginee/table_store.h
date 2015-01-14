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
#include "block_store.h"
#include "row_store.h"
#include "table_description.h"
#include "../common/char_buffer.h"
#include <fstream>

namespace enginee {

using namespace std;
using namespace common;

const static std::string TABLE_STORE_FILE_EXTENSION(".data");
const static std::string TABLE_LOCK_FILE_EXTENSION(".lock");

const static int TABLE_HEAD_LENGTH(1024);

const static int SYNC_BLOCK_HEAD = 1;
const static int SYNC_BLOCK_DATA = 2;

class TableStore {

private:
	TableDescription tbl_desc;
	std::string full_path; // the full path of file stores table data
	std::string lock_path;
	sdb_table_status status;

	time_t create_time;
	time_t update_time;

	long block_store_off;  // offset includes last block length, meanwhile it's the next new block start off

	long head_block_store_pos;
	long tail_block_store_pos;

	fstream table_stream;

	char_buffer head_buffer;

	bool update_row_buffer(long start, char_buffer & buff, int size);

public:
	explicit TableStore(const TableDescription & _desc) :
			tbl_desc(_desc), status(sdb_table_ready), head_buffer(char_buffer(TABLE_HEAD_LENGTH)) {

		full_path.append(_desc.get_sdb().get_full_path()).append("/").append(_desc.get_table_name()).append(
				TABLE_STORE_FILE_EXTENSION);

		lock_path.append(_desc.get_sdb().get_full_path()).append("/").append(_desc.get_table_name()).append(
				TABLE_LOCK_FILE_EXTENSION);

		time(&create_time);
		time(&update_time);
	}

	explicit TableStore(const TableStore & other) :
			head_buffer(char_buffer(TABLE_HEAD_LENGTH)) {
		this->tbl_desc = other.tbl_desc;
		this->head_buffer = other.head_buffer;
		this->full_path = other.full_path;
		this->create_time = other.create_time;
		this->update_time = other.update_time;
		this->lock_path = other.lock_path;
		this->status = other.status;
		this->head_block_store_pos = other.head_block_store_pos;
		this->tail_block_store_pos = other.tail_block_store_pos;
		this->block_store_off = other.block_store_off;
	}
	virtual ~TableStore();

	/**
	 * open the table data file and indicate it's status
	 */
	bool open();

	bool is_open();

	bool init_store(bool refresh);

	bool is_initialized();

	bool close();

	bool has_block_store();

	bool is_closed();

	bool read_head();
	bool sync_head();

	/**
	 * assign disk space for the empty block_store, set the it's position and end position
	 */
	bool assign_block_store(BlockStore & bs);

	/**
	 * sync head content of block_store specified to correct position on table store
	 */
	bool sync_buffer(BlockStore & bs, const int & block_ops);

	bool read_block_store(BlockStore & bs, const bool & with_content);

	bool update_row_store(const BlockStore & _bs, RowStore & rs);

	/*
	 * update row list within a BlockStore
	 */
	bool update_row_store(const BlockStore & _bs, std::list<RowStore> & rs_list);

	bool update_row_store(RowStore & rs);
	bool update_row_store(std::list<RowStore> & rs_list);

	bool mark_deleted_status(RowStore & _rs);
	bool mark_deleted_status(std::list<RowStore> & rs_list);

	bool mark_deleted_status(const BlockStore & bs, RowStore & rs);
	bool mark_deleted_status(const BlockStore & bs, std::list<RowStore> & rs_list);

	bool head_block(BlockStore * p_bs);

	bool read_row_store(const BlockStore & bs, RowStore * prs);

	bool read_row_store(RowStore * prs);

	bool tail_block(BlockStore * p_bs);

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

	const TableDescription& get_tbl_desc() const {
		return tbl_desc;
	}

	void set_tbl_desc(const TableDescription& tbldesc) {
		tbl_desc = tbldesc;
	}
};

} /* namespace enginee */

#endif /* TABLE_STORE_H_ */
