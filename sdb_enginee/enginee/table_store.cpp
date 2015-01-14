/*
 * table_store.cpp
 *
 *  Created on: Jan 4, 2015
 *      Author: linkqian
 */

#include "table_store.h"
#include "../common/sio.h"
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

namespace enginee {

bool TableStore::is_open() {
	return sdb_io::exist_file(lock_path) && status == sdb_table_opened;
}

bool TableStore::is_closed() {
	return status == sdb_table_closed || !sdb_io::exist_file(lock_path);
}

bool TableStore::init_store(bool refresh) {
	if (refresh || !is_initialized()) {

		block_store_off = TABLE_HEAD_LENGTH;
//		head_block_store_pos = table_head_length;

		head_buffer << tbl_desc.get_table_name() << full_path << create_time << update_time;
		head_buffer << block_store_off << head_block_store_pos << tail_block_store_pos;

		table_stream.open(full_path.c_str(), fstream::binary | fstream::out);
		if (table_stream.is_open()) {
			table_stream.seekg(0, fstream::beg);
			table_stream.write(head_buffer.data(), TABLE_HEAD_LENGTH);
			table_stream.close();
		}
	}
	return false;
}

bool TableStore::is_initialized() {
	if (sdb_io::exist_file(full_path)) {
		struct stat t_stat;
		stat(full_path.c_str(), &t_stat);
		return t_stat.st_size >= TABLE_HEAD_LENGTH;
	}
	return false;
}

bool TableStore::has_block_store() {
	if (sdb_io::exist_file(full_path)) {
		struct stat t_stat;
		stat(full_path.c_str(), &t_stat);
		return t_stat.st_size > TABLE_HEAD_LENGTH;
	}
	return false;
}
bool TableStore::head_block(BlockStore * p_bs) {
	bool r = false;
	long pos = table_stream.tellg();
	table_stream.seekg(TABLE_HEAD_LENGTH - pos, ios_base::cur);
	char * buff = new char[BLOCK_STORE_HEAD_SIZE];
	table_stream.read(buff, BLOCK_STORE_HEAD_SIZE);
	p_bs->push_head_chars(buff, BLOCK_STORE_HEAD_SIZE);
	p_bs->read_head();
	r = table_stream.good();
	delete[] buff;

	return r;
}
bool TableStore::open() {
	if (sdb_io::exist_file(lock_path)) {
		return false;
	} else {
		ofstream lock_stream(lock_path.c_str());
		bool locked = lock_stream.good();
		lock_stream.close();
		if (locked) {
			status = sdb_table_opened;
		} else {
			perror("open table store lock file failure");
		}

		if (locked) {
			table_stream.open(full_path.c_str(), ios_base::in | ios_base::out | ios_base::binary);
			if (table_stream.is_open()) {
				return read_head();
			} else {
				perror("open table store data file failure");
				return false;
			}

			//read head
		}
		return locked;
	}
}

bool TableStore::read_head() {
	char * buff = new char[TABLE_HEAD_LENGTH];
	head_buffer.rewind();
	table_stream.read(buff, TABLE_HEAD_LENGTH);
	head_buffer.push_chars(buff, TABLE_HEAD_LENGTH);
	delete[] buff;
	string t_name, t_path;

	head_buffer >> t_name >> t_path >> create_time >> update_time;
	if (t_name != this->tbl_desc.get_table_name() || t_path != full_path) {
		perror("invalid table store data file");
		return false;
	}
	head_buffer >> block_store_off >> head_block_store_pos >> tail_block_store_pos;

	return true;
}

bool TableStore::sync_head() {
	head_buffer.rewind();
	head_buffer << tbl_desc.get_table_name() << full_path << create_time << update_time;

//	head_buffer << block_store_off << head_block_store_pos << tail_block_store_pos;

	long pos = table_stream.tellg();
	table_stream.seekg(0, ios_base::beg);
	table_stream.write(head_buffer.data(), TABLE_HEAD_LENGTH);
	table_stream.seekg(pos, ios_base::beg);

	return !table_stream.bad();
}

bool TableStore::assign_block_store(BlockStore & bs) {
	bool success = false;
	if (bs.get_start_pos() <= 0) {
		bs.set_table_desc(&tbl_desc);
		bs.set_start_pos(block_store_off);

		time_t curr;
		time(&curr);

		bs.set_assign_time(curr);
		bs.set_status(enginee::DISK_SPACE_ASSIGNED);
		bs.fill_head();

		char_buffer block_buffer(bs.get_block_size());

		//append a empty block to table store data file
		table_stream.seekg(0, ios_base::end);
		table_stream.write(bs.get_block_store_head()->data(), enginee::BLOCK_STORE_HEAD_SIZE);
		table_stream.write(block_buffer.data(), block_buffer.capacity());
		table_stream.flush();

		time(&update_time);

		success = !table_stream.fail();
		if (success) {
			bs.set_status(enginee::DISK_SPACE_ASSIGNED);
			block_store_off += bs.get_block_size();
			bs.set_end_pos(block_store_off);

			if (bs.get_start_pos() == enginee::TABLE_HEAD_LENGTH) {
//			head_block_store = bs;
			}

			if (bs.get_start_pos() == block_store_off) {
//			tail_block_store = bs;
			}
		}
	}
	return success;
}
/**
 * read block store information from current position of the table data store
 */
bool TableStore::read_block_store(BlockStore & bs, const bool & with_content) {
	bool read = false;
	if (bs.get_start_pos() > 0) {
		long cur_pos = table_stream.tellg();
		table_stream.seekg(bs.get_start_pos() - cur_pos, ios_base::cur);
		char * h_buff = new char[enginee::BLOCK_STORE_HEAD_SIZE];
		table_stream.read(h_buff, enginee::BLOCK_STORE_HEAD_SIZE);
		bs.get_block_store_head()->rewind();
		bs.get_block_store_head()->push_chars(h_buff, enginee::BLOCK_STORE_HEAD_SIZE);
		bs.read_head();

		if (with_content) {
			char * d_buff = new char[bs.get_block_size()];
			table_stream.read(d_buff, bs.get_block_size());
			bs.set_block_data(d_buff);
		}

		read = table_stream.good();
	}
	return read;
}

bool TableStore::sync_buffer(BlockStore & bs, const int & ops) {
	bool synced = false;
	if (bs.get_start_pos() > 0) {
		if ((ops & enginee::SYNC_BLOCK_HEAD) == enginee::SYNC_BLOCK_HEAD) {
			int old_tail = bs.get_tail();
			bs.new_tail();
			bs.fill_head();
			long cur_pos = table_stream.tellg();
			table_stream.seekg(bs.get_start_pos() - cur_pos, ios_base::cur);
			table_stream.write(bs.head_buffer(), enginee::BLOCK_STORE_HEAD_SIZE);

			if (table_stream.good()) {
				if ((ops & enginee::SYNC_BLOCK_DATA) == enginee::SYNC_BLOCK_DATA) {

					char_buffer row_buff(bs.get_row_store_size());
					table_stream.seekg(old_tail, ios_base::cur);

					for (int i = 0; i < bs.get_bufferred_rows(); i++) {
						if (bs.get_block_buff()->pop_char_buffer(&row_buff)) {
							table_stream.write(row_buff.data(), row_buff.size());
							row_buff.rewind();
							table_stream.seekg(bs.get_row_store_size() - row_buff.size(), ios_base::cur);
						}
					}
				}
				if (table_stream.good() && table_stream.flush() == 0) {
					synced = true;
				}
			}
		}
	}
	return synced;
}

bool TableStore::update_row_store(const BlockStore & _bs, RowStore & _rs) {
	long start = _bs.get_start_pos() + _rs.get_start_pos();
	char_buffer row_buff(_bs.get_row_store_size());
	_rs.fill_char_buffer(&row_buff);
	return update_row_buffer(start, row_buff, row_buff.size());
}

bool TableStore::update_row_store(const BlockStore & _bs, std::list<RowStore> & rs_list) {
	bool success = true;
	std::list<RowStore>::iterator it = rs_list.begin();
	char_buffer row_buff(_bs.get_row_store_size());
	for (; success && it != rs_list.end(); it++) {
		long start = _bs.get_start_pos() + it->get_start_pos();
		it->fill_char_buffer(&row_buff);
		success = update_row_buffer(start, row_buff, row_buff.size());
		row_buff.rewind();
	}
	return success;
}

bool TableStore::update_row_buffer(long target_pos, char_buffer & buff, int size) {
	bool updated = false;
	long cur_pos = table_stream.tellg();
	long off = target_pos - cur_pos;
	table_stream.seekg(off, ios_base::cur);
	table_stream.write(buff.data(), size);
	updated = table_stream.good();
	return updated;
}

bool TableStore::update_row_store(RowStore & _rs) {
	char_buffer row_buff(_rs.get_end_pos() - _rs.get_start_pos());
	_rs.fill_char_buffer(&row_buff);
	return update_row_buffer(_rs.get_stream_pos(), row_buff, row_buff.size());
}

bool TableStore::update_row_store(std::list<RowStore> & rs_list) {
	bool success = true;
	std::list<RowStore>::iterator it = rs_list.begin();
	char_buffer row_buff(enginee::ROW_STORE_SIZE_4K);
	for (; success && it != rs_list.end(); it++) {
		it->fill_char_buffer(&row_buff);
		success = update_row_buffer(it->get_stream_pos(), row_buff, row_buff.size());
		row_buff.rewind();
	}
	return success;
}

bool TableStore::mark_deleted_status(RowStore & rs) {
	char_buffer buff(4);
	buff << enginee::ROW_STORE_DELETED;
	return update_row_buffer(rs.get_stream_pos(), buff, buff.size());
}

bool TableStore::mark_deleted_status(std::list<RowStore> & rs_list) {
	char_buffer buff(4);
	buff << enginee::ROW_STORE_DELETED;
	bool success = true;
	std::list<RowStore>::iterator it = rs_list.begin();
	for (; success && it != rs_list.end(); it++) {
		success = update_row_buffer(it->get_stream_pos(), buff, buff.size());
	}

	return success;
}

bool TableStore::mark_deleted_status(const BlockStore & bs, RowStore & rs) {
	char_buffer buff(4);
	buff << enginee::ROW_STORE_DELETED;
	return update_row_buffer(bs.get_start_pos() + rs.get_start_pos(), buff, buff.size());
}

bool TableStore::mark_deleted_status(const BlockStore & bs, std::list<RowStore> & rs_list) {
	char_buffer buff(4);
	buff << enginee::ROW_STORE_DELETED;
	bool success = true;
	std::list<RowStore>::iterator it = rs_list.begin();
	for (; success && it != rs_list.end(); it++) {
		success = update_row_buffer(it->get_start_pos() + bs.get_start_pos(), buff, buff.size());
	}

	return success;
}

bool TableStore::read_row_store(const BlockStore & bs, RowStore * prs) {
	bool read = false;

	if (prs->get_start_pos() > bs.get_tail()) {
		return false;
	}

	long off = bs.get_start_pos() + prs->get_start_pos() - table_stream.tellg();
	if (off > 0) {
		table_stream.seekg(off, ios_base::cur);
	}

	char * p = new char[enginee::ROW_STORE_HEAD];
	table_stream.read(p, enginee::ROW_STORE_HEAD);
	if (table_stream.good()) {
		char_buffer buff(enginee::ROW_STORE_HEAD);
		buff.push_chars(p, enginee::ROW_STORE_HEAD);
		delete[] p;

		int row_status = buff.pop_int();
		if (row_status == enginee::ROW_STORE_DELETED) {
			prs->set_status(row_status);
			return false;
		} else {
			int row_len = buff.pop_int();
			p = new char[row_len];
			table_stream.read(p, row_len);
			read = table_stream.good();
			if (read) {
				buff.push_chars(p, row_len);
				buff.rewind();
				prs->read_char_buffer(&buff);
			}
			delete[] p;
			return read;
		}
	} else {
		return false;
	}
}

bool TableStore::close() {
	if (sdb_io::exist_file(lock_path)) {
		table_stream.close();
		bool b = (remove(lock_path.c_str()) == 0);
		if (b) {
			status = sdb_table_closed;
		} else {
			perror("close table store failure");
		}
		return b;
	}
	return true;
}

TableStore::~TableStore() {
}

} /* namespace enginee */
