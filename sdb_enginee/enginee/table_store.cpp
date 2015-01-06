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

bool table_store::is_open() {
	return sdb_io::exist_file(lock_path);
}

bool table_store::is_closed() {
	return status == sdb_table_closed || !sdb_io::exist_file(lock_path);
}

bool table_store::init_store(bool refresh) {
	if (refresh || !is_initialized()) {

		block_store_off = table_head_length;
		head_block_store_pos = table_head_length;

		head_buffer << tbl_desc.get_table_name() << full_path << create_time << update_time;
		head_buffer << block_store_off << head_block_store_pos << tail_block_store_pos << block_count;

//		while(head_buffer.size() < table_header_length) {
//			head_buffer.push_back('0');
//		}
		table_stream.open(full_path, fstream::binary | fstream::out);
		if (table_stream.is_open()) {
			table_stream.seekg(0, fstream::beg);
			table_stream.write(head_buffer.data(), table_head_length);
			table_stream.close();
		}
	}
	return false;
}

bool table_store::is_initialized() {
	if (sdb_io::exist_file(full_path)) {
		struct stat t_stat;
		stat(full_path.c_str(), &t_stat);
		return t_stat.st_size >= table_head_length;
	}
	return false;
}

bool table_store::open() {
	if (sdb_io::exist_file(lock_path)) {
		return false;
	} else {
		ofstream lock_stream(lock_path);
		bool locked = lock_stream.good();
		lock_stream.close();
		if (locked) {
			status = sdb_table_opened;
		} else {
			perror("open table store lock file failure");
		}

		if (locked) {
			table_stream.open(full_path, ios_base::in | ios_base::out | ios_base::binary);
			if (table_stream.is_open()) {
				return read_header();
			} else {
				perror("open table store data file failure");
				return false;
			}

			//read head
		}
		return locked;
	}
}

bool table_store::read_header() {
	char * buff = new char[table_head_length];
	head_buffer.rewind();
	table_stream.read(buff, table_head_length);
	head_buffer.push_chars(buff, table_head_length);
	delete[] buff;
	string t_name, t_path;

	head_buffer >> t_name >> t_path >> create_time >> update_time;
	if (t_name != this->tbl_desc.get_table_name() || t_path != full_path) {
		perror("invalid table store data file");
		return false;
	}
	head_buffer >> block_store_off >> head_block_store_pos >> tail_block_store_pos >> block_count;

	return true;
}

bool table_store::sync_header() {
	head_buffer.rewind();
	head_buffer << tbl_desc.get_table_name() << full_path << create_time << update_time;
	head_buffer << block_store_off << head_block_store_pos << tail_block_store_pos << block_count;

	long pos = table_stream.tellg();
	table_stream.seekg(0, ios_base::beg);
	table_stream.write(head_buffer.data(), table_head_length);
	table_stream.seekg(pos, ios_base::beg);

	return !table_stream.bad();
}

bool table_store::assign_block_store(block_store & bs) {
	bool success = false;

	//TODO assign block previous block start and end position
	bs.set_tbl_desc(this->tbl_desc);
	bs.set_start_pos(block_store_off);
	block_store_off += bs.get_block_size();
	bs.set_end_pos(block_store_off);
	time_t curr;
	time(&curr);
	bs.set_assign_time(curr);

	char_buffer block_buffer(bs.get_block_size());
	bs.fill_block_head(block_buffer);

	//append a empty block to table store data file
	table_stream.seekg(0, ios_base::end);
	table_stream.write(block_buffer.data(), block_buffer.capacity());
	table_stream.sync();

	success = !table_stream.fail();
	if (success) {
		block_store_off += bs.get_block_size();

		if (bs.get_start_pos() == enginee::table_head_length) {
			head_block_store = bs;
		}

		if (bs.get_start_pos() == block_store_off) {
			tail_block_store = bs;
		}
	}
	return success;
}

bool table_store::close() {
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
table_store::~table_store() {
	// TODO Auto-generated destructor stub
}

} /* namespace enginee */
