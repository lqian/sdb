/*
 * datafile.h
 *
 *  Created on: Apr 20, 2015
 *      Author: linkqian
 */

#ifndef STORAGE_DATAFILE_H_
#define STORAGE_DATAFILE_H_

#include <ctime>
#include <string>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iostream>
#include "../common/char_buffer.h"

int SUCCESS = 0;
int FAILURE = -1;

namespace storage {

using namespace std;
using namespace common;

const int unused_id = 0;
const unsigned int data_file_magic = 0x7A9E401C;
const short data_file_head_size = 1024;
const short data_file_version = 0x10;
const long data_file_max_size = 0x10000000;  // 4G

const unsigned int segment_magic = 0x50FED398;
const short segment_head_size = 1024;
const short segment_tail_size = 1024;
const char segment_deleted = 0xff;
const char segment_alive = 0x01;
const short segment_space_reversed_for_insert = 20; //20
const short block_space_reversed_for_insert = 20;

int M_64 = 67108864;
int M_128 = 134217728;
int M_256 = 268435456;
int M_512 = 536870912;

int kilo_byte = 1024;

int UNKNOWN_OFFSET = -1;
int DELETE_OFFSET = -2;

int offset_bytes = 2;

/**
 * data block buffer internal:
 * +---+---+---+----------------------------+------+------+------+
 * |   |   |   |                            |      |      |      |
 * | O | O | O |                            |      |      |      |
 * | F | F | F |    UN-UTILIZAION SPACE     | ROW3 | ROW2 | ROW1 |
 * | F | F | F |                            | DATA | DATA | DATA |
 * | 1 | 2 | 3 |                            |      |      |      |
 * +---+---+---+----------------------------+------+------+------+
 *
 * ROW DIRECTORY has a OFFSET table, each OFFSET has 2 bytes, point a start
 * position for a  ROW DATA has variant bytes.
 *
 * The first OFFSET is placed the first-two-bytes, the first ROW DATA place
 * at the end of the block. The space behind the last OFFSET, before the last
 * ROW DATA is UN-UTILIZAION SPACE.
 * In default, a data block keep 20% UN-UTILIZAION SPACE for safe-update.
 *
 * when append a n-bytes row data to the block, first get the last OFFSET denoted
 * as OFF', the new OFFSET denoted as (OFFn = OFF' - n), we write the OFFn after
 * OFF', and write n-bytes row data at OFFn, but keep 20% free space. the position
 * of two bytes OFFn denoted as Pn, so the percentage of UN-UTILIZAION SPACE is,
 * 	free_perct = (OFFn - Pn － 2) * 100 / BLOCK_LENGTH
 *
 * when delete a row data, we just fill the 15th bit of OFFSET as 1, so OFFSET
 * has a max of 2^15. but it's enough for ROW DATA seeking within data block.
 *
 * when update a row data size exceeds its original size. make DELETEd flat on
 * its OFFSET,and then append new OFFSET and ROW DATA at un-utilization space.
 * else only overwrite its original space, but keep the row data LENGTH correct.
 *
 * update operation may incur some disk space become un-available. a thread
 * periodically re-organize data block to recycle them for re-use.
 */
struct data_block {
	int offset; /* block offset from start of segment which the block belong to,
	 the first data block offset is 0 within a segment */

	char *buffer; /* all data store in the buffer, include ROW DIRECTORY & DATA */
	short length; /* total length of buffer, include un-utilization space */

	short u_off_start = 0;
	short u_off_end = 0; /* update offset start and end  if the block buffer has been modified */

	short free_perct = 100;

	void init(int off, short len) {
		offset = off;
		length = len;
		buffer = new char[length];

		u_off_start = 0;
		u_off_end = 0;
	}
};

struct mem_data_block: data_block {
	vector<unsigned short> off_tbl;
	void calc_free_perct() {
		int s = off_tbl.size();
		if (s > 0) {
			free_perct = (off_tbl[s - 1] - s * offset_bytes) * 100 / length;
		}
	}

	bool enough_reserved_space(short len) {
		int s = off_tbl.size();
		return ((off_tbl[s - 1] - s * offset_bytes - len - offset_bytes) * 100
				/ length) >= 20;
	}

	int remainder_space() {
		int s = off_tbl.size();
		return s == 0 ? length : off_tbl[s - 1] - s * offset_bytes;
	}

	bool enough_space(short len) {
		int s = off_tbl.size();
		return s == 0 ?
				length :
				off_tbl[s - 1] - s * offset_bytes - len - offset_bytes > 0;
	}

	/**
	 * parse the OFFSET TABLE from char buffer, and modification start and end offset
	 */
	void parse_off_tbl() {
		unsigned short * p = (unsigned short *) buffer;
		while ((*p) > 0) {
			off_tbl.push_back((*p));
			++p;
		}

		int s = off_tbl.size();
		u_off_start = s * offset_bytes;
		u_off_end = length;
		if (s > 0) {
			u_off_end = off_tbl[s - 1];
		}
	}

	/**
	 * add a row data denoted by rl length of rb.
	 * if there isn't any row in the block and length of row is less than block length, add it,
	 * else keep 20% un-utilization disk space at least.
	 */
	int add_row_data(const char* rb, const int rl) {
		int r = UNKNOWN_OFFSET;
		int s = off_tbl.size();
		if (s == 0) {
			// there isn't any ROW DATA in the data block
			if (rl <= length - offset_bytes) {
				unsigned short l_off = length - rl;
				off_tbl.push_back(l_off);
				memcpy(buffer, rb, rl);
				calc_free_perct();
				r = l_off;
			}
		} else if (enough_reserved_space(rl)) {
			// make sure that reserve 20% free space in the data block
			unsigned short l_off = off_tbl[s - 1];
			l_off -= rl;
			off_tbl.push_back(l_off);
			memcpy(buffer + l_off, rb, rl);
			calc_free_perct();
			r = l_off;
		}
		return r;
	}

	/**
	 * update a existed row with a new given row and the row's existed offset, return new offset
	 * if update successfully, else UNKNOWN_OFFSET or DELETE_OFFSET.
	 *
	 * 1) if the offset denote the last row, and there are enough un-utilization remainder space,
	 * update the offset to point the new offset, save row data. whereas are not enough un-utilization
	 * space. delete the exist offset, return DELETE_OFFSET
	 *
	 * 2) if the offset denoted the non-last row. if exists row length is great than new row.
	 * overwrite the new row data from existed offset. return existed offset, else delete the
	 * existed offset,if there are enough un-utilization space, assign a new offset for the row,
	 * write row data to the new offset, return new offset, else do not write new row data, return
	 * DELETE_OFFSET.
	 *
	 * 3) update u_off and u_size whenever update offset or write row data
	 *
	 * 4) if does not find the offset and others, return UNKOWN_OFFSET
	 *
	 */
	int update_row_data(const unsigned short off, const char *rb,
			const int rl) {
		int r = UNKNOWN_OFFSET;
		int idx = index(off);
		if (idx != -1) {
			int o_rl = (idx == 0 ? length : off_tbl[idx - 1]) - off;
			if ((idx + 1) == off_tbl.size()) { // the last row
				if (enough_space(rl - o_rl)) { // enough space, maybe shrink existed row space
					int n_off = off + o_rl - rl;
					off_tbl[idx] = n_off;
					r = n_off;
					memcpy(buffer + n_off, rb, rl);
					// change the modification buffer range
					if (u_off_start > idx * offset_bytes)
						u_off_start = idx * offset_bytes;
					if (u_off_end < n_off + rl)
						u_off_end = n_off + rl;
				} else if (delete_row_off(off)) { // not enough space, only delete existed row offset
					r = DELETE_OFFSET;
				}
			} else if (o_rl >= rl) { // not last row, but existed row has enough space
				memcpy(buffer + off, rb, rl);
				if (u_off_end < off + rl)
					u_off_end = off + rl;
				r = off;
			} else if (remainder_space() >= rl) {
				// not last row, not enough space in existed row,
				// but the block has enough remainder space
				// delete original offset first, then assign new space.
				if (delete_row_off(off)) {
					if (enough_space(rl)) {
						int s = off_tbl.size();
						unsigned short l_off = off_tbl[s - 1];
						l_off -= rl;
						off_tbl.push_back(l_off);
						memcpy(buffer + l_off, rb, rl);
						r = l_off;

						// change the modification buffer range
						if (u_off_start > idx * offset_bytes)
							u_off_start = idx * offset_bytes;
						if (u_off_end < l_off)
							u_off_end = l_off;

					} else {
						r = DELETE_OFFSET;
					}
				}
			}

			calc_free_perct();
		}

		return r;
	}

	bool delete_row_off(const unsigned short off) {
		int idx = index(off);
		return delete_row_by_idx(idx);
	}

	bool delete_row_by_idx(int idx) {
		if (idx != -1) {
			unsigned short n = (1 << 15); /* the 15th bit as 1, DELETE flag*/
			unsigned short off = off_tbl[idx];
			off_tbl[idx] = off | n;

			if (u_off_start > idx * offset_bytes)
				u_off_start = idx * offset_bytes;
			return true;
		}
		return false;
	}

	int index(const unsigned short off) {
		auto it = find(off_tbl.begin(), off_tbl.end(), off);
		return it == off_tbl.end() ? -1 : it - off_tbl.begin();
	}

	/**
	 * write off table to the buffer
	 */
	void write_off_tbl() {
		common::char_buffer cb(length / 4);
		for (auto it = off_tbl.begin(); it != off_tbl.end(); ++it) {
			cb << (*it);
		}

		memcpy(buffer, cb.data(), cb.size());
	}
}
;

enum segment_type {
	index_segment, data_segment, undo_segment, redo_segment, tmp_segment
};

enum block_size_type {
	K_4 = 4, K_8 = 8, K_16 = 16, K_32 = 32, K_64 = 64
};

class segment;

class data_file {
	friend class segment;
private:
	int magic = data_file_magic;
	unsigned long id;
	std::string full_path;
	time_t create_time;  // data file object create time
	time_t update_time; // data file update time when modify physical disk file
	time_t attach_time; // first time that create disk file for data file object
	short version;
	char free_perct = 99; // disk space free percentage of current data file, 99 - 0;

	std::fstream fs;
	bool opened = false;
	bool closed = false;
	unsigned int length;
	unsigned int last_seg_offset = 0;

public:
	data_file() {
	}
	explicit data_file(std::string _full_path);
	explicit data_file(unsigned long id, std::string _full_path);
	explicit data_file(const data_file & another) = delete;
	virtual ~data_file();

	data_file operator=(const data_file & another) = delete;

	bool operator==(const data_file & another);

	/**
	 *  check exist of the file represented with full_path
	 */
	bool exist();

	/* if exist data file, open the file stream and verify data file head info,
	 * else create a new data file and write data file head info
	 */
	int open();

	int close();

	/**
	 * create physical disk file for the this object
	 * and write correct head information
	 */
	int create();

	void remove();

	/**
	 * assign disk space for a new segment, especially, the segment is empty
	 */
	int assgin_segment(segment & seg);

	/**
	 * write a in-memory segment to disk, both segment head and segment content
	 */
	int attach_segment(segment & seg);

	/**
	 * delete a segment from current data file
	 */
	int delete_segment(segment & seg);

	/**
	 * update a segment head information and entire content buffer to current data file.
	 */
	int update_segment(segment & seg);

	int seek_segment(const segment &seg);

	/**
	 * update a segment head information and apart content buffer to current data file.
	 */
	int update_segment(segment & seg, int offset, int length);

	/**
	 *  check has a segment from current position of the data file,
	 *  if read a valid segment from current position, store the segment into the parameter
	 */
	bool has_next_segment(segment & seg);

	/**
	 * skip a segment from current position within the data file inner stream
	 */
	void skip_segment(segment & seg);

	/**
	 * read a segment content from current position of the data file
	 */
	int fetch_segment(segment & seg);

	int fetch_segment_head(segment & seg);

	void fill_head_to_buff(common::char_buffer & buff);

	void fetch_head_from_buff(char_buffer & buff);

	bool is_valid();

	bool has_free_space() const {
		return free_perct > 0;
	}
};

class segment {
	friend class data_file;
private:
	int magic = segment_magic;
	unsigned long id; /* unique id for a segment */
	unsigned offset; /* offset position in a data file behind head */
	short status = segment_alive;
	int length;

	char free_perct = 99; /* current segment free space percentage 99 - 0 */
	time_t create_time;
	time_t assign_time;
	time_t update_time;

	data_file * d_file;
	int pre_seg_offset;
	int next_seg_offset;

	int block_size;

	bool has_buffer = false;
	char * content_buffer;

public:

	segment();

	segment(unsigned long _id);

	segment(unsigned long _id, data_file *_f, unsigned int _off = 0);

	segment(const segment & another);

	segment(segment && another);

	~segment();

	/**
	 *  serialize segment head information to char_buffer represented p_buff
	 */
	void fill_head_to_buff(char_buffer &buff);

	/**
	 * serialize entry segment information to char_buffer represented p_buff
	 */
	void fill_to_buff(char_buffer & buff);

	/**
	 *  fetch head information from char_buffer
	 */
	void fetch_head_from_buff(common::char_buffer & buff);

	/**
	 * fetch entry segment information from char_buffer
	 */
	void fetch_from_buff(common::char_buffer & buff);

	bool operator ==(const segment & other);

	time_t get_create_time() const {
		return create_time;
	}

	/**
	 * assign content buffer for the segment, if assign content buffer, the segment MUST delete pointer
	 * of the content buffer on the segment object destroy
	 */
	void assign_content_buffer() {
		if (!has_buffer) {
			has_buffer = true;
			content_buffer = new char[length - segment_head_size];
		}
	}
	unsigned long get_id() const {
		return id;
	}

	int get_length() const {
		return length;
	}

	void set_length(const int length) {
		this->length = length;
	}

	unsigned get_offset() const {
		return offset;
	}

	data_file * get_file() const {
		return d_file;
	}

	bool has_free_space() const {
		return free_perct > 0;
	}

	/**
	 *  determine free space exceeding reversed space for append operation
	 */
	bool exceeded() {
		return free_perct <= segment_space_reversed_for_insert;
	}

	inline bool is_valid() {
		return magic == segment_magic && id > unused_id && offset >= 0;
	}

	time_t get_assign_time() const {
		return assign_time;
	}

	void set_assign_time(time_t assigntime) {
		assign_time = assigntime;
	}

	int get_magic() const {
		return magic;
	}

	int get_next_seg_offset() const {
		return next_seg_offset;
	}

	void set_next_seg_offset(int nextsegoffset) {
		next_seg_offset = nextsegoffset;
	}

	void set_offset(unsigned offset) {
		this->offset = offset;
	}

	int get_pre_seg_offset() const {
		return pre_seg_offset;
	}

	void set_pre_seg_offset(int presegoffset) {
		pre_seg_offset = presegoffset;
	}

	short get_status() const {
		return status;
	}

	void set_status(short status = segment_alive) {
		this->status = status;
	}

	void update(int dest_off, const char * source, int src_off, int len) {
		memcpy(content_buffer + dest_off, source + src_off, len);
	}

	/**
	 * read segment content buffer from offset, length of chars to buff,
	 * do not check the buffer length.
	 */
	void read(char * buff, int offset, int len) {
		memcpy(buff, content_buffer + offset, len);
	}

	int get_block_size() const {
		return block_size;
	}

	void set_block_size(block_size_type t) {
		block_size = t * kilo_byte;
	}

	char* get_content_buffer() const {
		return content_buffer;
	}

	char get_free_perct() const {
		return free_perct;
	}

	bool is_has_buffer() const {
		return has_buffer;
	}

	time_t get_update_time() const {
		return update_time;
	}
};

} /* namespace storage */

#endif /* STORAGE_DATAFILE_H_ */
