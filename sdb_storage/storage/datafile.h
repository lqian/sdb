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

namespace sdb {

const int SUCCESS = 0;
const int FAILURE = -1;

namespace storage {

using namespace std;
using namespace common;

const int unused_id = 0;
const unsigned int data_file_magic = 0x7A9E401C;
const unsigned int block_magic = 0x98C2E0F9; /* default block magic */
const unsigned int segment_magic = 0x50FED398;

const short data_file_head_size = 1024;
const short data_file_version = 0x10;
const long data_file_max_size = 0x10000000;  // 4G

const short segment_head_size = 1024;
const short segment_tail_size = 1024;
const char segment_deleted = 0xff;
const char segment_alive = 0x01;
const short block_space_reversed_for_update = 20;

const int M_64 = 67108864;
const int M_128 = 134217728;
const int M_256 = 268435456;
const int M_512 = 536870912;

const int kilo_byte = 1024;

const int UNKNOWN_OFFSET = -1;
const int DELETE_OFFSET = -2;

const int offset_bytes = 2;

const int PRE_SEG_SPAN_DFILE_BIT = 0;
const int NEXT_SEG_SPAN_DFILE_BIT = 1;

const short block_header_size = 48;

/*
 * block head flag bit define
 */
const int REMOVED_BLK_BIT = 0;
const int NEXT_BLK_BIT = 1;
const int PRE_BLK_BIT = 2;
const int NEXT_BLK_SPAN_SEG_BIT = 3;
const int PRE_BLK_SPAN_SEG_BIT = 4;
const int NEXT_BLK_SPAN_DFILE_BIT = 5;
const int PRE_BLK_SPAN_DFILE_BIT = 6;

const int BLK_OFF_OVERFLOW = -2;
const int BLK_OFF_INVALID = -3;
const int NO_PRE_BLK = -4;
const int NO_NEXT_BLK = -5;
const int NO_PARENT_BLK=-6;
const int SEGMENT_NOT_ASSIGNED = -0x100;
const int SEGMENT_IS_FULL = -0x101;

class segment;

struct data_block;

enum segment_type {
	index_segment_type = 0x30,
	data_segment_type,
	undo_segment_type,
	redo_segment_type,
	tmp_segment_type
};

enum block_size_type {
	K_4 = 4, K_8 = 8, K_16 = 16, K_32 = 32, K_64 = 64
};

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

	int write_block(segment & seg, data_block &blk,
			bool update_seg_head = true);

	int read_block(segment &seg, data_block &blk);

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

	inline const unsigned long get_id() {
		return id;
	}
};

class segment {
	friend class data_file;
protected:
	int magic = segment_magic;
	unsigned long id; /* unique id for a segment */
	unsigned offset; /* offset position in a data file behind head */
	short status = segment_alive;
	int length; /* total length of the segment, include header size*/
	short seg_type = data_segment_type;

	time_t create_time;
	time_t assign_time;
	time_t update_time;

	data_file * d_file = nullptr;

	char span_dfile_flag = 0; /* the 0-bit, 1 denotes pre_seg_offset is in another data file,
	 the 1-bit, 1 denotes next_seg_offset is in another data file */
	unsigned int pre_seg_offset = 0;
	unsigned int next_seg_offset = 0;
	unsigned long pre_seg_id = 0;
	unsigned long next_seg_id = 0;
	unsigned long pre_seg_dfile_id = 0;
	unsigned long next_seg_dfile_id = 0;

	std::string pre_seg_dfile_path;
	std::string next_seg_dfile_path;

	int block_size;
	int block_count = 0;

	char * content_buffer = nullptr;

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

	/*
	 * sequentially add a new block to the segment. if the segment has enough remained space
	 * for assignation of a new data block, increment segment block count in memory. if the
	 * segment has assigned content buffer, the assigning block buffer references some place
	 * of segment buffer, else assign new buffer for the block.
	 *
	 * for eventually write blocks to disk, outside should invoke flush, flush_head or
	 *  flush_blcok method. operation of block has the same manner.
	 */
	int assign_block(data_block & blk);
	int remove_block(data_block & blk);
	int flush_block(data_block & blk);
	int read_block(data_block &blk);

	int flush();

	int flush(int off, int len);

	bool operator ==(const segment & other);
	segment & operator=(const segment & other);

	inline time_t get_create_time() const {
		return create_time;
	}

	/**
	 * assign content buffer for the segment, if assign content buffer, the segment MUST delete pointer
	 * of the content buffer on the segment object destroy
	 */
	inline void assign_content_buffer() {
		content_buffer = new char[length - segment_head_size];
	}

	inline void free_buffer() {
		if (content_buffer) {
			delete[] content_buffer;
			content_buffer = nullptr;
		}
	}

	inline unsigned long get_id() const {
		return id;
	}

	inline int get_length() const {
		return length;
	}

	inline void set_length(const int length) {
		this->length = length;
	}

	inline unsigned get_offset() const {
		return offset;
	}

	inline data_file * get_file() const {
		return d_file;
	}

	inline bool has_pre_seg() {
		return pre_seg_offset > 0
				|| ((span_dfile_flag >> PRE_SEG_SPAN_DFILE_BIT) & 1) == 1;
	}

	inline bool has_nex_seg() {
		return next_seg_offset > 0
				|| ((span_dfile_flag >> NEXT_SEG_SPAN_DFILE_BIT) & 1) == 1;
	}

	int next_seg(segment & seg) {
		if (this->d_file == nullptr) {
			//ignore this case
		} else if (d_file->id != pre_seg_dfile_id) {
			//the code cause invalid pointer
			data_file df(next_seg_dfile_id, next_seg_dfile_path);
			seg.offset = pre_seg_offset;
			seg.d_file = &df;
			df.open();
			return df.fetch_segment(seg);
		} else {
			seg.d_file = this->d_file;
			seg.offset = next_seg_offset;
			seg.id = next_seg_id;
			return d_file->fetch_segment(seg);
		}
		return sdb::FAILURE;
	}

	int pre_seg(segment & seg) {
		if (this->d_file == nullptr) {
			//ignore this case
		} else if (d_file->id != pre_seg_dfile_id) {
			//the code cause invalid pointer
			data_file df(pre_seg_dfile_id, pre_seg_dfile_path);
			seg.offset = pre_seg_offset;
			seg.d_file = &df;
			return df.fetch_segment(seg);
		} else {
			seg.d_file = this->d_file;
			seg.offset = pre_seg_offset;
			seg.id = pre_seg_id;
			return d_file->fetch_segment(seg);
		}
		return sdb::FAILURE;
	}

	inline bool is_valid() {
		return magic == segment_magic && id > unused_id;
	}

	inline time_t get_assign_time() const {
		return assign_time;
	}

	inline void set_assign_time(time_t assigntime) {
		assign_time = assigntime;
	}

	inline int get_magic() const {
		return magic;
	}

	inline int get_next_seg_offset() const {
		return next_seg_offset;
	}

	inline void set_next_seg_offset(int nextsegoffset) {
		next_seg_offset = nextsegoffset;
	}

	inline void set_offset(unsigned offset) {
		this->offset = offset;
	}

	inline int get_pre_seg_offset() const {
		return pre_seg_offset;
	}

	inline void set_pre_seg_offset(int presegoffset) {
		pre_seg_offset = presegoffset;
	}

	inline short get_status() const {
		return status;
	}

	inline int remain() {
		return length - segment_head_size - block_count * block_size;
	}

	inline void set_status(short status = segment_alive) {
		this->status = status;
	}

	inline void update(int dest_off, const char * source, int src_off,
			int len) {
		memcpy(content_buffer + dest_off, source + src_off, len);
	}

	/**
	 * read segment content buffer from offset, length of chars to buff,
	 * do not check the buffer length.
	 */
	inline void read(char * buff, int offset, int len) {
		memcpy(buff, content_buffer + offset, len);
	}

	inline int get_block_size() const {
		return block_size;
	}

	inline void set_block_size(block_size_type t) {
		block_size = t * kilo_byte;
	}

	inline char* get_content_buffer() const {
		return content_buffer;
	}

	inline bool has_buffer() const {
		return content_buffer != nullptr;
	}

	inline time_t get_update_time() const {
		return update_time;
	}

	inline int get_block_count() const {
		return block_count;
	}

	short int get_seg_type() const {
		return seg_type;
	}
};

/*
 * a common struct for data block stored in segmentF
 */
struct data_block {
	struct head {
		unsigned int blk_magic;
		time_t create_time;
		time_t assign_time;
		time_t update_time;
		short flag = 0;
		int pre_blk_off;
		int next_blk_off;
		int checksum;
	}*header = nullptr;

	bool ref_flag = false;
	int offset; /* block offset from start of segment which the block belong to,
	 the first data block offset is 0 within a segment */

	char *buffer; /* all data store in the buffer, include ROW DIRECTORY & DATA,
	 but excludes data block header */
	short length; /* total length of buffer, excludes header length */

	short u_off_start = 0;
	short u_off_end = 0; /* update offset start and end  if the block buffer has been modified */

	data_block *pre_blk, *next_blk;
	segment *seg;

	void init(int off, short len) {
		offset = off;
		length = len;
		buffer = new char[length + block_header_size];
		header = (head *) buffer;
		init_header();
		buffer += block_header_size;
		u_off_start = 0;
		u_off_end = 0;
	}

	/*
	 * reference the data header and content from a given buff parameter that denoted
	 * the header start position. the off parameter denotes the data block offset beh-
	 * hind segment header.
	 */
	virtual void ref(int off, char *buff, short len) {
		offset = off;
		length = len;
		header = (head *) buff;
		buffer = buff + block_header_size;
		ref_flag = true;
		u_off_start = 0;
		u_off_end = 0;

	}

	virtual void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	void set_pre_block(data_block * p) {
		pre_blk = p;
		p->next_blk = this;
		header->pre_blk_off = p->offset;
		p->header->next_blk_off = offset;
		set_flag(PRE_BLK_BIT);
		p->set_flag(NEXT_BLK_BIT);
		if (seg != p->seg) {
			set_flag(PRE_BLK_SPAN_SEG_BIT);
			p->set_flag(NEXT_BLK_SPAN_SEG_BIT);
			if (seg->get_file() != p->seg->get_file()) {
				set_flag(PRE_BLK_SPAN_DFILE_BIT);
				p->set_flag(NEXT_BLK_SPAN_DFILE_BIT);
			} else {
				remove_flag(PRE_BLK_SPAN_DFILE_BIT);
				p->remove_flag(NEXT_BLK_SPAN_DFILE_BIT);
			}
		} else {
			remove_flag(PRE_BLK_SPAN_SEG_BIT);
			p->remove_flag(NEXT_BLK_SPAN_SEG_BIT);
		}
	}

	void set_next_blk(data_block *p) {
		next_blk = p;
		p->pre_blk = this;
		header->next_blk_off = p->offset;
		p->header->pre_blk_off = offset;
		set_flag(NEXT_BLK_BIT);
		p->set_flag(PRE_BLK_BIT);
		if (seg != p->seg) {
			set_flag(NEXT_BLK_SPAN_SEG_BIT);
			p->set_flag(PRE_BLK_SPAN_SEG_BIT);
			if (seg->get_file() != p->seg->get_file()) {
				set_flag(NEXT_BLK_SPAN_DFILE_BIT);
				p->set_flag(PRE_BLK_SPAN_DFILE_BIT);
			} else {
				remove_flag(NEXT_BLK_SPAN_DFILE_BIT);
				p->remove_flag(PRE_BLK_SPAN_DFILE_BIT);
			}
		} else {
			remove_flag(NEXT_BLK_SPAN_SEG_BIT);
			p->remove_flag(PRE_BLK_SPAN_SEG_BIT);
		}
	}

	virtual void set_flag(int bit) {
		header->flag |= (1 << bit);
	}

	virtual void remove_flag(int bit) {
		header->flag &= ~(1 << bit);
	}

	virtual bool test_flag(int b) {
		return (header->flag >> b & 1) == 1;
	}

	virtual ~ data_block() {
//		if (!ref_flag) {
//			delete header;
//			delete[] buffer;
//		}
	}

};

/**
 * variant-size row of data block buffer internal:
 * +------+---+---+---+----------------------------+------+------+------+
 * |      |   |   |   |                            |      |      |      |
 * |      | O | O | O |                            |      |      |      |
 * | ROW  | F | F | F |    UN-UTILIZAION SPACE     | ROW3 | ROW2 | ROW1 |
 * | HEAD | F | F | F |                            | DATA | DATA | DATA |
 * |      | 1 | 2 | 3 |                            |      |      |      |
 * +------+---+---+---+----------------------------+------+------+------+
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
struct mem_data_block: data_block {
	vector<unsigned short> off_tbl;
	short free_perct = 100;

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
				memcpy(buffer + l_off, rb, rl);
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
		common::char_buffer cb(length / 10);
		for (auto it = off_tbl.begin(); it != off_tbl.end(); ++it) {
			cb << (*it);
		}

		memcpy(buffer, cb.data(), cb.size());
	}

	virtual ~ mem_data_block() {
		if (!ref_flag) {
			delete header;
			delete[] buffer;
		}
	}
}
;

} /* namespace storage */
} /* namespace sdb */

#endif /* STORAGE_DATAFILE_H_ */
