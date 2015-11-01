/*
 * datafile.h
 *
 *  Created on: Apr 20, 2015
 *      Author: linkqian
 */

#ifndef STORAGE_DATAFILE_H_
#define STORAGE_DATAFILE_H_

#include <cstring>
#include <ctime>
#include <string>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iostream>
#include "../common/char_buffer.h"
#include "../sdb_def.h"

namespace sdb {

namespace storage {

using namespace std;
using namespace sdb::common;

const unsigned int valid_data_file_id = 0;
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

const int M_1 = 1048576;
const int M_2 = 2097152;
const int M_4 = 4194304;
const int M_8 = 8388608;
const int M_16 = 16777216;
const int M_32 = 33554432;
const int M_64 = 67108864;
const int M_128 = 134217728;
const int M_256 = 268435456;
const int M_512 = 536870912;

const int kilo_byte = 1024;

const int UNKNOWN_OFFSET = -1;
const int DELETE_OFFSET = -2;
const int ROW_NOT_FOUND = -3;
const int ROW_NOT_IN_BLOCK = -4;

const int offset_bytes = 2;

const int PRE_SEG_SPAN_DFILE_BIT = 0;
const int NEXT_SEG_SPAN_DFILE_BIT = 1;

const short block_head_size = 48;
const short mem_block_head_size = 56;

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
const int NO_PARENT_BLK = -6;
const int SEGMENT_NOT_ASSIGNED = -0x100;
const int SEGMENT_IS_FULL = -0x101;

const int IO_READ_BLOCK_ERROR = -0x200;
const int IO_READ_SEGMENT_ERROR = -0x201;
const int ROW_DATA_SIZE_TOO_LARGE = -0x202;

/* row data flag define */
const int ROW_DELETED_FLAG_BIT = 15;

class segment;

struct data_block;

enum segment_type {
	system_segment_type = 0x10,
	index_segment_type = 0x30,
	data_segment_type,
	log_segment_type,
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
	unsigned int length = 0; // content length of data file, excludes data file head length
	unsigned int last_seg_offset = 0;

public:
	data_file() {
	}
	data_file(std::string _full_path);
	data_file(unsigned long id, std::string _full_path);
	data_file(const data_file & another);
	virtual ~data_file();

	data_file & operator=(const data_file & another);

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

	/*
	 * create physical disk file for the this object
	 * and write correct head information
	 */
	int create();

	void remove();

	/**
	 * assign disk space for a new segment, especially, the segment is empty
	 */
	int assign_segment(segment & seg);

	int assign_segment(segment * seg);

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
	int flush_segment(segment & seg);

	int flush_segment(segment * seg);

	int seek_segment(const segment &seg);

	/**
	 * update a segment head information and apart content buffer to current data file.
	 */
	int flush_segment(segment & seg, int offset, int length);

	int write_block(segment & seg, data_block &blk,
			bool update_seg_head = true);

	int read_block(segment &seg, data_block &blk);

	int read_block(segment *seg, data_block *blk);

	/**
	 *  check has a segment from current position of the data file,
	 *  if read a valid segment from current position, store the segment into the parameter
	 */
	bool has_next_segment(segment & seg);

	/**
	 * skip a segment from current position within the data file inner stream
	 */
	void skip_segment(segment & seg);

	/*
	 * read a segment content from the data file, which segment with specified offset
	 */
	int fetch_segment(segment & seg);

	/*
	 * read a segment content from the data file, which segment with specified offset
	 */
	int fetch_segment(segment * seg);

	/*
	 * from current position, read segment content;
	 */
	int next_segment(segment *seg);

	int fetch_segment_head(segment & seg);

	void fill_head_to_buff(common::char_buffer & buff);

	void fetch_head_from_buff(char_buffer & buff);

	bool is_valid();

	inline bool is_open() {
		return opened;
	}

	inline bool has_free_space() const {
		return free_perct > 0;
	}

	inline const unsigned long get_id() {
		return id;
	}

	inline void set_id(unsigned long id) {
		this->id = id;
	}

	inline void set_path(const std::string & full_path) {
		this->full_path = full_path;
	}

	const string & get_path() {
		return this->full_path;
	}
};

class segment {
	friend class data_file;
protected:
	int magic = segment_magic;
	unsigned long id; /* unique id for a segment */
	unsigned int offset; /* offset position in a data file behind head, includes segment head */
	short status = segment_alive;
	int length = 0; /* total length of the segment, include header size*/
	short seg_type = data_segment_type;

	time_t create_time = 0;
	time_t assign_time = 0;
	time_t update_time = 0;

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
	virtual int assign_block(data_block & blk);
	virtual int assign_block(data_block * blk);
	virtual int remove_block(data_block & blk);
	virtual int flush_block(data_block & blk);
	virtual int read_block(data_block &blk);
	virtual int read_block(data_block *blk);

	int flush();

	int flush(int off, int len);

	virtual bool operator ==(const segment & other);
	virtual bool operator !=(const segment & other);
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

	inline bool no_length() {
		return length == 0;
	}

	inline unsigned int last_block_off() {
		return block_count == 0 ? 0 : (block_count - 1) * block_size;
	}

	inline unsigned long get_id() const {
		return id;
	}

	inline void set_id(unsigned long id) {
		this->id = id;
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
		return pre_seg_offset > 0 || pre_seg_id
				|| ((span_dfile_flag >> PRE_SEG_SPAN_DFILE_BIT) & 1) == 1;
	}

	inline bool has_nex_seg() {
		return next_seg_offset > 0 || next_seg_id
				|| ((span_dfile_flag >> NEXT_SEG_SPAN_DFILE_BIT) & 1) == 1;
	}

	inline void set_next_seg(segment * seg) {
		next_seg_id = seg->id;
		next_seg_offset = seg->offset;
		seg->pre_seg_id = id;
		seg->pre_seg_offset = offset;

		if (d_file->id != seg->d_file->id) {
			span_dfile_flag |= (1 << NEXT_SEG_SPAN_DFILE_BIT);
			seg->span_dfile_flag |= (1 << PRE_SEG_SPAN_DFILE_BIT);
		}
	}

	inline void set_pre_seg(segment * seg) {
		pre_seg_id = seg->id;
		pre_seg_offset = seg->offset;
		seg->next_seg_id = id;
		seg->next_seg_offset = offset;

		if (d_file->id != seg->d_file->id) {
			span_dfile_flag |= (1 << PRE_SEG_SPAN_DFILE_BIT);
			seg->span_dfile_flag |= (1 << NEXT_SEG_SPAN_DFILE_BIT);
		}
	}

	inline int next_seg(segment & seg) {
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

	inline int next_seg(segment * seg) {
		if (this->d_file == nullptr) {
			//ignore this case
		} else if (d_file->id != pre_seg_dfile_id) {
			//the code cause invalid pointer
			data_file df(next_seg_dfile_id, next_seg_dfile_path);
			seg->offset = pre_seg_offset;
			seg->d_file = &df;
			df.open();
			return df.fetch_segment(seg);
		} else {
			seg->d_file = this->d_file;
			seg->offset = next_seg_offset;
			seg->id = next_seg_id;
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

	int pre_seg(segment * seg) {
		if (this->d_file == nullptr) {
			//ignore this case
		} else if (d_file->id != pre_seg_dfile_id) {
			//the code cause invalid pointer
			data_file df(pre_seg_dfile_id, pre_seg_dfile_path);
			seg->offset = pre_seg_offset;
			seg->d_file = &df;
			return df.fetch_segment(seg);
		} else {
			seg->d_file = this->d_file;
			seg->offset = pre_seg_offset;
			seg->id = pre_seg_id;
			return d_file->fetch_segment(seg);
		}
		return sdb::FAILURE;
	}

	inline bool is_valid() {
//		return magic == segment_magic && id > unused_id;
		return magic == segment_magic;
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

	inline bool has_remain_block() {
		return remain() >= block_size;
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

	inline bool has_assigned() const {
		return assign_time > 0 && content_buffer != nullptr;
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

	unsigned long get_next_seg_dfile_id() const {
		return next_seg_dfile_id;
	}

	void set_next_seg_dfile_id(unsigned long nextSegDfileId = 0) {
		next_seg_dfile_id = nextSegDfileId;
	}

	unsigned long get_next_seg_id() const {
		return next_seg_id;
	}

	void set_next_seg_id(unsigned long nextSegId = 0) {
		next_seg_id = nextSegId;
	}

	unsigned long get_pre_seg_dfile_id() const {
		return pre_seg_dfile_id;
	}

	void set_pre_seg_dfile_id(unsigned long preSegDfileId = 0) {
		pre_seg_dfile_id = preSegDfileId;
	}

	unsigned long get_pre_seg_id() const {
		return pre_seg_id;
	}

	void set_pre_seg_id(unsigned long preSegId = 0) {
		pre_seg_id = preSegId;
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
		ushort flag = 0;
		int pre_blk_off;
		int next_blk_off;
		int checksum;
	}*header = nullptr;

	bool ref_flag = false;
	uint offset; /* block offset from start of segment which the block belong to,
	 the first data block offset is 0 within a segment */

	char *buffer; /* all data store in the buffer, include ROW DIRECTORY & DATA,
	 but excludes data block header */
	ushort length; /* total length of buffer, excludes header length */

	/* update offset start and end  if the block buffer has been modified */
	//short u_off_start = 0;
	//short u_off_end = 0;
	data_block *pre_blk, *next_blk;
	segment *seg;

	inline virtual void init(int off, ushort w_len) {
		offset = off;
		length = w_len - block_head_size;
		buffer = new char[length + block_head_size];
		header = (head *) buffer;
		init_header();
		buffer += block_head_size;
		//u_off_start = 0;
		//u_off_end = 0;
	}

	/*
	 * reference the data header and content from a given buff parameter that denoted
	 * the header start position. the off parameter denotes the data block offset beh-
	 * hind segment header.
	 */
	inline virtual void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - block_head_size;
		header = (head *) buff;
		buffer = buff + block_head_size;
		ref_flag = true;
		//u_off_start = 0;
		//u_off_end = 0;
	}

	virtual void init_header()=0;
//	{
//		header->flag = 0;
//		header->blk_magic = block_magic;
//		time(&(header->create_time));
//	}

	inline virtual void set_assign_time(long t) {
		header->assign_time = t;
	}

	inline void set_pre_block(data_block * p) {
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

	inline void set_next_blk(data_block *p) {
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

	inline virtual void set_flag(int bit) {
		header->flag |= (1 << bit);
	}

	inline virtual void remove_flag(int bit) {
		header->flag &= ~(1 << bit);
	}

	inline virtual bool test_flag(int b) {
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
	struct head: data_block::head {
		ushort dir_off = 0;
	}* header = nullptr;

	short free_perct = 100;
	unsigned short entry_count = 0;
	unsigned short * p_dir_off = nullptr;

	inline virtual void init(int off, short w_len) {
		offset = off;
		length = w_len - sizeof(head);
		buffer = new char[length];
		header = (head *) buffer;
		init_header();
		buffer += block_head_size;
		p_dir_off = (unsigned short *) buffer;
	}

	inline virtual void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		header->dir_off = 0;
		time(&(header->create_time));
	}

	/*
	 * reference the data header and content from a given buff parameter that denoted
	 * the header start position. the off parameter denotes the data block offset beh-
	 * hind segment header.
	 */
	inline virtual void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - mem_block_head_size;
		header = (head *) buff;
		buffer = buff + mem_block_head_size;
		p_dir_off = (unsigned short *) buffer;
		ref_flag = true;
		if (header->dir_off > 0) {
			entry_count = (header->dir_off >> 1);
		}
	}

	inline virtual void set_assign_time(long t) {
		header->assign_time = t;
	}

	inline void calc_free_perct() {
		if (entry_count) {
			free_perct = 100
					* (length - header->dir_off - p_dir_off[entry_count - 1])
					/ length;
		}
	}

	inline int get_row_by_idx(int idx, char_buffer & buff) {
		if (idx >= 0 && idx < entry_count) {
			unsigned short offset = p_dir_off[idx];
			if (offset >> ROW_DELETED_FLAG_BIT) {
				return DELETE_OFFSET;
			}
			int rl = -1;
			if (idx == 0) {
				rl = length - offset;
			} else {
				rl = p_dir_off[idx-1] - offset;
			}

			buff.ref_buff(buffer + offset, rl);
			return sdb::SUCCESS;
		} else {
			return idx;
		}
	}

	inline int get_row(const unsigned short off, char_buffer & buff) {
		int r = index(off);
		return r == -1 ? UNKNOWN_OFFSET : get_row_by_idx(r, buff);
	}

	inline bool enough_reserved_space(short len) {
		return 100 * (p_dir_off[entry_count - 1] - header->dir_off - len)
				/ length >= 20;
	}

	inline int remain() {
		return p_dir_off[entry_count - 1] - header->dir_off;
	}

	inline bool enough_space(short len) {
		return p_dir_off[entry_count - 1] - header->dir_off >= len;
	}

	/**
	 * add a row data denoted by rl length of rb.
	 * if there isn't any row in the block and length of row is less than block length, add it,
	 * else keep 20% un-utilization disk space at least.
	 */
	inline int add_row_data(const char* rb, const int rl) {
		int r = UNKNOWN_OFFSET;
		if (entry_count == 0) {
			// there isn't any ROW DATA in the data block
			if (rl <= length - offset_bytes) {
				unsigned short l_off = length - rl;
				memcpy(buffer + l_off, rb, rl);
				r = entry_count;
				p_dir_off[r] = l_off;
				header->dir_off += offset_bytes;
				entry_count++;
			}
		} else if (enough_reserved_space(rl)) {
			// make sure that reserve 20% free space in the data block
			unsigned short off = p_dir_off[entry_count - 1];
			off -= rl;
			memcpy(buffer + off, rb, rl);
			r = entry_count;
			p_dir_off[r] = off;
			header->dir_off += offset_bytes;
			entry_count++;
		}
		return r;
	}

	/*
	 * assign a space for row with specified length, if assign success,
	 * return row offset index, else return UNKNOWN_OFFSET
	 */
	inline int assign_row(const int & rl) {
		int r = UNKNOWN_OFFSET;
		if (entry_count == 0) {
			// there isn't any ROW DATA in the data block
			if (rl <= length - offset_bytes) {
				unsigned short l_off = length - rl;
				r = entry_count;
				p_dir_off[r] = l_off;
				header->dir_off += offset_bytes;
				entry_count++;
			}
		} else if (enough_reserved_space(rl)) {
			// make sure that reserve 20% free space in the data block
			unsigned short off = p_dir_off[entry_count - 1];
			off -= rl;
			r = entry_count;
			p_dir_off[r] = off;
			header->dir_off += offset_bytes;
			entry_count++;
		}
		return r;
	}

	/**
	 * update a existed row with a new given row and the row's existed index, return new index
	 * if update successfully, else DELETE_OFFSET or UNKOWN_OFFSET.
	 *
	 * 1) if the index denote the last row, and there are enough un-utilization remainder space,
	 * update the offset to point the existed offset, save row data. whereas are not enough un-utilization
	 * space. delete the exist index, return DELETE_OFFSET
	 *
	 * 2) if the offset denoted the non-last row. if exists row length is great than new row.
	 * overwrite the new row data from existed offset. return existed index, else delete the
	 * existed offset,if there are enough un-utilization space, assign a new offset for the row,
	 * write row data to the new offset, return new index, else do not write new row data, return
	 * DELETE_OFFSET.
	 *
	 * 3) update u_off and u_size whenever update offset or write row data
	 *
	 * 4) if does not find the offset and others, return UNKOWN_OFFSET
	 *
	 */
	inline int update_row_by_index(int idx, const char *rb, const int rl) {
		int r = UNKNOWN_OFFSET;
		if (idx < 0) {
			return r;
		}
		int off = p_dir_off[idx];
		int o_rl = (idx == 0 ? length : p_dir_off[idx - 1]) - off;
		if (o_rl == rl) {
			memcpy(buffer + off, rb, rl);
			r = idx;
		} else if (idx + 1 == entry_count) { // the last row
			if (enough_space(rl - o_rl)) {
				// enough space, maybe shrink existed row space
				int n_off = off + o_rl - rl;
				memcpy(buffer + n_off, rb, rl);
				p_dir_off[idx] = n_off;
				r = idx;
			} else if (delete_row_by_idx(idx)) {
				// not enough space, only delete existed row offset
				r = DELETE_OFFSET;
			}
		} else if (o_rl >= rl) {
			// not last rows, but existed row has enough space
			memcpy(buffer + off, rb, rl);
			r = idx;
		} else if (remain() >= rl) {
			// not last row, not enough space in existed row,
			// but the block has enough remainder space
			// delete original offset first, then assign new space.
			delete_row_by_idx(idx);
			unsigned short l_off = p_dir_off[entry_count - 1];
			l_off -= rl;
			memcpy(buffer + l_off, rb, rl);
			p_dir_off[entry_count] = l_off;
			header->dir_off += offset_bytes;
			r = entry_count;
			entry_count++;
		} else {
			r = DELETE_OFFSET;
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
	 */
	inline int update_row_data(const unsigned short row_off, const char *rb,
			const int rl) {
		int r = UNKNOWN_OFFSET;
		int idx = index(row_off);
		return idx == -1 ? r : update_row_by_index(idx, rb, rl);
	}

	inline bool delete_row_off(const unsigned short off) {
		int idx = index(off);
		return delete_row_by_idx(idx);
	}

	inline bool delete_row_by_idx(int idx) {
		if (idx != -1 && idx < entry_count) {
			unsigned short n = (1 << ROW_DELETED_FLAG_BIT); /* the 15th bit as 1, DELETE flag*/
			unsigned short off = p_dir_off[idx];
			p_dir_off[idx] = off | n;
			return true;
		} else {
			return false;
		}
	}

	inline int index(const unsigned short row_off) {
		for (int i = 0; i < entry_count; i++) {
			if (p_dir_off[i] == row_off) {
				return i;
			}
		}
		return -1;
	}

	virtual ~ mem_data_block() {
//		if (!ref_flag) {
//			delete header;
//			delete[] buffer;
//		}
	}
}
;

} /* namespace storage */
} /* namespace sdb */

#endif /* STORAGE_DATAFILE_H_ */
