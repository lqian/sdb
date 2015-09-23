/*
 * log_mgr.h
 *
 *  Created on: Sep 11, 2015
 *      Author: lqian
 */

#ifndef LOG_MGR_H_
#define LOG_MGR_H_

#include <cstring>
#include <fstream>
#include <map>
#include "trans_mgr.h"
#include "../common/char_buffer.h"
#include "../common/sio.h"

namespace sdb {
namespace enginee {
using namespace std;
using namespace sdb::common;

const string CHECKPOINT_FILE_SUFFIX = ".chk";
const string LOG_FILE_SUFFIX = ".log";

const int LOG_FILE_MAGIC = 0x7FA9C83D;
const int LOG_BLOCK_MAGIC = 0xF79A8CD3;

const int LOG_FILE_HEADER_LENGTH = 1024;
const int LOG_BLOCK_HEADER_LENGTH = 24;
const int DIRCTORY_ENTRY_LENGTH = 18;

const int LOG_BLK_SPACE_NOT_ENOUGH = -0X500;
const int OUTOF_ENTRY_INDEX = -0X501;


class log_file;

enum log_status {
	initialized = 0, active, opened, closed
};

enum log_sync {
	immeidate, buffered, async
};

class undo_buffer {
private:
	int capacity;
public:
	int log_action(timestamp ts, action & a);
	int log_commit(timestamp ts);
	int log_rollback(timestamp ts);
	int log_abort(timestamp *ts);
};

class log_mgr {
	friend class log_file;
private:
	log_sync sync = log_sync::immeidate;
	string path;  // log directory that contains log file
	undo_buffer ub;
	char * log_buffer;
	int lb_cap = 1048576; // log buffer capacity
	int log_file_max_size = 67108864;
public:
	int log_action(timestamp ts, action & a);
	int log_commit(timestamp ts);
	int log_rollback(timestamp ts);
	int log_abort(timestamp *ts);

	int find_log(timestamp ts, char_buffer & buff);
	int redo_log();
	int undo_log();

	/*
	 * scan from the end of log, seek old data item for a transaction specified with timestamp
	 */
	int undo_log(timestamp ts);
	int check();

	void set_log_file_max_size(int size = 67108864);

	log_mgr();
	virtual ~log_mgr();
};


class log_block {
	friend class log_file;
	struct header {
		unsigned int magic = 0xF79A8CD3;
		unsigned int offset = 0;
		unsigned int pre_blk_off = 0;
		unsigned int next_blk_off = 0;
		unsigned int writing_entry_off = 0;
		int remain_space = 0;

		void write_to(char_buffer & buff);
		void read_from(char_buffer & buff);
	};

	struct dir_entry {
		timestamp ts;
		unsigned short seq;
		unsigned int offset;
		unsigned int length;

		void write_to(char_buffer & buff);
		void read_from(char_buffer & buff);
	};
private:
	bool ref_flag;
	header _header;
	char *buffer;
	int length;
	int read_entry_off = 0;
public:
	log_block(int block_size = 4096);
	log_block(char *buff, int block_size = 4096);
	log_block(const log_block & another);
	log_block & operator=(const log_block & antoher);
	bool operator==(const log_block & another)=delete;
	~log_block();
	int add_action(timestamp ts, action & a);
	void head();
	void tail();
	bool has_next();
	bool has_pre();
	void pre_entry(char_buffer & buff);
	void next_entry(char_buffer & buff);
	int get_entry(int idx, char_buffer & buff);
	int count_entry();

	void set_block_size(const int block_size);
	void assign_block_buffer();
	void ref_buffer(char * buff, int len);
	void write_to(char * buff);
};

class log_file {
	struct header {
		unsigned int magic = LOG_FILE_MAGIC;
		int block_size = 4096;
		bool active;

		void write_to(char_buffer & buff);
		void read_from(char_buffer & buff);
		bool is_valid();
	};
private:
	bool initialized = false;
	string pathname;
	fstream log_stream;
	header _header;
	int checked_offset = 0;
	log_block last_blk;  // the last log block for append log entry
	char * block_buffer;
	log_mgr * _log_mgr = nullptr;


	int init_file_header();
	int renewal_last_block();
	int flush_last_block();

	/*
	 * append buffer that contains several log blocks. must flush the last log block
	 * before append those blocks
	 */
	int append(const char * buff, int len);
public:
	log_file(const string& fn);
	log_file(const log_file & another) = delete;
	log_file(const log_file && another) = delete;
	~log_file();

	void set_log_mgr(log_mgr * lm);
	int open();

	int close();

	int append(timestamp ts, action& a);

	bool has_next_block();
	int next_block(char_buffer & buff);
	bool is_active();
};

class log_check_file {

};

} /* namespace enginee */
} /* namespace sdb */

#endif /* LOG_MGR_H_ */
