/*
 * log_mgr.h
 *
 *  Created on: Sep 11, 2015
 *      Author: lqian
 */

#ifndef LOG_MGR_H_
#define LOG_MGR_H_

#include <cstring>
#include <cstdlib>
#include <forward_list>
#include <set>
#include <iostream>
#include <fstream>
#include <map>
#include <list>
#include <string>
#include "trans_mgr.h"
#include "../common/char_buffer.h"
#include "../common/sio.h"

namespace sdb {
namespace enginee {
using namespace std;
using namespace sdb::common;

const string CHECKPOINT_FILE_SUFFIX = ".chk";
const string LOG_FILE_SUFFIX = ".log";
const string LOG_MGR_LOCK_FILENAME = "in_lock";

const int COMMIT_DEFINE = 0xffffffff;
const int ABORT_DEFINE = 0xfffffffe;
const int START_DEFINE = 0xfffffffd;

const int LOG_FILE_MAGIC = 0x7FA9C83D;
const int LOG_BLOCK_MAGIC = 0xF79A8CD3;

const int NOT_FIND_TRANSACTION = -1;
const int FIND_TRANSACTION_START = 1;
const int CONTINUE_TO_FIND = 2;

const int LOG_FILE_HEADER_LENGTH = 1024;
const int LOG_BLOCK_HEADER_LENGTH = 24;
const int DIRCTORY_ENTRY_LENGTH = 18;
const int CHECK_POINT_LENGTH = 16;

const int LOG_BLK_SPACE_NOT_ENOUGH = -0X500;
const int OUTOF_ENTRY_INDEX = -0X501;
const int DATA_LENGTH_TOO_LARGE = -0X502;
const int INIT_LOG_FILE_FAILURE = -0x503;
const int LOG_STREAM_ERROR = 0X504;
const int LOCKED_LOG_MGR_PATH = -0X505;
const int LOCK_STREAM_ERROR = -0X506;
const int OUT_LOCK_LOGMGR_FAILURE = -0X507;
const int LOG_FILE_IS_ACTIVE = -0X508;
const int INVALID_OPS_ON_ACTIVE_LOG_FILE = -0X509;
const int MISSING_LAST_CHECK_POINT = -0X50A;

class log_mgr;

enum log_status {
	initialized = 0, active, opened, closed
};

enum log_sync_police {
	immeidate, buffered, async
};

enum dir_entry_type {
	unknown = -1, start_item, data_item, commit_item, abort_item
};

struct check_point {
	timestamp ts;
	int log_blk_off; // log block offset
	int dir_ent_off; // log directory entry offset

	bool operator==(const check_point & an);
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

class check_thread {

};

class log_block {

	friend class log_file;

	struct header {
		unsigned int magic = 0xF79A8CD3;
		unsigned int offset = 0;
		unsigned int pre_blk_off = 0;
		unsigned int next_blk_off = 0;
		unsigned int writing_entry_off = 0;
		unsigned int writing_data_off = 0;

		void write_to(char_buffer & buff);
		void read_from(char_buffer & buff);

	};

private:
	bool ref_flag = false;
	bool ass_flag = false; //assign buffer flag
	header _header;
	char *buffer;
	int length;
	int read_entry_off = 0;

public:

	struct dir_entry {
		timestamp ts;
		unsigned short seq;
		int offset;
		unsigned int length = 0;

		void write_to(char_buffer & buff);
		void read_from(char_buffer & buff);

		dir_entry_type get_type();
		void as_start();
		void as_commit();
		void as_abort();
	};

	log_block(int block_size = 4096);
	log_block(char *buff, int block_size = 4096);
	log_block(const log_block & another);
	log_block & operator=(const log_block & antoher);
	bool operator==(const log_block & another) = delete;
	~log_block();

	int add_start(timestamp ts);
	int add_action(timestamp ts, action & a);
	int add_commit(timestamp ts);
	int add_abort(timestamp ts);

	void head();
	void tail();
	void seek(int dir_ent_off);
	bool has_next();
	bool has_pre();
	void pre_entry(dir_entry & e);
	void next_entry(dir_entry & e);
	dir_entry get_entry(int idx);

	int copy_data(int idx, char_buffer & buff);
	void copy_data(const dir_entry & e, char_buffer & buff);
	void copy_data(const dir_entry &e, action & a);
	ulong get_seg_id(const dir_entry & e);

	int count_entry();
	int remain();
	void reset();

	void set_block_size(const int block_size);
	void assign_block_buffer();
	void ref_buffer(char * buff, int len);
	void write_to(char * buff);
};

class log_file {
	friend class log_mgr;

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
	bool opened = false;
	string pathname;
	std::fstream log_stream;
	header _header;
	check_point * cutoff_point = nullptr;
	int read_blk_offset = LOG_FILE_HEADER_LENGTH; //includes the log file header length
	log_block last_blk;  // the last log block for append log entry
	char * write_buffer = nullptr;
	log_mgr * _log_mgr = nullptr;

	forward_list<check_point> check_list;
	set<ulong> check_segs;

	int init_log_file();
	int renewal_last_block();
	int flush_last_block();

	/*
	 * append buffer that contains several log blocks. must flush the last log block
	 * before append those blocks
	 */
	int append(const char * buff, int len);
	int re_open();
	void check_log_block(log_block & lb);
	int check(int blk_off = LOG_FILE_HEADER_LENGTH, int dir_ent_off = 0);
	int cut_off();

	/*
	 * from the active log file end-place, find actions for a transaction
	 * specified with ts parameter, and fill them to actions parameter,
	 * if found all actions for the transaction,
	 * return FIND_TRANSACTION_START, NOT_FIND_TRANSACTION or CONTINUE_TO_FIND
	 */
	int rfind(timestamp ts, list<action> &actions);

	/*
	 * from the inactive log file end-place, find actions for a transaction
	 * specified with ts parameter, and fill them to actions parameter,
	 * if found all actions for the transaction,
	 * return FIND_TRANSACTION_START, NOT_FIND_TRANSACTION or CONTINUE_TO_FIND
	 * return INVALID_OPS_ON_ACTIVE_LOG_FILE if invoke the method at the active log file
	 */
	int irfind(timestamp ts, list<action>& actinos);

public:
	log_file();
	log_file(const string& fn);
	log_file(const log_file & another) = delete;
	log_file(const log_file && another) = delete;
	~log_file();

	void set_log_mgr(log_mgr * lm);
	int open();
	int open(const string &pathname);
	int close();
	bool is_open();

	int append_start(timestamp ts);
	int append(timestamp ts, action& a);
	int append_commit(timestamp ts);
	int append_abort(timestamp ts);

	int head();
	int seek(int blk_off);
	int next_block(char * buff);
	int tail();
	int pre_block(char * buff);

	bool is_active();
	int inactive();

	void set_block_size(int bs);
	int get_block_size();
};

class checkpoint_file {
	friend class log_mgr;
private:
	string pathname;
	fstream chk_stream;
public:
	int open(const string & pathname);
	int write_checkpoint(const check_point * cp);
	int write_checkpoint(const check_point & cp);
	int write_checkpoint(const timestamp &ts, const int & lbo, const int & deo);
	int last_checkpoint(check_point &cp);
	int last_checkpoint(check_point *cp);
	void close();
};

/*
 * current the log management just support sync_immediate policy.
 */
class log_mgr {
	friend class log_file;
	friend class checkpoint_file;

private:
	bool opened = false;
	log_sync_police sync_police = log_sync_police::immeidate;
	string path;  // log directory that contains log files
	string lock_pathname;
	undo_buffer ub;
	char * log_buffer;
	int lb_cap = 1048576; // log buffer capacity
	int log_file_max_size = 67108864;

	log_file curr_log_file;
	log_file last_log_file;

	long check_interval; // check interval in milliseconds

	ulong log_file_seq = 0;
	ulong chk_file_seq = 0;

	int in_lock();
	int out_lock();

public:
	int load();
	int load(const string & path);
	bool is_open();
	int close();
	int flush();

	int log_start(timestamp ts);
	int log_action(timestamp ts, action & a);
	int log_commit(timestamp ts);
	int log_abort(timestamp ts);

	/*
	 * find actions for a transaction in reserved order in case an transaction
	 * need to be roll back
	 */
	int rfind(timestamp ts, list<action> & actions);

	/*
	 * redo and undo action depends seg_mgr is loaded
	 */
	int redo();
	int undo();
	int check();

	void set_log_file_max_size(int size = 67108864);
	void set_sync_police(const enum log_sync_police & sp);
	const log_sync_police & get_sync_police() const;

	log_mgr();
	log_mgr(const string & path);
	log_mgr(const log_mgr & antoher) = delete;
	log_mgr(const log_mgr && another) = delete;
	virtual ~log_mgr();
};

} /* namespace enginee */
} /* namespace sdb */

#endif /* LOG_MGR_H_ */
