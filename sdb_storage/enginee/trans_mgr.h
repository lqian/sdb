/*
 * trans_mgr.h
 *
 *  Created on: Sep 9, 2015
 *      Author: lqian
 */

#ifndef TRANS_MGR_H_
#define TRANS_MGR_H_

#include <cstdlib>
#include <list>
#include <map>
#include <mutex>
#include <atomic>
#include <set>

#include "../sdb_def.h"
#include "../common/char_buffer.h"
#include "ThreadPool.h"

namespace sdb {
namespace enginee {

using namespace common;
using namespace sdb::common;

class trans_task;
class trans_mgr;
class transaction;
class comparator;

typedef unsigned long ulong;
typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned long timestamp;
typedef transaction * p_trans;

const short NEW_VALUE_BIT = 7;
const short OLD_VALUE_BIT = 6;

const int ACTION_HEADER_LENGTH = 15;

/*
 * CAUTION: currently only support read_committed
 */
enum isolation {
	SERIALIZABLE, REPEAT_READ, READ_COMMITTED, READ_UNCOMMITTED
};

/*
 * 1) Active – the initial state; the transaction stays in this state while it is executing
 *
 * 2) Aborted – after the transaction has been rolled back and the database restored to its state prior to the start of the transaction.
 *
 * 3) Committed – after successful completion.
 *
 * 4) Partially committed – after the final statement has been executed.
 *
 * 5) Failed -- after the discovery that normal execution can no longer proceed.
 *
 */
enum trans_status {
	INITIAL, ACTIVE, ABORT, COMMITTED, PARTIALLY_COMMITTED, FAILED
};

enum action_op {
	WRITE, READ
};

struct data_item_ref {
	ulong seg_id;
	uint blk_off;
	ushort row_idx;
	timestamp wts = 0; // write timestamp
	timestamp rts = 0; // read timestamp

	bool operator==(const data_item_ref & an);
};

class action {
public:
	unsigned short seq; // maybe
	action_op op;
	data_item_ref di;  // data item ref
	char flag = 0;

	char * wd = nullptr; // action data buffer, includes data_item_ref  and flag
	int wl = 0; // action data length, include data_item_ref and flag

	/*
	 * op is write, but the action doesn't have old data, the new data specified
	 * with two parameters, the method assume that data_item_ref has been assigned
	 */
	void create(char * n_buff, int n_len);

	void update(char * n_buff, int n_len, char * o_buff, int o_len);

	void remove(char * o_buff, int o_len);

	action& operator=(const action & an);

	void read_from(char_buffer & buff);
	void write_to(char_buffer & buff);
	int copy_nitem(char * buff);
	int copy_oitem(char * buff);

	action() {
	}
	action(const action & an);
	~action();
};

class transaction {
	friend class trans_mgr;
private:
	timestamp ts = 0;
	trans_status tst = INITIAL;
	std::list<action> actions;
	std::list<p_trans> deps;  	  // the transaction depends other transactions
	std::list<p_trans> wait_fors;  // other transaction wait for the transaction

	bool auto_commit;
	trans_mgr * tm = nullptr;

	/*
	 * restore data item modification by the transaction
	 */
	int restore();
	int read(data_item_ref * pdi);
	int write(data_item_ref * pdi);
	int write_log(data_item_ref *pdi);
public:
	void execute();
	void commit();
	void rollback();

	void add_action(action_op op, data_item_ref * di);
	void add_action(const action & a);

	transaction() {
	}
	transaction(timestamp _ts) :
			ts(_ts) {
	}
	~transaction() {
	}

	inline void set_trans_mgr(trans_mgr * tm) {
		this->tm = tm;
	}

	inline bool assignable() {
		return ts == 0 || tm == nullptr;
	}

	inline bool is_auto_commit() const {
		return auto_commit;
	}

	inline void set_auto_commit(bool autoCommit) {
		auto_commit = autoCommit;
	}
};

class trans_mgr {
private:
	timestamp curr_ts;
	std::map<timestamp, p_trans> att; // active transaction table
	std::map<data_item_ref *, std::set<p_trans>> adit; // active data item table

	std::mutex mtx;
	uint ticks = 0;
	sdb::common::ThreadPool thread_pool;
public:
	void assign_trans(p_trans t);
	void submit(p_trans t);
	inline void store_ts(timestamp &ts) {
		if (ts > curr_ts) {
			curr_ts = ts;
		}
	}

	inline void next_ts() {
		ticks++;
		curr_ts &= 0xFFFFFFFF00000000;
		curr_ts |= ticks;
	}
	trans_mgr();
	virtual ~trans_mgr();
};

class trans_task: public sdb::common::Runnable {
private:
	p_trans t;

public:
	virtual void run() {
		t->execute();
	}

	trans_task(p_trans _t) :
			t(_t) {
	}
	virtual ~trans_task() {
	}
};
} /* namespace enginee */
} /* namespace sdb */

#endif /* TRANS_MGR_H_ */
