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
#include <forward_list>
#include <map>
#include <mutex>
#include <atomic>
#include <set>
#include <ctime>

#include "seg_mgr.h"
#include "log_mgr.h"
#include "trans_def.h"
#include "../sdb_def.h"
#include "../common/char_buffer.h"
#include "ThreadPool.h"

namespace sdb {
namespace enginee {
using namespace std;
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
typedef data_item_ref * p_dif;

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
	INITIAL, ACTIVE, ABORTED, COMMITTED, PARTIALLY_COMMITTED, FAILED, PRE_ASSIGN
};

struct row_item {
	ulong seg_id;
	uint blk_off;
	ushort row_idx;

	bool operator==(const row_item & an);
};

struct trans_row_item: row_item {
	timestamp wts = 0;
	timestamp rts = 0;
};

class trans_mgr {
	friend class transaction;
private:
	bool opened;
	time_t timer;
	timestamp curr_ts;
	int core_size = 4;
	int max_active_trans = 2000;

	/*
	 *  active data item and active transaction map
	 */
	map<data_item_ref *, std::set<p_trans>> adit;

	mutex ticks_mtx, adit_mtx;
	uint ticks = 0;
	sdb::common::ThreadPool thread_pool;

	inline void remove_trans(p_trans pts);
	inline void add_trans(data_item_ref *dif, p_trans pts);

	inline void next_ts() {
		ticks++;
		curr_ts &= 0xFFFFFFFFFF000000;
		curr_ts |= ticks;
	}

public:
	void open();
	int close();
	void assign_trans(p_trans t);
	void submit(p_trans t);

	inline void set_core_size(int cs = 4) {
		core_size = cs;
	}

	inline void set_max_active_trans(int mat) {
		max_active_trans = mat;
	}

	inline bool is_open() {
		return opened;
	}

	inline timestamp get_curr_ts() const {
		return curr_ts;
	}

	trans_mgr();
	virtual ~trans_mgr();
};

static trans_mgr LOCAL_TRANS_MGR;

class transaction {
	friend class trans_mgr;
	friend class trans_task;
private:
	timestamp ts = 0;
	trans_status tst = INITIAL;
	/*
	 * when a stable.save/update/delete/find, convert logic operation to
	 * an action object and put it to the list
	 */
	std::list<action> actions;
	std::forward_list<p_trans> deps; // the transaction depends other transactions
	std::forward_list<p_trans> wait_fors; // other transaction wait for the transaction
	int op_step = 0;

	bool auto_commit;

	trans_mgr * tm = nullptr;
	log_mgr * lm = nullptr;
	seg_mgr * sm = nullptr;

	trans_task * task = nullptr;



	int read(action & a);
	int write(action & a);
	void restore();



public:
	void begin();
	void execute();
	void commit();
	void abort();

	void add_action(action_op op, data_item_ref * di);
	void add_action(const action & a);

	transaction(bool ac = true) :
			auto_commit(ac) {
		tm = &LOCAL_TRANS_MGR;
		lm = &LOCAL_LOG_MGR;
		sm = &LOCAL_SEG_MGR;
	}
	transaction(timestamp _ts, bool ac = true) :
			ts(_ts), auto_commit(ac) {
		tm = &LOCAL_TRANS_MGR;
		lm = &LOCAL_LOG_MGR;
		sm = &LOCAL_SEG_MGR;
	}
	~transaction() {
		if (task) {
			delete task;
		}
	}

	inline void set_trans_mgr(trans_mgr * tm) {
		this->tm = tm;
	}

	inline void set_seg_mgr(seg_mgr * sm) {
		this->sm = sm;
	}

	inline void set_log_mgr(log_mgr *lm) {
		this->lm = lm;
	}

	inline bool assignable() {
		return ts == 0 || tm == nullptr || lm == nullptr || sm == nullptr;
	}

	inline bool is_auto_commit() const {
		return auto_commit;
	}

	inline void set_auto_commit(bool ac) {
		auto_commit = ac;
	}

	inline const trans_status status() const {
		return this->tst;
	}
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

	trans_task(const trans_task & an) {
		t = an.t;
	}

	trans_task & operator=(const trans_task & an) {
		t = an.t;
		return *this;
	}

	virtual ~trans_task() {
		std::cout << "destory trans_task" << std::endl;
	}
};
} /* namespace enginee */
} /* namespace sdb */

#endif /* TRANS_MGR_H_ */
