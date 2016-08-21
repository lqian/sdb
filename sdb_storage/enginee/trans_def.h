/*
 * trans_def.h
 *
 *  Created on: Oct 16, 2015
 *      Author: lqian
 */

#ifndef TRANS_DEF_H
#define TRANS_DEF_H

#include "../common/char_buffer.h"
#include "../sdb_def.h"
#include <mutex>
#include <list>
#include <ctime>
#include <string>

const short NEW_VALUE_BIT = 7;
const short OLD_VALUE_BIT = 6;

const int ACTION_HEADER_LENGTH = 15;

const int NON_TRANSACTINAL_TIMESTAMP(0);


typedef unsigned long timestamp;

namespace sdb {
namespace enginee {

using namespace std;
using namespace sdb::common;

enum action_op {
	READ, WRITE, UPDATE, DELETE
};

enum commit_flag {
	trans_leave, trans_on
};

/*
 * CAUTION: currently only support read_committed
 */
enum isolation {
	SERIALIZABLE, REPEAT_READ, READ_COMMITTED, READ_UNCOMMITTED
};

const std::string trans_status_msg[] = {
		"transaction is in initialized",
		"transaction is executing",
		"transaction is aborted",
		"transaction is ready aborted",
		"transaction is committed",
		"transaction is partially committed",
        "transaction is failed",
        "transaction is re-assigned"};

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
	INITIAL,
	ACTIVE,
	ABORTED,
	READY_ABORTED,
	COMMITTED,
	PARTIALLY_COMMITTED,
	FAILED,
	PRE_ASSIGN
};



struct row_item {
	ulong seg_id;
	uint blk_off;
	ushort row_idx;

	bool operator ==(const row_item & ri) {
		return this == &ri
				|| (seg_id == ri.seg_id && blk_off == ri.blk_off
						&& row_idx == ri.row_idx);
	}

	bool operator <(const row_item & ri) {
		if (this != &ri) {
			return (seg_id < ri.seg_id)
					|| (seg_id == ri.seg_id && blk_off < ri.blk_off)
					|| (seg_id == ri.seg_id && blk_off == ri.blk_off
							&& row_idx < ri.row_idx);
		} else {
			return false;
		}
	}
};

typedef row_item * row_item_ptr;

/*
 *
 */
struct ver_item {
	ulong ts; // a timestamp represent a transaction action operate on it. max(rts, wts)
	ulong rts = NON_TRANSACTINAL_TIMESTAMP;   //read timestamp
	ulong wts = NON_TRANSACTINAL_TIMESTAMP;   // write timestamp
	bool ref_row_item = false;
	bool cmt = false;   // commit flag
	int len = 0;
	char * buff;

	row_item * p_row_item;

	inline bool operator ==(const ver_item & vi) {
		return this == &vi
				|| (ts == vi.ts && rts == vi.rts && wts == vi.wts
						&& len == vi.len && vi.p_row_item == p_row_item);
	}

	inline void free() {
		if (ref_row_item) {
			delete p_row_item;
		} else if (len > 0) {
			delete[] buff;
		}
	}

	~ver_item() {

	}
};

struct row_item_ptr_comp {
	bool operator()(const row_item * a, const row_item * b) const {
		return (a->seg_id < b->seg_id)
				|| (a->seg_id == b->seg_id && a->blk_off < b->blk_off)
				|| (a->seg_id == b->seg_id && a->blk_off == b->blk_off
						&& a->row_idx < b->row_idx);
	}
};

struct row_item_comp {
	bool operator()(const row_item & a, const row_item & b) const {
		return (a.seg_id < b.seg_id)
				|| (a.seg_id == b.seg_id && a.blk_off < b.blk_off)
				|| (a.seg_id == b.seg_id && a.blk_off == b.blk_off
						&& a.row_idx < b.row_idx);
	}
};

/*
 * action in transaction represents operation on row items
 */
struct action {
	timestamp aid;
	action_op op;
	std::list<row_item_ptr> * row_items_ptr = nullptr;
	std::list<ver_item *> * ver_items_prt = nullptr;
};

typedef action * action_ptr;

struct trans {

	timestamp tid = NON_TRANSACTINAL_TIMESTAMP;

	bool ar; // auto restart

	timestamp cid;

	trans_status status;

	isolation iso;

	list<action_ptr> * actions_ptr;

	~trans();
};

class ts_chrono {
private:
	timestamp ts;
	mutex ts_mtx;
public:
	ts_chrono() {
		renew_ts();
	}

	inline int set_ts(timestamp ts) {
		int r = sdb::SUCCESS;
		ts_mtx.lock();
		if (ts > this->ts) {
			this->ts = ts;
		} else {
			r = sdb::FAILURE;
		}
		ts_mtx.unlock();
		return r;
	}

	inline timestamp next_ts() {
		ts_mtx.lock();
		ts++;
		ts_mtx.unlock();
		return ts;
	}

	inline void renew_ts() {
		long curr;
		time(&curr);
		ts = curr << 4;
	}
};

static ts_chrono TS_CHRONO;

}
}
#endif /* TRANS_DEF_H */

