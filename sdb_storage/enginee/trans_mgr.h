/*
 * trans_mgr.h
 *
 *  Created on: May 7, 2016
 *      Author: lqian
 */

#ifndef TRANS_MGR_H_
#define TRANS_MGR_H_

#include <thread_pool.h>
#include <map>
#include <thread>

#include "trans_def.h"
#include "ver_mgr.h"

namespace sdb {
namespace enginee {

const unsigned int DEFAULT_MAX_TRANS_QUEUE(2048);
const unsigned int DEFAULT_LOCK_TIMEOUT(1000);  // 1 second
const unsigned int DEFAULT_GC_INTERVAL(1000);   // 1 second

const int TRANS_MGR_ALREADY_OPEN(-0x600);
const int ILLEGAL_CORE_NUM(-0x601);
const int ILLEGAL_GC_INTERVAL(-0x602);
const int ILLEGAL_MAX_QUEUE(-0x603);
const int ILLEGAL_LOCK_TIMEOUT(0x604);

using namespace std;
using namespace sdb::common;

typedef trans * trans_ref;

enum trans_mgr_status {
	INSTANTIAL, OPENED, CLOSING, CLOSED
};

class trans_mgr {
private:
	unsigned int core_num = 0;
	unsigned int max_queue = DEFAULT_MAX_TRANS_QUEUE;
	unsigned int lock_timeout = DEFAULT_LOCK_TIMEOUT;
	unsigned int gc_interval = DEFAULT_GC_INTERVAL;
	trans_mgr_status status = trans_mgr_status::INSTANTIAL;

	map<timestamp, trans_ref> att;  //active transaction table
	ts_chrono * thp;  // ts_chrono ptr
	thread_pool * tpp;  // thread pool ptr
	ver_mgr * ver_mgr;
	thread * gc_thread;

	int remove_trans(const timestamp & ts);
	int abort_trans(const timestamp & ts);

public:
	int open();
	int close(bool force);
	int submit_trans(const trans_ref tr);

	inline int set_core_num(const unsigned int & cn) {
		if (status > INSTANTIAL) {
			return TRANS_MGR_ALREADY_OPEN;
		} else if (cn>0) {
			core_num = cn;
			return sdb::SUCCESS;
		}
		else {
			return ILLEGAL_CORE_NUM;
		}
	}

	inline int set_max_queue(unsigned int & mq) {
		int r = sdb::SUCCESS;
		if (mq > 0) {
			this->max_queue = mq;
		} else {
			r = ILLEGAL_MAX_QUEUE;
		}
		return r;

	}

	inline int set_lock_timeout(const unsigned int & timeout) {
		int r = sdb::SUCCESS;
		if (timeout > 0) {
			this->lock_timeout = timeout;
		} else {
			r = ILLEGAL_LOCK_TIMEOUT;
		}
		return r;
	}

	inline const trans_mgr_status & get_status() {
		return status;
	}

	inline int set_gc_interval(const unsigned int & gc_interval) {

		int r = sdb::SUCCESS;
		if (gc_interval > 0) {
			this->gc_interval = gc_interval;
		} else {
			r = ILLEGAL_GC_INTERVAL;
		}
		return r;

	}

	void gc_version();

	trans_mgr();
	virtual ~trans_mgr();
};

static trans_mgr LOCAL_TRANS_MGR;




} /* namespace enginee */
} /* namespace sdb */

#endif /* TRANS_MGR_H_ */
