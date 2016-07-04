/*
 * trans_mgr.h
 *
 *  Created on: May 7, 2016
 *      Author: lqian
 */

#ifndef TRANS_MGR_H_
#define TRANS_MGR_H_

#include <map>
#include <thread>

#include "../common/thread_pool.h"
#include "trans_def.h"
#include "ver_mgr.h"
#include "log_mgr.h"

namespace sdb {
namespace enginee {

const unsigned int DEFAULT_MAX_TRANS_QUEUE(2048);
const unsigned int DEFAULT_LOCK_TIMEOUT(1000);  // 1 second
const unsigned int DEFAULT_GC_INTERVAL(1000);   // 1 second

const int TRANS_MGR_ALREADY_OPEN(-0x600);
const int ILLEGAL_CORE_NUM(-0x601);
const int ILLEGAL_GC_INTERVAL(-0x602);
const int ILLEGAL_MAX_QUEUE(-0x603);
const int ILLEGAL_LOCK_TIMEOUT(-0x604);

const int ALREADY_SUBMIT_TRANSACTION(-0x615);
const int NON_STARTED_TRANSACTION(-0x616);
const int ABORT_TRANSACTION_FAILED(-0x617);

using namespace std;
using namespace sdb::common;

typedef trans * trans_ptr;

class trans_task;

enum trans_mgr_status {
	INSTANTIAL, OPENED, CLOSING, CLOSED
};

enum trans_task_status {
	TTS_INITIALIZED, TTS_ACTIVE, TTS_RUNNING, TTS_CLOSED, TTS_EXCEPTIONAL
};

class trans_mgr {
	friend class trans_task;
private:
	unsigned int core_num = 0;
	unsigned int max_queue = DEFAULT_MAX_TRANS_QUEUE;
	unsigned int lock_timeout = DEFAULT_LOCK_TIMEOUT;
	unsigned int gc_interval = DEFAULT_GC_INTERVAL;
	trans_mgr_status status = trans_mgr_status::INSTANTIAL;

	string log_paths;

	map<timestamp, trans_ptr> att;  //active transaction table
	ts_chrono * thp;  				// ts_chrono ptr
	thread_pool<trans_task> * tpp;  // thread pool ptr
	ver_mgr * vmp;					// version manager ptr
	log_mgr * lmp;					// log mgr ptr
	thread * gc_thread;

	void remove_att_trans(const timestamp & ts);

public:
	trans_mgr();
	virtual ~trans_mgr();

	int open();
	int close(bool force);
	int submit_trans(trans_ptr tr);
	int restart_trans(trans_ptr tr);
	int abort_trans(trans * tr);
	void gc_version();

	inline int set_core_num(const unsigned int & cn) {
		if (status > INSTANTIAL) {
			return TRANS_MGR_ALREADY_OPEN;
		} else if (cn > 0) {
			core_num = cn;
			return sdb::SUCCESS;
		} else {
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

	inline int set_log_paths(const string & paths) {
		int r = sdb::SUCCESS;
		if (INSTANTIAL == status) {
			this->log_paths = paths;
		} else {
			r = TRANS_MGR_ALREADY_OPEN;
		}
		return r;
	}

};

static trans_mgr LOCAL_TRANS_MGR;

class trans_task: public sdb::common::runnable {
	friend class trans_mgr;
private:
	trans_mgr * tm;
	ver_mgr * vmp;
	trans * t = nullptr;
	trans_task_status tts = trans_task_status::TTS_INITIALIZED;
public:
	trans_task(){};
	trans_task(trans_mgr *_tm, trans* _t) :
			tm(_tm), t(_t) {
		this->vmp = _tm->vmp;
	}

	trans_task(const trans_task & another) {
		this->tm = another.tm;
		this->vmp = another.vmp;
		this->t = another.t;
	}

	virtual void run();
	virtual int status();
	virtual string msg();

};

} /* namespace enginee */
} /* namespace sdb */

#endif /* TRANS_MGR_H_ */
