/*
 * trans_mgr.cpp
 *
 *  Created on: May 7, 2016
 *      Author: lqian
 */

#include "trans_mgr.h"

namespace sdb {
namespace enginee {

int trans_mgr::open() {
	int r = sdb::SUCCESS;

	// init thread pool and gc thread
	tpp = new thread_pool<trans_task>(core_num, max_queue, lock_timeout);
	thp = &sdb::enginee::TS_CHRONO;
	thp->renew_ts();
	gc_thread = new thread(&trans_mgr::gc_version, this);

	// initial ver_mgr and log_mgr
	vmp = &sdb::enginee::LOCAL_VER_MGR;
	lmp = &sdb::enginee::LOCAL_LOG_MGR;
	r = lmp->load(log_paths);
	if (lmp->is_open()) {
		status = trans_mgr_status::OPENED;
	}

	return r;
}

/*
 * assign the transaction a new timestamp and submit to thread pool
 */
int trans_mgr::submit_trans(trans * tr) {
	int r = sdb::SUCCESS;
	if (tr->tid == NON_TRANSACTINAL_TIMESTAMP) {
		tr->tid = thp->next_ts();
		trans_task tt(this, tr);
		tpp->push_back(tt);
	} else {
		r = ALREADY_SUBMIT_TRANSACTION;
	}
	return r;
}

int trans_mgr::restart_trans(trans* tr) {
	int r = sdb::SUCCESS;
	if (tr->tid == NON_TRANSACTINAL_TIMESTAMP) {
		r = NON_STARTED_TRANSACTION;
	} else {
		if (abort_trans(tr) == sdb::SUCCESS) {
			tr->tid = thp->next_ts();
			trans_task tt(this, tr);
			tpp->push_back(tt);
		} else {
			r = ABORT_TRANSACTION_FAILED;
		}
	}
	return r;
}

int trans_mgr::abort_trans(trans *tr) {
	if (lmp->log_abort(tr->tid) != LOG_BLK_SPACE_NOT_ENOUGH) {
		tr->status = trans_status::ABORTED;
		remove_att_trans(tr->tid);
		vmp->del_ver_for_trans(tr);
	}
}

void trans_mgr::remove_att_trans(const timestamp & ts) {
	auto it = att.find(ts);
	if (it != att.end()) {
		att.erase(it);
	}
}

void trans_task::run() {
//TODO implements mvcc trans execute logic
	auto it = t->actions_ptr->iterator;

	t->actions_ptr;
}

void trans_mgr::gc_version() {
	const timestamp eldest_ts = att.begin()->first;
	vmp->gc(eldest_ts);
}

trans_mgr::trans_mgr() {

}

trans_mgr::~trans_mgr() {
	gc_thread->join();
	delete gc_thread;

	if (!tpp->is_terminated()) {
		tpp->await_terminate(false);
	}

	delete tpp;
}

} /* namespace enginee */
} /* namespace sdb */
