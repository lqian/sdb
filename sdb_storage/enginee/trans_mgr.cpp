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
	tpp = new thread_pool(core_num, max_queue, lock_timeout);
	thp = &sdb::enginee::TS_CHRONO;
	thp->renew_ts();

	// start gc thread
	ver_mgr = & sdb::enginee::LOCAL_VER_MGR;
	gc_thread = new thread(&trans_mgr::gc_version, this);
	status = trans_mgr_status::OPENED;

	return r;
}


int trans_mgr::submit_trans(trans * tr) {
	int r = sdb::SUCCESS;
	tr->tid = thp->next_ts();
	return r;
}

void trans_mgr::gc_version() {
	const timestamp eldest_ts = att.begin()->first;
	ver_mgr->gc(eldest_ts);
}

trans_mgr::trans_mgr() {

}

trans_mgr::~trans_mgr() {
	delete tpp;

	if (!tpp->is_terminated()) {
		tpp->await_terminate(false);
	}
}

} /* namespace enginee */
} /* namespace sdb */
