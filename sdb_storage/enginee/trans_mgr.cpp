/*
 * trans_mgr.cpp
 *
 *  Created on: Sep 9, 2015
 *      Author: lqian
 */

#include "trans_mgr.h"

namespace sdb {
namespace enginee {

void transaction::execute() {
	tst = ACTIVE;
	for (auto & a : actions) {
		if (a.op == action_op::WRITE) {
			if (ts > a.pdi->wts) {
				if (write_log(a.pdi)) { // write log
					write(a.pdi);  // write data
				} else {
					ts = restore() ? ABORT : FAILED;
					return;
				}
			} else {
				// skip current update
			}
		} else if (a.op == action_op::READ) {
			if (ts > a.pdi->wts) {
				read(a.pdi);
				if (ts > a.pdi->rts) {
					a.pdi->rts = ts;
				}
			} else {
				ts = restore() ? ABORT : FAILED;
				return;
			}
		}
	}
	tst = PARTIALLY_COMMITTED;

	if (auto_commit) {
		tst = commit() ? COMMITTED : FAILED;
	}
}

void transaction::add_action(const action & a) {
	actions.push_back(a);
}

void transaction::add_action(action_op op, data_item_ref * di) {
	action a;
	a.op = op;
	a.pdi = di;
	actions.push_back(a);
}

void trans_mgr::assign_trans(p_trans t) {
	mtx.lock();
	next_ts();
	t->ts = curr_ts;
	mtx.unlock();
	t->tm = this;
}

void trans_mgr::submit(transaction * t) {
	assign_trans(t);
	trans_task task(t);
	thread_pool.push_back(task);
}

trans_mgr::trans_mgr() {

}

trans_mgr::~trans_mgr() {
}

} /* namespace enginee */
} /* namespace sdb */
