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
			if (ts > a.di->wts) {
				if (write_log(a.di)) { // write log
					write(a.di);  // write data
				} else {
					rollback();
					return;
				}
			} else {
				// skip current update
			}
		} else if (a.op == action_op::READ) {
			if (ts > a.di->wts) {
				if (read(a.di) != sdb::SUCCESS) {
					rollback();
					return;
				}
			} else {
				rollback();
				return;
			}
		}
	}
	tst = PARTIALLY_COMMITTED;

	if (auto_commit) {
		commit();
	}
}

void transaction::commit() {

}

void transaction::rollback() {

}

int transaction::write_log(data_item_ref *pdi) {
	int r = sdb::SUCCESS;

	return r;
}

int transaction::restore() {
	int r = sdb::SUCCESS;

	return r;
}

int transaction::write(data_item_ref * di) {
	int r = sdb::SUCCESS;

	return r;
}

int transaction::read(data_item_ref * di) {
	int r = sdb::SUCCESS;

	//TODO read data here maybe acquire a lock

//	ts_mutex.lock();
	if (ts > di->rts) {
		di->rts = ts;
	}
//	ts_mutex.unlock();

	return r;
}

void transaction::add_action(const action & a) {
	actions.push_back(a);
}

void transaction::add_action(action_op op, data_item_ref * di) {
	action a;
	a.op = op;
	a.di = di;
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
