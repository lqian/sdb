/*
 * trans_mgr.cpp
 *
 *  Created on: Sep 9, 2015
 *      Author: lqian
 */

#include "trans_mgr.h"

namespace sdb {
namespace enginee {

bool data_item_ref::operator ==(const data_item_ref & an) {
	return an.seg_id == seg_id && an.blk_off == blk_off && an.row_idx == row_idx;
}

void action::create(char * buff, int len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	tmp << di.seg_id << di.blk_off << di.row_idx << flag;
	tmp.push_back(buff, len, true);
}

void action::update(char * n_buff, int n_len, char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + n_len;
	wl += INT_CHARS + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	flag |= (1 << OLD_VALUE_BIT);
	tmp << di.seg_id << di.blk_off << di.row_idx << flag;
	tmp.push_back(n_buff, n_len, true);
	tmp.push_back(o_buff, o_len, true);
}

void action::remove(char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	char flag = 0;
	flag |= (1 << OLD_VALUE_BIT);
	tmp << di.seg_id << di.blk_off << di.row_idx << flag;
	tmp.push_back(o_buff, o_len, true);
}

void action::read_from(char_buffer & buff) {
	buff >> di.seg_id >> di.blk_off >> di.row_idx >> flag;
	wl = buff.remain();
	wd = new char[wl];
	buff.pop_cstr(wd, wl, false);
}

void action::write_to(char_buffer & buff) {
	buff << di.seg_id << di.blk_off << di.row_idx << flag;
	buff.push_back(wd, wl, false);
}

action::~action() {
	if (wd) {
		delete[] wd;
	}
}

action & action::operator=(const action & an) {
	seq = an.seq;
	op = an.op;
	di = an.di;
	wl = an.wl;
	wd = new char[wl];
	memcpy(wd, an.wd, wl);
	return *this;
}

action::action(const action & an) {
	seq = an.seq;
	op = an.op;
	di = an.di;
	wl = an.wl;
	wd = new char[wl];
	memcpy(wd, an.wd, wl);
}

void transaction::execute() {
	tst = ACTIVE;
	for (auto & a : actions) {
		if (a.op == action_op::WRITE) {
			if (ts > a.di.wts) {
				if (write_log(&a.di)) { // write log
					write(&a.di);  // write data
				} else {
					rollback();
					return;
				}
			} else {
				// skip current update
			}
		} else if (a.op == action_op::READ) {
			if (ts > a.di.wts) {
				if (read(&a.di) != sdb::SUCCESS) {
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
	a.di = (*di);
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
