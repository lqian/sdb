/*
 * trans_mgr.cpp
 *
 *  Created on: Sep 9, 2015
 *      Author: lqian
 */

#include "trans_mgr.h"

namespace sdb {
namespace enginee {

bool row_item::operator==(const row_item & an) {
	return this == &an
			|| (an.seg_id == seg_id && an.blk_off == blk_off
					&& an.row_idx == row_idx);
}

bool data_item_ref::operator ==(const data_item_ref & an) {
	return an.seg_id == seg_id && an.blk_off == blk_off && an.row_idx == row_idx;
}

void action::create(char * buff, int len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	tmp << dif.seg_id << dif.blk_off << dif.row_idx << flag;
	tmp.push_back(buff, len, true);
}

void action::update(char * n_buff, int n_len, char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + INT_CHARS + n_len + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	flag |= (1 << OLD_VALUE_BIT);
	tmp << dif.seg_id << dif.blk_off << dif.row_idx << flag;
	tmp.push_back(n_buff, n_len, true);
	tmp.push_back(o_buff, o_len, true);
}

void action::remove(char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	char flag = 0;
	flag |= (1 << OLD_VALUE_BIT);
	tmp << dif.seg_id << dif.blk_off << dif.row_idx << flag;
	tmp.push_back(o_buff, o_len, true);
}

void action::read_from(char_buffer & buff) {
	int len;
	buff >> dif.seg_id >> dif.blk_off >> dif.row_idx >> flag >> len;
	buff.skip(len);
	if ((flag >> NEW_VALUE_BIT & 1) && (flag >> OLD_VALUE_BIT & 1)) {
		buff >> len;
		buff.skip(len);
	}

	wl = buff.header();
	wd = new char[wl];
	memcpy(wd, buff.data(), wl);
}

void action::write_to(char_buffer & buff) {
	buff << dif.seg_id << dif.blk_off << dif.row_idx << flag;
	buff.push_back(wd, wl, false);
}

int action::copy_nitem(char *buff) {
	if (flag >> NEW_VALUE_BIT & 1) {
		char_buffer tmp(wd, wl, true);
		tmp.skip(ACTION_HEADER_LENGTH);
		int len = 0;
		tmp >> len;
		buff = new char[len];
		memcpy(buff, tmp.curr(), len);
		return len;
	} else {
		return sdb::FAILURE;
	}
}

int action::copy_oitem(char *buff) {
	if (flag >> OLD_VALUE_BIT & 1) {
		char_buffer tmp(wd, wl, true);
		tmp.skip(ACTION_HEADER_LENGTH);
		int len = 0;
		if (flag >> NEW_VALUE_BIT & 1) {
			tmp >> len;
			tmp.skip(len);
		}
		tmp >> len;
		memcpy(buff, tmp.curr(), len);
		return len;
	} else {
		return sdb::FAILURE;
	}
}

action::~action() {
	if (wd) {
		delete[] wd;
	}
}

action & action::operator=(const action & an) {
	seq = an.seq;
	op = an.op;
	dif = an.dif;
	if (an.wl > 0) {
		wl = an.wl;
		wd = new char[wl];
		memcpy(wd, an.wd, wl);
	}
	return *this;
}

action::action(const action & an) {
	seq = an.seq;
	op = an.op;
	dif = an.dif;
	if (an.wl > 0) {
		wl = an.wl;
		wd = new char[wl];
		memcpy(wd, an.wd, wl);
	}
	memcpy(wd, an.wd, wl);
}

void transaction::begin() {
	tm->assign_trans(this);
}

void transaction::execute() {
	tst = ACTIVE;
	auto ret = tm->att.insert(std::make_pair(ts, this));
	if (ret.second) {
		for (auto & a : actions) {
			if (a.op == action_op::WRITE) {
				if (ts > a.dif.wts) {
					if (lm->log_action(ts, a) < 0) { // write log
						abort();
						return;
					}
				} else {
					// skip current write
				}
			} else if (a.op == action_op::READ) {
				if (ts > a.dif.wts) {
					if (read(&a.dif) != sdb::SUCCESS) {
						abort();
						return;
					}
				} else {
					abort();
					return;
				}
			}
		}

		tst = PARTIALLY_COMMITTED;
		if (auto_commit) {
			commit();
		}
	} else {
		tst = PRE_ASSIGN;
	}
}

void transaction::commit() {
	if (tst == PARTIALLY_COMMITTED) {
		int r = deffer_write_data();
		if (r) {
			if (lm->log_commit(ts)) {
				tst = COMMITTED;
			} else {
				tst = FAILED;
			}
		} else {
			// restore old value from action
		}
	}
	tm->att.erase(ts);
}

void transaction::abort() {
	if (lm->log_abort(ts)) {
		tst = ABORTED;
	} else {
		tst = FAILED;
	}
	tm->att.erase(ts);
}

/*
 * return SUCCESS if all data write successfully.
 * else index of actions that write successfully, in this case write data
 * is partially success.
 */
int transaction::deffer_write_data() {
	for (auto & a : actions) {
		if (true) { // table.update or seg_mgr.update(dif, char * buff, int len);
			write_idx++;
		} else {
			return sdb::FAILURE;
		}
	}
	return sdb::SUCCESS;;
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
	a.dif = (*di);
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
	time(&timer);
}

trans_mgr::~trans_mgr() {
}

} /* namespace enginee */
} /* namespace sdb */
