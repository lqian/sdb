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
	tmp << dif->seg_id << dif->blk_off << dif->row_idx << flag;
	tmp.push_back(buff, len, true);
}

void action::update(char * n_buff, int n_len, char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + INT_CHARS + n_len + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	flag |= (1 << OLD_VALUE_BIT);
	tmp << dif->seg_id << dif->blk_off << dif->row_idx << flag;
	tmp.push_back(n_buff, n_len, true);
	tmp.push_back(o_buff, o_len, true);
}

void action::remove(char * o_buff, int o_len) {
	wl = ACTION_HEADER_LENGTH + INT_CHARS + o_len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	char flag = 0;
	flag |= (1 << OLD_VALUE_BIT);
	tmp << dif->seg_id << dif->blk_off << dif->row_idx << flag;
	tmp.push_back(o_buff, o_len, true);
}

void action::read_from(char_buffer & buff) {
	int len;
	buff >> dif->seg_id >> dif->blk_off >> dif->row_idx >> flag >> len;
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
	buff << dif->seg_id << dif->blk_off << dif->row_idx << flag;
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
int action::ref_nitem(char * buff) {
	if (flag >> NEW_VALUE_BIT & 1) {
		char_buffer tmp(wd, wl, true);
		tmp.skip(ACTION_HEADER_LENGTH);
		int len = 0;
		tmp >> len;
		buff = tmp.curr();
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

int action::ref_oitem(char * buff) {
	if (flag >> OLD_VALUE_BIT & 1) {
		char_buffer tmp(wd, wl, true);
		tmp.skip(ACTION_HEADER_LENGTH);
		int len = 0;
		if (flag >> NEW_VALUE_BIT & 1) {
			tmp >> len;
			tmp.skip(len);
		}
		tmp >> len;
		buff = tmp.curr();
		return len;
	} else {
		return sdb::FAILURE;
	}
}

action::~action() {
	if (wd) {
		delete[] wd;
	}

	if (rd) {
		delete[] rd;
	}

	if (assign_dif && dif) {
		delete dif;
		assign_dif = false;
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

	if (an.rl > 0) {
		rl = an.rl;
		rd = new char[rl];
		memcpy(rd, an.rd, rl);
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

	if (an.rl > 0) {
		rl = an.rl;
		rd = new char[rl];
		memcpy(rd, an.rd, rl);
	}
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
				if (ts > a.dif->wts && a.dif->cmt_flag == trans_leave) {
					if (lm->log_action(ts, a) >= 0) {
						if (write(a) != sdb::SUCCESS) {
							abort();
						} else {

						}
					} else {
						//can not log the action, abort current transaction
						abort();
						return;
					}
				} else {
					// a later transaction already write the data item. skip current write
				}
			} else if (a.op == action_op::READ) {
				if (ts > a.dif->wts && a.dif->cmt_flag == trans_leave) {
					if (read(a) != sdb::SUCCESS) {
						abort();
					}
				} else {
					abort();
					return;
				}
			}
			op_step++;
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
	if (deps.empty()) {
		// waiting until deps is empty
	}
	if (tst == PARTIALLY_COMMITTED) {
		if (deps.empty() && lm->log_commit(ts) >= 0) {
			for (auto & a : actions) {
				if (a.op == action_op::WRITE) {
					a.dif->cmt_flag == commit_flag::trans_leave;
				}

				auto it = tm->adit.find(a.dif);
				if (it != tm->adit.end()) {
					it->second.erase(this);
					if (it->second.size() == 0) {
						tm->adit.erase(a.dif);
					}
				}
			}
			tst = COMMITTED;

			//info other transaction reduce deps
			for (auto & wf : wait_fors) {
				wf->deps.remove(this);
			}
		} else {
			restore();
			tst = FAILED;
		}
	}
	tm->att.erase(ts);
}

void transaction::abort() {
	restore();
	if (lm->log_abort(ts)) {
		tst = ABORTED;

		for (auto &wf : wait_fors) {
			wf->abort();
		}
	} else {
		tst = FAILED;
	}
	tm->att.erase(ts);
}

int transaction::read(action & a) {
	int r = sdb::FAILURE;
	a.dif->mtx.lock();
	char_buffer buff;
	r = sm->get_row(a.dif->seg_id, a.dif->blk_off, a.dif->row_idx, buff);
	a.dif->rts = ts;
	a.dif->mtx.unlock();
	if (r == sdb::SUCCESS) {
		a.rl = buff.capacity();
		a.rd = new char[a.rl];
		memcpy(a.rd, buff.data(), a.rl);
	}
	return r;
}

int transaction::write(action & a) {
	int r = sdb::FAILURE;
	char * nbuff;
	int len = a.ref_nitem(nbuff);
	a.dif->mtx.lock();
	a.dif->cmt_flag = commit_flag::trans_on;

	auto it = tm->adit.find(a.dif);
	if (it != tm->adit.end()) {
		for (auto & e : it->second) {
			deps.insert_after(deps.before_begin(), e);
			e->wait_fors.insert_after(e->wait_fors.before_begin(), this);
		}
		it->second.insert(this);
	} else {
		set<p_trans> pset;
		pset.insert(this);
		tm->adit.insert(std::make_pair(a.dif, pset));
	}

	r = sm->write(a.dif->seg_id, a.dif->blk_off, a.dif->row_idx, nbuff, len);
	if (r == sdb::SUCCESS) {
		a.dif->wts = ts;
	}
	a.dif->mtx.unlock();
	return r;
}

void transaction::restore() {
	auto it = actions.rbegin();
	int s = actions.size();
	while (s > op_step && it != actions.rend()) {
		it++;
		s--;
	}

	for (int i = 0; i < op_step; i++, it++) {
		if (it->op == action_op::WRITE) {
			if (ts > it->dif->wts) {
				char * nbuff;
				int len = it->ref_oitem(nbuff);
				it->dif->mtx.lock();
				sm->write(it->dif->seg_id, it->dif->blk_off, it->dif->row_idx,
						nbuff, len);
				it->dif->mtx.unlock();
			}
		}
	}
}

void transaction::add_action(const action & a) {
	actions.push_back(a);
}

void transaction::add_action(action_op op, data_item_ref * di) {
	action a;
	a.op = op;
	a.dif = di;
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
