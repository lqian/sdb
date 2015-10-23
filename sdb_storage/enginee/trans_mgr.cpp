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
	n_len = len;
	wl = ACTION_HEADER_LENGTH + INT_CHARS + len;
	wd = new char[wl];
	char_buffer tmp(wd, wl, true);
	flag |= (1 << NEW_VALUE_BIT);
	tmp << dif->seg_id << dif->blk_off << dif->row_idx << flag;
	tmp.push_back(buff, len, true);
}

void action::update(char * n_buff, int n_len, char * o_buff, int o_len) {
	this->n_len = n_len;
	this->o_len = o_len;
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
	this->o_len = o_len;
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
	if ((flag | 0xC0) == 0xC0) { // update 2^7 + 2^6
		n_len = len;
		buff >> o_len;
		buff.skip(o_len);
	} else if ((flag | 0x80) == 0x80) {
		n_len = len;
	} else if ((flag | 0x40) == 0x40) {
		o_len = len;
	}

	wl = buff.header();
	wd = new char[wl];
	memcpy(wd, buff.data(), wl);
}

void action::write_to(char_buffer & buff) {
	buff << dif->seg_id << dif->blk_off << dif->row_idx << flag;
	buff.push_back(wd, wl, false);
}

int action::copy_nitem(char * & buff) {
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

int action::ref_nitem(char * & buff) {
	if (flag >> NEW_VALUE_BIT & 1) {
		char_buffer tmp(wd, wl, true);
		tmp.skip(ACTION_HEADER_LENGTH);
		int len = -1;
		tmp >> len;
		buff = tmp.curr();
		return len;
	} else {
		return sdb::FAILURE;
	}
}

int action::copy_oitem(char * & buff) {
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

int action::ref_oitem(char * & buff) {
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
	assign_dif = an.assign_dif;
	flag = an.flag;
	n_len = an.n_len;
	o_len = an.o_len;
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
	assign_dif = an.assign_dif;
	flag = an.flag;
	n_len = an.n_len;
	o_len = an.o_len;
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
	if (ts == 0) {
		tm->assign_trans(this);
	}

	tst = ACTIVE;
	for (auto & a : actions) {
		if (a.op == action_op::WRITE) {
			if (ts > a.dif->wts) {
				if (a.dif->cmt_flag == trans_leave) {
					if (lm->log_action(ts, a) >= 0) {
						if (write(a) < 0) {
							abort();
							return;
						}
						op_step++;
					} else {
						//can not log the action, abort current transaction
						abort();
						return;
					}
				} else { // the data_item was write by another transaction, but not commit
					abort();
					return;
				}
			} else if (ts < a.dif->wts) {
				// a later transaction already write the data item. skip current write
				op_step++;
			}
		} else if (a.op == action_op::READ) {
			if (ts > a.dif->wts) {
				if (a.dif->cmt_flag == trans_leave) {
					if (read(a) == sdb::FAILURE) {
						abort();
						return;
					}
					op_step++;
				} else {
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
}

void transaction::commit() {
	mtx.lock();
	cva.wait(mtx, [this]() {return deps.empty();});
	mtx.unlock();

	if (tst == PARTIALLY_COMMITTED) {
		if (lm->log_commit(ts) >= 0) {
			for (auto it = actions.begin(); it != actions.end(); it++) {
				if (it->op == action_op::WRITE) {
					it->dif->cmt_flag = commit_flag::trans_leave;
				}
			}
			tm->remove_trans(this);
			tst = COMMITTED;

			// info other transaction reduce deps
			for (auto & wf : wait_fors) {
				wf->inform_commit_from(this);
			}
		} else { // when a transaction hasn't commit log, judge it as aborted status
			restore();
			tst = ABORTED;
		}
	} else if (tst == READY_ABORTED) {
		abort();
	}
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
	a.ref_nitem(nbuff);

	a.dif->mtx.lock();
	a.dif->cmt_flag = commit_flag::trans_on;

	tm->adit_mtx.lock();
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
	tm->adit_mtx.unlock();  // unlock adit_mtx

	r = sm->write(a.dif, nbuff, a.n_len);
	if (r >= 0) {
		a.dif->wts = ts;
	}
	a.dif->mtx.unlock(); // write data, unlock data_item_ref.mtx
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
			if (ts >= it->dif->wts) {
				char * nbuff;
				int len = it->ref_oitem(nbuff);
				it->dif->mtx.lock();
				sm->write(it->dif->seg_id, it->dif->blk_off, it->dif->row_idx,
						nbuff, len);
				it->dif->cmt_flag = trans_leave;
				it->dif->mtx.unlock();
			}
		}
	}

	tm->remove_trans(this);
}

void transaction::inform_commit_from(p_trans t) {
	mtx.lock();
	deps.remove(t);
	cva.notify_one();
	mtx.unlock();
}

void transaction::inform_abort() {
	mtx.lock();
	tst = READY_ABORTED;
	cva.notify_all();
	mtx.unlock();
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

void trans_mgr::open() {
	thread_pool.set_core_size(core_size);
	thread_pool.set_max_task(max_active_trans);
	thread_pool.init_thread_workers();
	time(&timer);
	ticks_mtx.lock();
	curr_ts = (timer << 24);
	ticks_mtx.unlock();
	opened = true;
}

int trans_mgr::close() {
	thread_pool.await_terminate();
	return thread_pool.is_terminated() && thread_pool.is_empty();
}

void trans_mgr::assign_trans(p_trans t) {
	ticks_mtx.lock();
	t->ts = curr_ts;
	next_ts();
	ticks_mtx.unlock();
	t->tm = this;
}

void trans_mgr::remove_trans(p_trans pts) {
	adit_mtx.lock();
	auto ait = pts->actions.begin();
	while (ait != pts->actions.end()) {
		auto it = adit.find(ait->dif);
		if (it != adit.end()) {
			it->second.erase(pts);
			if (it->second.size() == 0) {
				adit.erase(ait->dif);
			}
		}
		ait++;
	}
	adit_mtx.unlock();
}

void trans_mgr::add_trans(data_item_ref *dif, p_trans pts) {
	adit_mtx.lock();
	auto it = adit.find(dif);
	if (it != adit.end()) {
		it->second.insert(pts);
	} else {
		set<p_trans> pset;
		pset.insert(pts);
		adit.insert(std::make_pair(dif, pset));
	}
	adit_mtx.unlock();
}

void trans_mgr::submit(transaction * t, bool assign) {
	if (assign) {
		assign_trans(t);
	}
	trans_task * task = new trans_task(t);
	t->task = task;
	thread_pool.push_back(task);
}

trans_mgr::trans_mgr() {

}

trans_mgr::~trans_mgr() {
}

} /* namespace enginee */
} /* namespace sdb */
