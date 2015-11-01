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

action::~action() {
	if (!ref_flag && buff) {
		delete[] buff;
	}
}

action & action::operator=(const action & an) {
	if (&an != this) {
		dif = an.dif;
		seq = an.seq;
		op = an.op;
		len = an.len;
		ref_flag = an.ref_flag;
		if (!ref_flag && buff) {
			buff = new char[len];
			memcpy(buff, an.buff, len);
		} else {
			buff = an.buff;
		}
	}
	return *this;
}

action::action(const action & an) {
	seq = an.seq;
	op = an.op;
	dif = an.dif;
	len = an.len;
	ref_flag = an.ref_flag;

	if (!ref_flag && buff) {
		buff = new char[len];
		memcpy(buff, an.buff, len);
	} else {
		buff = an.buff;
	}
}

void transaction::begin() {
	tm->assign_trans(this);
}

void transaction::execute() {
	if (ts == 0) {
		tm->assign_trans(this);
	}

	if(lm->log_start(ts)<0) {
		tst = FAILED;
		return ;
	}

	tst = ACTIVE;
	for (auto & a : actions) {
		if (a.op == action_op::WRITE || a.op == action_op::UPDATE) {
			if (ts > a.dif->wts) {
				if (a.dif->cmt_flag == trans_leave) {
					a.dif->mtx.lock();
					int r = log_action(a);
					if (r >= 0) {
						r = write(a);
					}
					a.dif->mtx.unlock();
					if (r < 0) {
						abort();
						return;
					} else {
						op_step++;
					}
				} else {
					// the data_item was write by another transaction, but not commit
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

int transaction::log_action(const action & a) {
	int r = sdb::SUCCESS;
	if (r >= 0) {
		log_block::log_entry le;
		le.di = a.dif;
		le.seq = a.seq;
		if (a.op == UPDATE) {
			char_buffer obuff;
			int r = sm->get_row(a.dif, obuff);
			le.update(a.buff, a.len, obuff.data(), obuff.capacity());
		} else {
			le.create(a.buff, a.len);
		}
		r = lm->log_entry(ts, le);
	}

	return r;
}
void transaction::commit() {
	mtx.lock();
	cva.wait(mtx, [this]() {return deps.empty();});
	mtx.unlock();

	if (tst == PARTIALLY_COMMITTED) {
		if (lm->log_commit(ts) >= 0) {
			for (auto it = actions.begin(); it != actions.end(); it++) {
				if (it->op >= action_op::WRITE) {
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
	if (r == sdb::SUCCESS) {
		a.len = buff.capacity();
		a.buff = new char[a.len];
		memcpy(a.buff, buff.data(), a.len);
	}
	a.dif->mtx.unlock();
	return r;
}

int transaction::write(action & a) {
	int r = sdb::FAILURE;

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

	r = sm->write(a.dif, a.buff, a.len);
	if (r >= 0) {
		a.dif->wts = ts;
	}
	return r;
}

void transaction::restore() {

	// action is in ascend order. move to the last executed action
	auto ait = actions.rbegin();
	int s = actions.size();
	while (s > op_step && ait != actions.rend()) {
		ait++;
		s--;
	}

	list<log_block::log_entry> entries;  // entry is in descend order
	if (lm->rfind(ts, entries) == FIND_TRANSACTION_START) {
		// maybe log_entry successfully but write action failure, align action and log entry
		//while (ait->seq < eit->seq && eit != entries.end()) {
		//	eit++;
		//}
		//align action and log entry
		//while (ait->seq < eit->seq && eit != entries.begin()) {
		//	eit++;
		//}

		for (auto eit = entries.begin();
				ait != actions.rend() && eit != entries.end(); ait++) {
			if (ait->op >= action_op::UPDATE) {
				ait->dif->mtx.lock();
				if (ts >= ait->dif->wts) {
					if (ait->seq == eit->seq) {

						char * obuff;
						int len = eit->ref_oitem(obuff); // get old value from log entry;
						sm->write(ait->dif, obuff, len);
						ait->dif->cmt_flag = trans_leave;

						//TODO support logic roll back

						eit++;
					}
				}
				ait->dif->mtx.unlock();
			}
		}
	} else {
		// cannot find the log, ignore
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

void trans_mgr::submit(transaction * t, bool assign_ts) {
	if (assign_ts) {
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
