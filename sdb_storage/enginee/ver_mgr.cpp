/*
 * ver_mgr.cpp
 *
 *  Created on: May 5, 2016
 *      Author: linkqian
 */

#include "ver_mgr.h"

namespace sdb {
namespace enginee {

ver_mgr::ver_mgr(const ulong & max) {
	this->max_ver_data_size = max;
}

/**
 * ver_item list is order descent by rts
 */
int ver_mgr::add_ver(row_item * ri, ver_item * vi) {
	int r = sdb::FAILURE;
	ver_mtx.lock();
	int len = vi->len;
	if (cv_data_full.wait_for(ver_mtx, lock_timeout,
			[this, len]() {return is_hold_for(len);})) {
		auto it = ver_data.find(ri);
		if (it != ver_data.end()) {
			list<ver_item *> * vil = it->second;
			auto vit = vil->begin();
			for (; vit != vil->end(); ++vit) {
				ver_item * v = *vit;
				if (vi->ts > v->ts) {
					break;
				} else if (v->ts == vi->ts) {
					r = VER_ITEM_EXISTED;
				}
				// vi.ts < v.ts, continue to move the iterator point
				//else {
				//}
			}

			vil->insert(vit, vi);
			ver_data_size += vi->len;
			r = VER_WRITE_SUCCESS;
		} else {
			// row_item not existed in ver_data, create a ver_list for the row_item
			// and push the new version
			list<ver_item *> * vil = new list<ver_item *>;
			vil->push_back(vi);
			auto ret = ver_data.insert(make_pair(ri, vil));
			if (ret.second) {
				ver_data_size += vi->len;
			}
			r = ret.second ? VER_WRITE_SUCCESS : VER_WRITE_FAILURE;
		}
		cv_data_full.notify_one();

	} else {
		r = LOCK_TIMEOUT;
	}
	ver_mtx.unlock();
	return r;

}

int ver_mgr::del_ver(row_item * ri, const ulong &ts) {
	int r = VER_ITEM_DELETE_SUCCESS;
	ver_mtx.lock();
	auto it = ver_data.find(ri);
	if (it != ver_data.end()) {
		list<ver_item *> * vil = it->second;
		bool found = false;
		auto vit = vil->begin();
		for (; !found && vit != vil->end(); ++vit) {
			ver_item * v = *vit;
			found = (v->rts == ts);
		}
		if (found) {
			vil->erase(vit);
			if (gc_empty_row_item_immediately && vil->empty()) {
				delete vil;
				ver_data.erase(ri);
				cv_data_full.notify_one();
			}
		} else {
			r = VER_NOT_EXISTED;
		}
	} else {
		r = ROW_ITEM_NOT_EXISTED;
	}
	ver_mtx.unlock();
	return r;
}

int ver_mgr::del_ver_for_trans(const trans* tr) {
	auto vit = tr->actions_ptr->begin();
	for (; vit != tr->actions_ptr->end(); vit++) {
		action_ptr a = *vit;
		if (a->op == action_op::WRITE) {
			for (auto ril = a->row_items_ptr->begin();
					ril != a->row_items_ptr->end(); ril++) {
				row_item * ri = (*ril);
				this->del_ver(ri, tr->tid);
			}
		}
	}
}

int ver_mgr::read_ver(row_item *ri, const timestamp & ts, ver_item * vi,
		isolation l) {
	int r = sdb::FAILURE;
	ver_mtx.lock();
	auto it = ver_data.find(ri);
	if (it != ver_data.end()) {
		list<ver_item *> * vil = it->second;
		auto it = vil->begin();
		for (; it != vil->end(); ++it) {
			ver_item * v = *it;

			// read the first ver_item'ts less then the ts
			if (isolation::READ_UNCOMMITTED == l) {
				if (v->ts < ts) {
					vi->rts = ts;
					vi->ts = ts;
					vi->wts = v->wts;
					if (v->ref_row_item) {
						vi->ref_row_item = v->ref_row_item;
						vi->p_row_item = v->p_row_item;
					} else {
						vi->len = v->len;
						vi->buff = v->buff;
					}
					break;
				}
			}
			// read the first ver_item'ts less then the ts and ver_item'cmt is true
			else {
				if (v->ts < ts && v->cmt) {
					vi->rts = ts;
					vi->ts = ts;
					vi->wts = v->wts;
					if (v->ref_row_item) {
						vi->ref_row_item = v->ref_row_item;
						vi->p_row_item = v->p_row_item;
					} else {
						vi->len = v->len;
						vi->buff = v->buff;
					}
					break;
				}
			}
		}

		it = vil->erase(it);
		vil->insert(it, vi);
		r = VER_READ_SUCCESS;
	} else {  //row item must be committed status when found it in segment
		char_buffer buff;
		if (_seg_mgr->get_row(ri, buff) == sdb::SUCCESS) {
			vi = new ver_item;
			vi->ts = ts;
			vi->rts = ts;
			vi->ref_row_item = true;
			vi->cmt = true;
			vi->p_row_item = ri;
			vi->len = buff.size();
			r = VER_READ_SUCCESS;
		}
	}
	ver_mtx.unlock();
	return r;
}

int ver_mgr::write_ver(ver_item * nvi) {
	int r = sdb::FAILURE;
	ver_mtx.lock();
	auto it = ver_data.find(nvi->p_row_item);
	if (it != ver_data.end()) {
		// check the ts whether exists in ver_item_list
		auto vil = it->second;
		bool found = false;
		auto vit = vil->begin();
		for (; !found && vit != vil->end(); ++vit) {
			ver_item * vi = *vit;
			found = (vi->ts == nvi->ts);
			if (found) {
				r = VER_ITEM_EXISTED;
			} else if (vi->wts < nvi->ts && vi->rts > nvi->ts) {
				r = VER_ITEM_ABORTED;
			}
			else {
				//insert ver_item to head of the list
				r = add_ver(nvi->p_row_item, nvi);
			}
		}
	} else {
		r = add_ver(nvi->p_row_item, nvi);
	}
	ver_mtx.unlock();
	return r;
}

/*
 * scan all row_item's ver_data, remove the elder version and save to segment
 */
int ver_mgr::gc(const ulong &ts) {
	int r = sdb::SUCCESS;
	ver_mtx.lock();
	for (auto it = ver_data.begin(); it != ver_data.end(); ++it) {
		list<ver_item *> * vil = it->second;
		row_item * ri = it->first;
		bool elder = false;
		auto vit = vil->begin();

		// move to the ver_item element just elder than the timestamp specified with ts parameter
		for (; !elder && vit != vil->end(); ++vit) {
			elder = (*vit)->rts > ts;
		}

		if (elder) {
			// there is only one ver_item elder than timestamp specified
			// merge the ver_item to segment, free ver_item_list, remove row_item from ver_data
			// else free the memory of elder and remove them
			if (vil->size() == 1) {
				ver_item * vi = *vil->begin();
				_seg_mgr->write(ri, vi->buff, vi->len);
				vi->free();
				delete it->second;
				ver_data.erase(it);
			} else {
				for (; vit != vil->end(); vit++) {
					(*vit)->free();
				}
				vil->remove_if([ts] (const ver_item * e) {return e->rts < ts;});
			}
		}
	}
	ver_mtx.unlock();
	return r;
}

int ver_mgr::set_max_ver_data_size(const ulong & max) {
	int r = sdb::SUCCESS;
	if (max >= max_ver_data_size) {
		max_ver_data_size = max;
	} else if (max < max_ver_data_size) {
		r = sdb::FAILURE;
	}
	return r;
}
void ver_mgr::set_gc_empty_row_item_immediately(const bool & flag) {
	this->gc_empty_row_item_immediately = flag;
}

const bool & ver_mgr::is_set_gc_empty_row_item_immediately() {
	return this->gc_empty_row_item_immediately;
}

void ver_mgr::set_seg_mgr(seg_mgr * sm) {
	this->_seg_mgr = sm;
}

ver_mgr::~ver_mgr() {
	for (auto it = ver_data.begin(); it != ver_data.end(); ++it) {
		delete it->second;
	}
}

} /* namespace enginee */
} /* namespace sdb */
