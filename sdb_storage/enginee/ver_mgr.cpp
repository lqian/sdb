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
 * ver_item list is order descent by timestamp
 */
int ver_mgr::add_ver(const row_item &  ri,  ver_item * vi) {
	ver_mtx.lock();
	if (this->ver_data_size + vi->len > max_ver_data_size) {
		return EXCEED_MAX_VER_DATA_SIZE;
	}
	auto it = ver_data.find(ri);
	if (it != ver_data.end()) {
		list<ver_item *> * vil = it->second;
		auto vit = vil->begin();
		for (; vit != vil->end(); ++vit) {
			ver_item * v = *vit;
			if (vi->ts > v->ts) {
				break;
			} else if (v->ts == vi->ts) {
				return VER_ITEM_EXISTED;
			}
			// vi.ts < v.ts, continue to move the iterator point
			//else {
			//}
		}

		vil->insert(vit, vi);
		ver_data_size += vi->len;
		ver_mtx.unlock();
		return VER_INSERT_SUCCESS;
	} else {
		// row_item not existed in ver_data, create a ver_list for the row_item
		// and push the new version
		list<ver_item *> * vil = new list<ver_item *>;
		vil->push_back(vi);
		auto ret = ver_data.insert(make_pair(ri, vil));
		if (ret.second) {
			ver_data_size += vi->len;
		}
		ver_mtx.unlock();
		return ret.second ? VER_INSERT_SUCCESS : VER_INSERT_FAILURE;
	}
}

int ver_mgr::del_ver(const row_item & ri, const ulong &ts) {
	int r = VER_ITEM_DELETE_SUCCESS;
	ver_mtx.lock();
	auto it = ver_data.find(ri);
	if (it != ver_data.end()) {
		list<ver_item *> * vil = it->second;
		bool found = false;
		auto vit = vil->begin();
		for (; !found && vit != vil->end(); ++vit) {
			ver_item * v = *vit;
			found = (v->ts == ts);
		}
		if (found) {
			vil->erase(vit);
			if (gc_empty_row_item_immediately && vil->empty()) {
				delete vil;
				ver_data.erase(ri);
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

/*
 * scan all row_item's ver_data, remove the elder version and save to segment
 */
int ver_mgr::gc(const ulong &ts) {
	int r = sdb::SUCCESS;
	ver_mtx.lock();
	for (auto it = ver_data.begin(); it != ver_data.end(); ++it) {
		list<ver_item *> * vil  = it->second;
		row_item ri = it->first;
		bool elder = false;
		auto vit = vil->begin();

		// move the ver_item element just elder than the timestamp specified with ts parameter
		for (; !elder && vit != vil->end(); ++vit) {
			elder = (*vit)->ts > ts;
		}

		if (elder) {
			// there is only one ver_item elder than timestamp specified
			// merge the ver_item to segment, free ver_item_list, remove row_item from ver_data
			// else free the memory of elder and remove them
			if (vil->size() == 1) {
				ver_item * vi = *vil->begin();
				_seg_mgr->write(&ri, vi->buff, vi->len);
				vi->free();
				delete it->second;
				ver_data.erase(it);
			} else {
				for (; vit != vil->end(); vit++) {
					(*vit)->free();
				}
				vil->remove_if([ts] (const ver_item * e) {return e->ts < ts;});
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
