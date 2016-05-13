/*
 * ver_mgr.h
 *
 *  Created on: May 5, 2016
 *      Author: linkqian
 */

#ifndef VER_MGR_H_
#define VER_MGR_H_

#include <map>
#include <list>
#include <mutex>

#include "trans_def.h"
#include "../sdb_def.h"
#include "seg_mgr.h"

namespace sdb {
namespace enginee {

using namespace std;

const ulong VER_MGR_DEFAULT_VER_DATA_SIZE = 0x200000L;

const int VER_INSERT_FAILURE(0);
const int VER_INSERT_SUCCESS(1);
const int VER_ITEM_EXISTED(2);
const int VER_ITEM_DELETE_SUCCESS(3);
const int ROW_ITEM_NOT_EXISTED(4);
const int VER_NOT_EXISTED(5);
const int EXCEED_MAX_VER_DATA_SIZE(6);

class ver_gc_thread;

class ver_mgr {
	friend class ver_gc;
private:
	bool gc_empty_row_item_immediately = false;
	ulong ver_data_size = 0;
	ulong max_ver_data_size;
	map<row_item, list<ver_item *> *, row_item_comp> ver_data;
	std::mutex ver_mtx;

	seg_mgr * _seg_mgr = & sdb::enginee::LOCAL_SEG_MGR;



public:
	ver_mgr(const ulong & max = VER_MGR_DEFAULT_VER_DATA_SIZE);
	virtual ~ver_mgr();

	ver_mgr(const ver_mgr & another) = delete;
	ver_mgr(const ver_mgr && another) = delete;
	ver_mgr & operator=(const ver_mgr& antoher) = delete;

	int add_ver(const row_item & ri,  ver_item * vi);

	int del_ver(const row_item & ri, const ulong & ts);

	int read_ver(const row_item * ri, const timestamp & ts, ver_item * vi);

	/*
	 * two old version data is garbage for multiple version concurrency control.
	 * ver_mgr provide gc method to collect this garbage of version data to free memory.
	 */
	int gc(const ulong & ts);

	int shrink(const ulong & new_ver_size) = delete;

	int set_max_ver_data_size(const ulong & max);

	void set_gc_empty_row_item_immediately(
			const bool & gc_empty_row_item_immediately);

	const bool & is_set_gc_empty_row_item_immediately();

	inline void set_seg_mgr(seg_mgr * sm);

};

static ver_mgr LOCAL_VER_MGR;

} /* namespace enginee */
} /* namespace sdb */

#endif /* VER_MGR_H_ */
