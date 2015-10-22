/*
 * seg_mgr.h
 *
 *  Created on: Aug 5, 2015
 *      Author: lqian
 */

#ifndef SEG_MGR_H_
#define SEG_MGR_H_

#include <map>
#include <list>
#include <mutex>

#include "../storage/datafile.h"
#include "trans_def.h"

namespace sdb {
namespace enginee {
using namespace sdb::common;

const int ASSIGN_DATA_FILE_FAILURE = -0x400;
const int ASSIGN_SEGMENT_FAILURE = -0x401;
const int ASSIGN_BLOCK_FAILURE = -0x402;
const int SEGMENG_NOT_EXISTED = -0x403;

using namespace std;
using namespace sdb::storage;

typedef unsigned long int ulong;
typedef segment * pseg;

/*
 * segment management is a tool that manage locale segment in a data node.
 * it has those function:
 *
 * 1) find/assign segment for request, isolates data_file create/close
 * 2) manage segment swap in and swap out upon data_file
 */
class seg_mgr {
public:

	seg_mgr();
	virtual ~seg_mgr();

	enum swap_policy {
		ALWAYS, FIFO, LRU, LIRS, CLOCK_PRO
	};

	int load();
	void flush(data_file * pdf);
	void flush_all_segment();
	void close();
	void clean();
	void remove_files();

	void set_seg_default_length(int length = M_64);

	int assign_segment(segment * & seg);

	segment * find_segment(ulong id);

	int get_row(ulong seg_id, uint blk_off, int idx, char_buffer & buff);

	int get_row(const data_item_ref & dif, char_buffer & buff);

	int get_row(const data_item_ref * dif, char_buffer & buff);

	/*
	 * write a exist row_idx with buffer and its length
	 */
	int write(ulong seg_id, uint blk_off, int idx, const char * buff, const int& len);

	/*
	 * write a data_item with buffer and length, return success if write else false
	 */
	int write(const data_item_ref *dif, const char * buff, const int & len);

	inline void set_swap_police(swap_policy & sp) {
		this->sp = sp;
	}

	inline swap_policy get_swap_police() const {
		return sp;
	}

	inline void set_seg_id(const ulong sid) {
		local_segment_id = sid;
	}

	inline void set_data_file_id(const ulong dfid) {
		local_data_file_id = dfid;
	}

	void set_datafile_paths(const string & paths);
	void add_datafile_paths(const string & paths);

	/* if current data file is full of segment, create a new data file and store it into list */
	int assign_data_file(data_file * pdf);

	data_file * find_data_file(ulong id);

private:
	int seg_default_length = M_64;
	ulong local_segment_id = 0;
	ulong local_data_file_id = 0;
	list<string> data_file_paths;
	map<string, data_file *> path_df_map; // last data file is assigning segment in paths
	map<unsigned long, data_file *> df_map;
	map<unsigned long, pseg> seg_map;
	list<segment> seg_list;
	ulong max_segment;
	ulong max_open_file;
	swap_policy sp;

	std::mutex mtx;

	int do_load(const string & pathname);
	int do_load_seg(data_file * df);

	int assign_data_file(data_file * pdf, map<string, data_file *>::iterator it);
};

static seg_mgr LOCAL_SEG_MGR;


} /* namespace enginee */
} /* namespace sdb */

#endif /* SEG_MGR_H_ */
