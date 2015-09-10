/*
 * table.h
 *
 *  Created on: Aug 26, 2015
 *      Author: lqian
 */

#ifndef TABLE_H_
#define TABLE_H_

#include "seg_mgr.h"
#include "table_desc.h"

namespace sdb {
namespace enginee {
using namespace common;

/*
 * a table in DB term was create, SDB append table and column meta to system segment, and assign a new data segment
 * for the new table.
 */
class table {
private:
	seg_mgr * segmgr;
	table_desc * tdesc = nullptr;
	segment * first_seg = nullptr;
	segment * last_seg = nullptr;
	mem_data_block * last_blk = nullptr;

	mem_data_block curr_blk;
	segment * curr_seg;
	unsigned int curr_off = 0;
	int curr_row_idx = 0;

	bool has_next_block();
	void next_block(mem_data_block *blk);
	bool has_next_seg();
	segment * next_seg();
	segment * pre_seg();
	bool has_pre_seg();
	bool has_pre_block();
	void pre_block(mem_data_block * blk);

public:

	int open();
	int close();
	int flush();
	int head();  // more to the head of table rows
	int tail();  // move to the tail of table rows

	/* low storage api for add_row, get_row, delete_row, update_row */
	int add_row(row_store & rs);

	int get_row(unsigned short off, char_buffer & buff);
	int get_row(unsigned int blk_off, unsigned short off, char_buffer & buff);
	int get_row(unsigned long seg_id, unsigned int blk_off, unsigned short off,
			char_buffer & buff);

	int delete_row(unsigned short off);
	int delete_row(unsigned int blk_off, unsigned short off);
	int delete_row(unsigned long seg_id, unsigned int blk_off,
			unsigned short off);

	int update_row(unsigned short off, row_store &rs);
	int update_row(unsigned int blk_off, unsigned short off, row_store &rs);
	int update_row(unsigned long seg_id, unsigned int blk_off,
			unsigned short off, row_store &rs);

	bool has_next_row();
	void next_row(char_buffer & buff);
	bool has_pre_row();
	void pre_row(char_buffer & buff);

	void set_table_desc(table_desc * td);
	void set_start(segment * fs);
	void set_end(segment *es);
	void set_seg_mgr(seg_mgr * ps);
	segment * start();
	segment * end();

	table();
	table(segment * start, segment *end, table_desc * td);
	virtual ~table();
};

} /* namespace enginee */
} /* namespace sdb */

#endif /* TABLE_H_ */
