/*
 * table.cpp
 *
 *  Created on: Aug 26, 2015
 *      Author: lqian
 */

#include "table.h"

namespace sdb {
namespace enginee {

bool table::has_next_row() {
	if (curr_blk.row_count() > curr_row_idx) {
		return true;
	} else if (has_next_block()) {
		next_block(&curr_blk);
		return true;
	} else {
		return false;
	}
}

bool table::has_pre_row() {
	if (curr_row_idx > 0) {
		return true;
	} else if (has_pre_block()) {
		pre_block(&curr_blk);
		return true;
	} else {
		return false;
	}
}

void table::next_row(char_buffer & buff) {
	curr_blk.get_row_by_idx(curr_row_idx++, buff);
}

void table::pre_row(char_buffer &buff) {
	curr_blk.get_row_by_idx(--curr_row_idx, buff);
}

bool table::has_next_seg() {
	return curr_seg->has_nex_seg();
}

bool table::has_pre_seg() {
	return curr_seg->has_pre_seg();
}

segment * table::pre_seg() {
	return segmgr->find_segment(curr_seg->get_pre_seg_id());
}

segment * table::next_seg() {
	return segmgr->find_segment(curr_seg->get_next_seg_id());
}

bool table::has_next_block() {
	return (curr_seg->last_block_off() > curr_off) ? true : has_next_seg();
}

bool table::has_pre_block() {
	return curr_off == 0 ? has_pre_seg() : true;
}

void table::next_block(mem_data_block * blk) {
	if (curr_off < curr_seg->last_block_off()) {
		curr_off += curr_seg->get_block_size();
	} else {
		curr_seg = segmgr->find_segment(curr_seg->get_next_seg_id());
		curr_off = 0;
	}
	blk->offset = curr_off;
	curr_seg->read_block(blk);
	blk->off_tbl.clear();
	blk->parse_off_tbl();
	curr_row_idx = 0;
}

void table::pre_block(mem_data_block * blk) {
	if (curr_off == 0) {
		curr_seg = pre_seg();
		curr_off = curr_seg->last_block_off();
	} else {
		curr_off -= curr_seg->get_block_size();
	}
	blk->offset = curr_off;
	curr_seg->read_block(blk);
	blk->off_tbl.clear();
	blk->parse_off_tbl();
	curr_row_idx = blk->row_count();
}

int table::get_row(unsigned short off, char_buffer &buff) {
	return last_blk->get_row(off, buff);
}

int table::get_row(unsigned int blk_off, unsigned short off,
		char_buffer & buff) {
	mem_data_block blk;
	blk.offset = off;
	int r = last_seg->read_block(blk);
	if (r) {
		return blk.get_row(off, buff);
	}
	return r;
}

int table::get_row(unsigned long seg_id, unsigned int blk_off,
		unsigned short off, char_buffer & buff) {
	int r = sdb::FAILURE;
	segment * seg = segmgr->find_segment(seg_id);
	if (seg != nullptr) {
		mem_data_block blk;
		blk.offset = off;
		int r = seg->read_block(blk);
		if (r) {
			r = blk.get_row(off, buff);
		}
	}
	return r;
}

int table::open() {
	int r = sdb::FAILURE;

	curr_seg = first_seg;
	curr_off = 0;
	curr_blk.offset = curr_off;
	if (first_seg->get_block_count() == 0) {
		r = first_seg->assign_block(curr_blk);
	} else {
		r = first_seg->read_block(curr_blk);
	}
	if (r != sdb::SUCCESS) {
		return r;
	}
	curr_blk.parse_off_tbl();
	last_blk = new mem_data_block;
	last_blk->offset = last_seg->last_block_off();
	r = last_seg->read_block(last_blk);
	return r;
}

int table::head() {
	curr_seg = first_seg;
	curr_off = 0;
	curr_blk.offset = curr_off;
	int r = first_seg->read_block(curr_blk);
	if (r) {
		curr_blk.off_tbl.clear();
		curr_blk.parse_off_tbl();
	}
	return r;
}

int table::tail(){
	int r = sdb::SUCCESS;
	curr_seg = last_seg;
	curr_off = last_seg->last_block_off();
	r = curr_seg->read_block(curr_blk);
	if (r) {
		curr_blk.off_tbl.clear();
		curr_blk.parse_off_tbl();
	}
	return r;
}

int table::close() {
	int r = sdb::SUCCESS;
	r = flush();
	return r;
}

int table::flush() {
	last_blk->write_off_tbl();
	return sdb::SUCCESS;
}

int table::add_row(row_store & rs) {
	int r = sdb::FAILURE;
	char_buffer buff(1024);
	rs.write_to(buff);

	//last block is full
	r = last_blk->add_row_data(buff.data(), buff.size());
	if (r == UNKNOWN_OFFSET) {
		last_blk->write_off_tbl();
		if (!last_seg->has_remain_block()) {
			segment * seg = new segment;
			r = segmgr->assign_segment(seg);
			if (r) {
				last_seg->set_next_seg(seg);
				last_seg = seg;
			} else {
				return r;
			}
		}

		mem_data_block *nl = new mem_data_block;
		if (last_seg->assign_block(nl)) {
			delete last_blk;
			last_blk = nl;
			return last_blk->add_row_data(buff.data(), buff.size());
		} else {
			return ASSIGN_BLOCK_FAILURE;
		}
	}
	return r;
}

void table::set_seg_mgr(seg_mgr * ps) {
	segmgr = ps;
}
int table::delete_row(unsigned short off) {
	return last_blk->delete_row_off(off);
}

int table::delete_row(unsigned int blk_off, unsigned short row_off) {
	mem_data_block blk;
	blk.offset = blk_off;
	int r = last_seg->read_block(blk);
	if (r) {
		blk.parse_off_tbl();
		r = blk.delete_row_off(row_off);
		if (r) {
			blk.write_off_tbl();
		}
	}
	return r;
}

int table::delete_row(unsigned long seg_id, unsigned int blk_off,
		unsigned short row_off) {
	int r = sdb::FAILURE;
	segment * seg = segmgr->find_segment(seg_id);
	if (seg != nullptr) {
		mem_data_block blk;
		blk.offset = blk_off;
		r = seg->read_block(blk);
		if (r) {
			blk.parse_off_tbl();
			r = blk.delete_row_off(row_off);
			if (r) {
				blk.write_off_tbl();
			}
		}
	} else {
		return SEGMENG_NOT_EXISTED;
	}
	return r;
}

int table::update_row(unsigned short off, row_store & rs) {
	int r = sdb::FAILURE;
	char_buffer buff;
	rs.write_to(buff);
	r = last_blk->update_row_data(off, buff.data(), buff.size());

	if (r == DELETE_OFFSET) { // the row offset is delete
		mem_data_block * old = last_blk;
		r = add_row(rs);
		if (r != UNKNOWN_OFFSET) {
			old->write_off_tbl();
		} else {
			return r;
		}
	}

	return r;
}

int table::update_row(unsigned int blk_off, unsigned short off,
		row_store & rs) {
	int r = sdb::FAILURE;
	char_buffer buff;
	rs.write_to(buff);
	mem_data_block blk;
	blk.offset = blk_off;
	r = last_seg->read_block(blk);
	if (r) {
		r = blk.update_row_data(off, buff.data(), buff.size());
		if (r == DELETE_OFFSET) {
			r = add_row(rs);
			if (r != UNKNOWN_OFFSET) {
				blk.write_off_tbl();
			}
		}
	}
	return r;
}

int table::update_row(unsigned long seg_id, unsigned int blk_off,
		unsigned short off, row_store & rs) {
	int r = sdb::FAILURE;
	segment * seg = segmgr->find_segment(seg_id);
	if (seg != nullptr) {
		char_buffer buff;
		rs.write_to(buff);
		mem_data_block blk;
		blk.offset = blk_off;
		r = seg->read_block(blk);
		if (r) {
			r = blk.update_row_data(blk_off, buff.data(), buff.size());
			if (r == DELETE_OFFSET) {
				r = add_row(rs);
				if (r != UNKNOWN_OFFSET) {
					blk.write_off_tbl();
				}
			}
		}
	} else {
		return SEGMENG_NOT_EXISTED;
	}

	return r;
}

segment * table::start() {
	return first_seg;
}
segment * table::end() {
	return last_seg;
}

void table::set_start(segment * fs) {
	first_seg = fs;
}
void table::set_end(segment *es) {
	last_seg = es;
}

void table::set_table_desc(table_desc * td) {
	tdesc = td;
}

table::table(segment * start, segment *end, table_desc * td) {
	first_seg = start;
	last_seg = end;
	tdesc = td;
}

table::table() {

}

table::~table() {
	last_blk->write_off_tbl();
	delete last_blk;
}

} /* namespace enginee */
} /* namespace sdb */
