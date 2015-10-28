/*
 * bpt2.cpp
 *
 *  Created on: Oct 26, 2015
 *      Author: lqian
 */

#include "../index/bpt2.h"

namespace sdb {
namespace index {

void set_flag(char & c, const int bit) {
	char v = 1 << bit;
	c |= v;
}

void set_flag(ushort &s, const int bit) {
	ushort v = 1 << bit;
	s |= v;
}
void set_flag(short &s, const int bit) {
	ushort v = 1 << bit;
	s |= v;
}

void remove_flag(char & c, const int bit) {
	char v = ~(1 << bit);
	c &= v;
}

void remove_flag(ushort &s, const int bit) {
	ushort v = ~(1 << bit);
	s &= v;
}

void remove_flag(short &s, const int bit) {
	ushort v = ~(1 << bit);
	s &= v;
}

void _val::ref(char *buff, int len) {
	this->buff = buff;
	this->len = len;
}

void _val::set_data_item(const data_item &di) {
	char_buffer tmp(buff, len, true);
	tmp << di.seg_id << di.blk_off << di.row_idx;
}

void _val::set_data_item(const data_item *di) {
	char_buffer tmp(buff, len, true);
	tmp << di->seg_id << di->blk_off << di->row_idx;
}

void _val::to_data_item(data_item &di) {
	char_buffer tmp(buff, len, true);
	tmp >> di.seg_id >> di.blk_off >> di.row_idx;
}

void _val::to_data_item(data_item *di) {
	char_buffer tmp(buff, len, true);
	tmp >> di->seg_id >> di->blk_off >> di->row_idx;
}


void _inode::set_left_ipage(_ipage * p) {
	left_lpage = nullptr;
	left_ipage = p;
	header->left_pg_off = p->offset;
	header->left_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, INODE_LEFT_IPAGE_BIT);

	p->set_parent(this);
}

void _inode::set_right_ipage(_ipage *p) {
	right_lpage = nullptr;
	right_ipage = p;
	header->right_pg_off = p->offset;
	header->right_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, INODE_RIGHT_IPAGE_BIT);

	p->set_parent(this);
}

void _inode::set_left_lpage(_lpage *p) {
	left_ipage = nullptr;
	left_lpage = p;
	header->left_pg_off = p->offset;
	header->left_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, INODE_LEFT_LPAGE_BIT);

	p->set_parent(this);
}

void _inode::set_right_lpage(_lpage *p) {
	right_ipage = nullptr;
	right_lpage = p;
	header->right_pg_off = p->offset;
	header->right_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, INODE_RIGHT_LPAGE_BIT);

	p->set_parent(this);
}

void _ipage::set_parent(const _inode * n) {
	header->parent_in_off = n->offset;
	header->parent_ipg_off = n->cp->offset;
	header->parent_pg_seg_id = n->cp->seg->get_id();
	sdb::index::set_flag(header->flag, PAGE_PARENT_BIT);
}

int fsn_ipage::assign_inode(_inode &in) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off + ns <= length) {
		in.ref(off, buffer + off, ns);
		header->node_count++;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fsn_ipage::read_inode(ushort idx, _inode &n) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		n.ref(offset, buffer + off, ns);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fsn_ipage::remove_inode(ushort idx) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		fs_inode n;
		n.ref(off, buffer + off, ns);
		sdb::index::set_flag(n.header->flag, INODE_REMOVE_BIT);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

void _lpage::set_parent(const _inode *n) {
	header->parent_in_off = n->offset;
	header->parent_ipg_off = n->cp->offset;
	header->parent_pg_seg_id = n->cp->seg->get_id();
	sdb::index::set_flag(header->flag, PAGE_PARENT_BIT);
}

int fsn_lpage::assign_lnode(_lnode &ln) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off + ns <= length) {
		ln.ref(buffer + off, ns);
		header->node_count++;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fsn_lpage::remove_lnode(ushort idx) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		fs_lnode n;
		n.ref(buffer + off, ns);
		sdb::index::set_flag(n.header->flag, LNODE_REMOVE_BIT);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fsn_lpage::read_lnode(ushort idx, _lnode &ln) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		ln.ref(buffer + off, ns);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

bpt2::bpt2() {
	// TODO Auto-generated constructor stub
}

bpt2::~bpt2() {
	// TODO Auto-generated destructor stub
}

} /* namespace tree */
} /* namespace sdb */
