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

void _inode::set_left_page(_page *p) {
	left_page = p;
	header->left_pg_off = p->offset;
	header->left_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, NODE_LEFT_PAGE_BIT);
	p->set_parent(this);
}

void _inode::set_right_page(_page *p) {
	right_page = p;
	header->right_pg_off = p->offset;
	header->right_pg_seg_id = p->seg->get_id();
	sdb::index::set_flag(header->flag, NODE_RIGHT_PAGE_BIT);

	p->set_parent(this);
}

void _page::set_parent(const _inode * n) {
	header->parent_in_off = n->offset;
	header->parent_ipg_off = n->cp->offset;
	header->parent_pg_seg_id = n->cp->seg->get_id();
	sdb::index::set_flag(header->flag, PAGE_PARENT_BIT);
}

int fs_page::assign_node(_node *n) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off + ns <= length) {
		n->ref(off, buffer + off, ns);
		header->node_count++;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fs_page::read_node(ushort idx, _node *n) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		n->ref(offset, buffer + off, ns);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fs_page::remove_node(ushort idx) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off <= length) {
		fs_inode n;
		n.ref(off, buffer + off, ns);
		n.remove();
	} else {
		r = sdb::FAILURE;
	}
	return r;
}


bpt2::bpt2() {
	// TODO Auto-generated constructor stub
}


} /* namespace tree */
} /* namespace sdb */
