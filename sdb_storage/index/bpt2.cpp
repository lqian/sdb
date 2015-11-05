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
		header->active_node_count++;
	} else {
		set_flag(PAGE_FULL_BIT);
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
		header->active_node_count--;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int vs_page::assign_node(_node * n) {
	int weo = header->writing_entry_off;
	int wdo = header->writing_data_off;
	if (weo + n->len + NODE_DIR_ENTRY_LENGTH < wdo) {
		header->writing_entry_off += NODE_DIR_ENTRY_LENGTH;
		header->writing_data_off -= n->len;
		n->ref(weo, buffer + weo, n->len);
		header->active_node_count++;
		return weo;
	} else {
		set_flag(PAGE_FULL_BIT);
		return -1;
	}
}
int vs_page::read_node(ushort idx, _node *) {
	int weo = idx * NODE_DIR_ENTRY_LENGTH;
	char_buffer buff(buffer + weo, NODE_DIR_ENTRY_LENGTH, true);
	return 0;
}
int vs_page::remove_node(ushort idx) {
	header->active_node_count--;
	return 0;
}

key_test _ipage::test(_key &k, _inode & in) {
	_key ik;
	int i = 0;
	for (; i < count(); i++) {
		read_node(i, &in);
		if (in.test_flag(NODE_REMOVE_BIT)) {
			continue;
		}
		in.read_key(ik);
		int c = k.compare(ik);
		if (c == 0) {
			return key_test::key_equal;
		} else if (c < 0) {
			return key_test::key_less;
		} else if (c > 0) {
			continue;
		}
	}

	if (i == count()) {
		return key_test::key_greater;
	}
}

bpt2::bpt2() {
	sm = &LOCAL_SEG_MGR;
}

bpt2::bpt2(ulong obj_id) {
	this->obj_id = obj_id;
	sm = &LOCAL_SEG_MGR;
}

bpt2::~bpt2() {
	if (loaded) {
		delete root;
	}
}

int bpt2::load(ulong first_seg_id, ulong last_seg_id, uint root_blk_off) {
	int r = sdb::SUCCESS;
	first_seg = sm->find_segment(first_seg_id);
	if (first_seg) {
		mem_data_block blk;
		blk.offset = root_blk_off;
		if (blk.test_flag(PAGE_LEAF_BIT)) {
			if (fixed_size) {
				root = new fs_ipage;
			} else {
				root = new vs_ipage;
			}
		} else {
			if (fixed_size) {
				root = new fs_lpage;
			} else {
				root = new vs_lpage;
			}
		}
		root->offset = root_blk_off;
		r = first_seg->read_block(root);

	} else {
		r = INVALID_SEGMENT_ID;
	}

	last_seg = sm->find_segment(last_seg_id);
	if (last_seg == nullptr) {
		r = INVALID_SEGMENT_ID;
	}

	if (r == sdb::SUCCESS) {
		loaded = true;
	}

	return r;
}

int bpt2::create(ulong first_seg_id) {
	int r = sdb::SUCCESS;
	first_seg = sm->find_segment(first_seg_id);
	if (first_seg) {
		last_seg = first_seg;
		if (fixed_size) {
			root = new fs_lpage;
		} else {
			root = new vs_lpage;
		}
		first_seg->assign_block(root);
	} else {
		r = INVALID_SEGMENT_ID;
	}
	return r;
}

int bpt2::add_node(_lnode *n) {
	if (root->test_flag(PAGE_LEAF_BIT)) {
	} else {
		add_node(root, n);
	}
}

int bpt2::add_node(_page *lp, _lnode*n) {
	_lnode *ln;
	create_lnode(ln);
	lp->assign_node(ln);
	_key k;
	_val v;
	n->read_key(k);
	n->read_val(v);
	ln->write_key(k);
	ln->write_val(v);


	if (lp->is_full()) {
		if (lp->test_flag(PAGE_PARENT_BIT)) {
			_inode *pn;
			_ipage *pp;
			craete_inode(pn);
			create_ipage(pp);

			fetch_parent_inode(lp, pn);
			fetch_parent_page(lp, pp);
			delete pn;
			delete pp;
		} else { // the root is leaf page
			_lpage *half;
			_inode *in;
			_key & m;
			assign_lpage(half);
			split_2(lp, half);

			_ipage * n_root;
			assign_ipage(n_root);
			craete_inode(in);
			n_root->assign_node(in);

			// the first key of right half page to parent page
			half->read_node(0, ln);
			ln->read_key(m);
			in->write_key(m);

			delete left, right, root;
			delete in;
			root = n_root;
		}
		//split, add_to parent
		// parent split
	}

	delete ln;
}

void bpt2::split_2(_page * p, _page *half) {
	p->sort_nodes();
}

void bpt2::create_ipage(_ipage *&p) {
	p = fixed_size ? new fs_ipage : new vs_ipage;
}

void bpt2::create_lpage(_lpage *&p) {
	p = fixed_size ? new fs_lpage : new vs_lpage;
}

void bpt2::craete_inode(_inode * &in) {
	in = fixed_size ? new fs_inode : new vs_inode;
}

void bpt2::create_lnode(_lnode * & ln) {
	ln = fixed_size ? new fs_lnode : new vs_lnode;
}

int bpt2::assign_lpage(segment *seg, _lpage *&lp) {
	if (fixed_size) {
		lp = new fs_lpage;
	} else {
		lp = new vs_lpage;
	}
	return seg->assign_block(lp);
}

int bpt2::assign_ipage(segment *seg, _ipage *&ip) {
	if (fixed_size) {
		ip = new fs_ipage;
	} else {
		ip = new vs_ipage;
	}
	return seg->assign_block(ip);
}

int bpt2::assign_ipage(_ipage*& ip) {
	int r = assign_ipage(last_seg, ip);
	if (r == sdb::FAILURE) {
		segment * seg;
		r = sm->assign_segment(seg);
		if (r) {
			last_seg = seg;
			r = assign_ipage(last_seg, ip);
		}
	}
	return r;
}

int bpt2::assign_lpage(_lpage * & lp) {
	int r = assign_lpage(last_seg, lp);
	if (r == sdb::FAILURE) {
		segment * seg;
		r = sm->assign_segment(seg);
		if (r) {
			last_seg = seg;
			r = assign_lpage(last_seg, lp);
		}
	}
	return r;
}

int bpt2::fetch_right_page(_inode *in, _page *p) {
	int r = sdb::SUCCESS;
	if (in->test_flag(NODE_RIGHT_PAGE_BIT)) {
		segment *seg = sm->find_segment(in->header->right_pg_seg_id);
		if (seg) {
			p->offset = in->header->right_pg_off;
			seg->read_block(p);
			p->set_parent(in);
			in->set_right_page(p);
			r = sdb::SUCCESS;
		} else {
			r = INVALID_SEGMENT_ID;
		}
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int bpt2::fetch_left_page(_inode *in, _page * p) {
	int r = sdb::SUCCESS;
	if (in->test_flag(NODE_LEFT_PAGE_BIT)) {
		segment * seg = sm->find_segment(in->header->left_pg_seg_id);
		if (seg) {
			p->offset = in->header->left_pg_off;
			seg->read_block(p);
			in->left_page = p;
			p->set_parent(in);
			in->set_left_page(p);
			r = sdb::SUCCESS;
		} else {
			r = INVALID_SEGMENT_ID;
		}
	} else {
		r = sdb::FAILURE;
	}

	return r;
}
} /* namespace tree */
} /* namespace sdb */
