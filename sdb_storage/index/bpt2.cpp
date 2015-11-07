/*
 * bpt2.cpp
 *
 *  Created on: Oct 26, 2015
 *      Author: lqian
 */

#include "../index/bpt2.h"

namespace sdb {
namespace index {

bool test_flag(ushort &s, const int bit) {
	ushort v = 1 << bit;
	return s & v == v;
}

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

key_test test(_page *p, _key &k, _node & in) {
	_key ik;
	int i = 0;
	int nc = p->count();
	for (; i < nc; i++) {
		p->read_node(i, &in);
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

	if (i == nc) {
		return key_test::key_greater;
	}
}

_page *new_page(_page * sibling, bool fixed_size) {
	if (sibling->test_flag(PAGE_HAS_LEAF_BIT)) {
		return new_ipage();
	} else {
		return new_lpage();
	}
}

_ipage * new_ipage(bool fixed_size) {
	_ipage *p;
	p = fixed_size ? (_ipage *) new fs_ipage : new vs_ipage;
	p->set_flag(PAGE_HAS_LEAF_BIT);
	return p;
}

_lpage * new_lpage(bool fixed_size) {
	return fixed_size ? (_lpage *) new fs_lpage : new vs_lpage;
}

_lnode * new_lnode(bool fixed_size) {
	return fixed_size ? (_lnode *) new fs_lnode : new vs_lnode;
}

_inode * new_inode(bool fixed_size) {
	return fixed_size ? (_inode *) new fs_inode : new vs_inode;
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
	header->parent_in_idx = n->offset;
	header->parent_ipg_off = n->cp->offset;
	header->parent_pg_seg_id = n->cp->seg->get_id();
	set_flag(PAGE_HAS_PARENT_BIT);
}

int fs_page::assign_node(fs_page *p, _node *n) {
	int r = p->header->node_count;
	int ns = p->header->node_size;
	int off = p->header->node_count * ns;
	if (off + ns < p->length) {
		n->ref(off, p->buffer + off, ns);
		p->header->node_count++;
		p->header->active_node_count++;
	} else {
		p->set_flag(PAGE_FULL_BIT);
		r = sdb::FAILURE;
	}
	return r;
}

int fs_page::read_node(fs_page *p, ushort idx, _node *n) {
	int r = idx;
	int ns = p->header->node_size;
	int off = idx * ns;
	if (idx < p->header->node_count) {
		n->ref(off, p->buffer + off, ns);
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int fs_page::remove_node(fs_page *p, ushort idx) {
	int r = idx;
	int ns = p->header->node_size;
	int off = idx * ns;
	if (r < p->header->node_count) {
		fs_inode n;
		n.ref(off, p->buffer + off, ns);
		n.remove();
		p->header->active_node_count--;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

void fs_page::clean_node(fs_page *p) {
	p->header->node_count = 0;
	p->header->active_node_count = 0;
}

void fs_page::sort_nodes(list<_key_field> key_fields, fs_page *p, bool ascend) {
	_node * a, *b;
//	if (sdb::index::test_flag(p->header->flag, PAGE_HAS_LEAF_BIT)) {
	if (p->test_flag(PAGE_HAS_LEAF_BIT)) {
		a = new_inode();
		b = new_inode();
	} else {
		a = new_lnode();
		b = new_lnode();
	}

	int c;
	_key ka, kb;
	ka.key_fields = key_fields;
	kb.key_fields = key_fields;
	ushort ns = p->header->node_size;
	int hs = sizeof(fs_inode::head);
	ka.len = ns - hs;
	kb.len = ns - hs;
	char * tmp = new char[ns];
	for (int i = 0; i < p->header->node_count; i++) {
		for (int j = i - 1; j >= 0; j--) {
			read_node(p, j + 1, a);
			if (a->test_flag(NODE_REMOVE_BIT))
				continue;

			read_node(p, j, b);
			if (b->test_flag(NODE_REMOVE_BIT))
				continue;

			a->read_key(ka);
			b->read_key(kb);

			c = ka.compare(kb);
			if ((ascend && c < 0) || (!ascend && c > 0)) {
				memcpy(tmp, a->buffer - hs, ns);
				memcpy(a->buffer - hs, b->buffer - hs, ns);
				memcpy(b->buffer - hs, tmp, ns);
			}
		}
	}
	delete[] tmp;
}

void fs_page::init_header(fs_page *p) {
	std::cout << "" << std::endl;
}
/*
 * for variant node, its length must be clear
 */
int vs_page::assign_node(vs_page *p, _node * n) {
	int weo = p->header->writing_entry_off;
	int wdo = p->header->writing_data_off;
	int r = wdo;
	if (weo + n->len + VS_PAGE_DIR_ENTRY_LENGTH > wdo) {
		p->set_flag(PAGE_FULL_BIT);
		return sdb::FAILURE;
	} else {
		// write node's offset to directory
		ushort off = wdo + n->len;
		char_buffer cb(p->buffer + weo, VS_PAGE_DIR_ENTRY_LENGTH, true);
		char c = 0;
		cb << off << c;

		//ref buffer for the node
		p->header->writing_data_off -= n->len;
		wdo += p->header->writing_entry_off += VS_PAGE_DIR_ENTRY_LENGTH;
		p->header->active_node_count++;
		n->ref(off, p->buffer + off, n->len);
		return weo;
	}
}

int vs_page::read_node(vs_page *p, ushort idx, _node * n) {
	int dir_len = VS_PAGE_DIR_ENTRY_LENGTH;
	if (idx * dir_len < p->header->writing_entry_off) {
		int weo = idx * dir_len;
		char_buffer cb(p->buffer + weo, dir_len, true);
		ushort off;
		char c;
		cb >> off >> c;
		int len;
		if (idx == 0) {
			len = p->length - off;
		} else {
			cb.reset();
			cb.ref_buff(p->buffer + weo + dir_len, dir_len);
			ushort off2;
			cb >> off2;
			len = off - off2;
		}
		n->ref(off, p->buffer + off, len);
	} else {
		return sdb::FAILURE;
	}
}

int vs_page::remove_node(vs_page *p, ushort idx) {
	int r = sdb::SUCCESS;
	_node *n =
			p->test_flag(PAGE_HAS_LEAF_BIT) ?
					(_node*) new fs_lnode : new vs_inode;
	if (read_node(p, idx, n) >= 0) {
		n->remove();
	} else {
		r = sdb::FAILURE;
	}
	delete n;
	return r;
}

void vs_page::clean_node(vs_page *p) {
	p->header->writing_entry_off = 0;
	p->header->writing_data_off = p->length;
}

void vs_page::init_header(vs_page *p) {

}

void vs_page::sort_nodes(list<_key_field> key_fields, vs_page *p, bool ascend) {

}

void fs_lpage::ref(uint off, char* buff, ushort w_len) {
	offset = off;
	length = w_len - sizeof(head);
	header = (head *) buff;
	buffer = buff + sizeof(head);
	ref_flag = true;
}

int fs_lpage::assign_node(_node* n) {
	return fs_page::assign_node(this, n);
}

int fs_lpage::read_node(ushort idx, _node* in) {
	return fs_page::read_node(this, idx, in);
}

int fs_lpage::remove_node(ushort idx) {
	return fs_page::remove_node(this, idx);
}

int fs_lpage::count() {
	return header->node_count;
}

void fs_lpage::clean_nodes() {
	return fs_page::clean_node(this);
}

void fs_lpage::sort_nodes(list<_key_field> key_fields, bool ascend) {
	return fs_page::sort_nodes(key_fields, this, ascend);
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
		if (blk.test_flag(PAGE_HAS_LEAF_BIT)) {
			root = new_lpage(fixed_size);
		} else {
			root = new_ipage(fixed_size);
		}
		root->offset = root_blk_off;
		r = first_seg->read_block(root);

	} else {
		r = INVALID_INDEX_SEGMENT_ID;
	}

	last_seg = sm->find_segment(last_seg_id);
	if (last_seg == nullptr) {
		r = INVALID_INDEX_SEGMENT_ID;
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
		root = new_lpage(fixed_size);
		first_seg->assign_block(root);
	} else {
		r = INVALID_INDEX_SEGMENT_ID;
	}
	return r;
}

int bpt2::insert(_page *lp, _key *k, _val *v) {
	int r = sdb::SUCCESS;
	_lnode *ln = new_lnode(fixed_size);
	lp->assign_node(ln);
	ln->write_key(k);
	if (v != nullptr) {
		ln->write_val(v);
	}

	if (lp->test_flag(PAGE_HAS_LEAF_BIT)) {
		// lp->sort_nodes();
	}

	if (lp->is_full()) {
		_lpage *half;
		_inode *in = new_inode(fixed_size);
		_key mk;
		assign_lpage(half);
		split_2(lp, half);

		// set lp's next page as half's next page
		if (lp->test_flag(NEXT_BLK_BIT)) {
			_page *nxt = new_page(lp, fixed_size);
			fetch_next_page(lp, nxt);
			half->set_next_blk(nxt);
			delete nxt;
		}
		lp->set_next_blk(half);
		half->set_pre_block(lp);

		if (lp->test_flag(PAGE_HAS_PARENT_BIT)) {
			_inode *pn = new_inode(fixed_size);
			_ipage *pp = new_ipage(fixed_size);

			fetch_parent_page(lp, pp);
			pp->read_node(lp->header->parent_in_idx, pn);

			// set the right page as null of old parent node
			pn->clean_flag(NODE_RIGHT_PAGE_BIT);

			in->set_left_page(lp);

//			if (in->has_right_sibling) {
//				in->right_sibling->set_left_page(half)
//			}
//			else

			in->set_right_page(half);
			in->read_key(mk);
			r = insert(pp, &mk);
			delete pn;
			delete pp;
		} else { // the root is leaf page
			_ipage * n_root;
			assign_ipage(n_root);
			n_root->assign_node(in);

			// the first key of right half page to parent page
			half->read_node(0, ln);
			ln->read_key(mk);
			in->write_key(mk);

			in->set_left_page(lp);
			in->set_right_page(half);
			n_root->set_flag(PAGE_HAS_LEAF_BIT);

			delete half;
			delete root;
			root = n_root;
		}
		//split, add_to parent
		// parent split
		delete in;
	}

	delete ln;

	return r;
}

void bpt2::split_2(_page * p, _page *half) {
	//p->sort_nodes();
}

int bpt2::assign_lpage(segment *seg, _lpage *&lp) {
	lp = new_lpage(fixed_size);
	return seg->assign_block(lp);
}

int bpt2::assign_ipage(segment *seg, _ipage *&ip) {
	ip = new_ipage(fixed_size);
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
			r = INVALID_INDEX_SEGMENT_ID;
		}
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

int bpt2::fetch_pre_page(_page *p, _page *pre) {
	int r = sdb::SUCCESS;
	if (p->test_flag(PRE_BLK_BIT)) {
		segment *seg = sm->find_segment(p->header->parent_pg_seg_id);
		if (seg) {
			pre->offset = p->header->pre_blk_off;
			seg->read_block(pre);
			p->set_pre_block(pre);
		} else {
			r = INVALID_INDEX_SEGMENT_ID;
		}
	} else {
		r = NO_PRE_BLK;
	}
	return r;
}

int bpt2::fetch_next_page(_page *p, _page * nxt) {
	int r = sdb::SUCCESS;
	if (p->test_flag(PRE_BLK_BIT)) {
		segment *seg = sm->find_segment(p->header->parent_pg_seg_id);
		if (seg) {
			nxt->offset = p->header->next_blk_off;
			seg->read_block(nxt);
			p->set_next_blk(nxt);
		} else {
			r = INVALID_INDEX_SEGMENT_ID;
		}
	} else {
		r = NO_PRE_BLK;
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
			r = INVALID_INDEX_SEGMENT_ID;
		}
	} else {
		r = sdb::FAILURE;
	}

	return r;
}

int bpt2::fetch_parent_inode(_page *p, _inode *in) {

}

int bpt2::fetch_parent_page(_page *p, _page *pp) {
	int r = sdb::SUCCESS;
	if (p->test_flag(PRE_BLK_BIT)) {
		segment *seg = sm->find_segment(p->header->parent_pg_seg_id);
		if (seg) {
			pp->offset = p->header->parent_ipg_off;
			r = seg->read_block(pp);
		} else {
			r = INVALID_INDEX_SEGMENT_ID;
		}
	} else {
		r = NO_PRE_BLK;
	}
	return r;
}

} /* namespace tree */
} /* namespace sdb */

void sdb::index::vs_lpage::ref(uint off, char* buff, ushort w_len) {
	offset = off;
	header = (head *) buff;
	buffer = buff + sizeof(head);
	length = w_len - sizeof(head);
	ref_flag = true;
}

int sdb::index::vs_lpage::assign_node(_node* n) {
	return vs_page::assign_node(this, n);
}

int sdb::index::vs_lpage::read_node(ushort idx, _node* n) {
	return vs_page::read_node(this, idx, n);
}

int sdb::index::vs_lpage::remove_node(ushort idx) {
	return vs_page::remove_node(this, idx);
}

void sdb::index::vs_lpage::sort_nodes(list<_key_field> key_fields,
		bool ascend) {
	return vs_page::sort_nodes(key_fields, this, ascend);
}

int sdb::index::vs_lpage::count() {
	return header->writing_entry_off / VS_PAGE_DIR_ENTRY_LENGTH;
}

void sdb::index::vs_lpage::clean_nodes() {
	return vs_page::clean_node(this);
}
