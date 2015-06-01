/*
 * bp.cpp
 *
 *  Created on: May 12, 2015
 *      Author: linkqian
 */

#include <exception>
#include "bptree.h"
#include "../common/encoding.h"

namespace sdb {
namespace tree {

/*
 * a disk b+tree uses a fixed-size-data-block as a page that contains node2.
 *
 * the first page of the first/root segment is the root page. it always keep
 * its position when it splits. and its index segment assign two child pages.
 *
 * if an index segment is full, assign a new index segment and makes double link
 * of them. and assign new page in the new index segment.
 *
 * node2 in the non-leaf page is sorted. node2 of leaf page is not. sort can
 * be operated in phrase after splitting/merging in memory.
 *
 *
 */
bptree::bptree(index_segment * _root_seg, int kl, int vl, int bs) {
	this->root_seg = _root_seg;
	this->root_seg->tree = this;
	this->curr_seg = this->root_seg;
	this->key_len = kl;
	this->val_len = vl;
	this->block_size = bs * kilo_byte;
	this->m = (block_size - block_header_size)
			/ (key_len + val_len + node2_header_size);

	if (root_seg->block_count == 0) {
		root_seg->assign_page(&root_page);
	} else {
		root_page.offset = 0;
		root_seg->read_page(&root_page);
	}
}

bptree::bptree(index_segment * _root_seg, int m, int kl, int vl, int bs) {
	this->root_seg = _root_seg;
	this->root_seg->tree = this;
	this->curr_seg = this->root_seg;
	this->key_len = kl;
	this->val_len = vl;
	this->block_size = bs * kilo_byte;
	this->m = m;
	int rm = (block_size - block_header_size)
			/ (key_len + val_len + node2_header_size);
	if (m > rm) {
		//	throw exception("invalid max order value");
	}

	if (root_seg->block_count == 0) {
		root_seg->assign_page(&root_page);
	} else {
		root_page.offset = 0;
		root_seg->read_page(&root_page);
	}
}

int bptree::fetch_left_page(node2 & n, page & lp) {
	int off = n.header->left_pg_off;
	lp.offset = off;
	lp.parent = &n;
	int r = sdb::FAILURE;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_LP_SEG_BIT)) {
		segment seg;
		if (ref_seg->next_seg(seg)) {  //TODO find seg via seg_id
			index_segment idx_seg(seg);
			r = idx_seg.read_page(&lp); // initialized the pointer
		}
	} else {
		r = n.ref_page->idx_seg->read_page(&lp);
	}
	// refine the page parent define if parent node has been found moving or split
	if (r == sdb::SUCCESS) {
		lp.header->parent_blk_off = n.ref_page->offset;
		lp.header->parent_nd2_off = n.offset;
	}
	return r;
}

int bptree::fetch_right_page(node2 & n, page & rp) {
	int off = n.header->right_pg_off;
	rp.offset = off;
	rp.parent = &n;
	int r = sdb::FAILURE;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_RP_SEG_BIT) || n.test_flag(ND2_RP_DF_BIT)) {
		segment seg;
		if (ref_seg->next_seg(seg)) {
			index_segment idx_seg(seg);
			r = idx_seg.read_page(&rp);
		}
	} else {
		r = n.ref_page->idx_seg->read_page(&rp);
	}
	// refine the page parent define if parent node has been found moving or split
	if (r == sdb::SUCCESS) {
		rp.header->parent_blk_off = n.ref_page->offset;
		rp.header->parent_nd2_off = n.offset;
	}
	return r;
}

int bptree::fetch_parent_page(node2 &n, page & pp) {
	int off = n.ref_page->header->parent_blk_off;
	pp.offset = off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.ref_page->test_flag(ND2_PP_SEG_BIT)
			|| n.ref_page->test_flag(ND2_PP_DF_BIT)) {
		segment seg;
		if (ref_seg->next_seg(seg)) {
			index_segment idx_seg(seg);
			return idx_seg.read_page(&pp);
		}
	} else {
		return n.ref_page->idx_seg->read_page(&pp);
	}
}

int bptree::fetch_pre_page(page &p, page & pre) {
	pre.offset = p.header->pre_blk_off;
	if (p.test_flag(PRE_BLK_BIT)) {
		if (p.test_flag(PRE_BLK_SPAN_DFILE_BIT)
				|| p.test_flag(PRE_BLK_SPAN_SEG_BIT)) {
			int p_id = p.idx_seg->pre_seg_id;
			auto it = seg_map.find(p_id);
			if (it != seg_map.end()) {
				return (*it).second.read_page(&pre);
			} else {
				segment p_seg;
				int r = p.idx_seg->pre_seg(p_seg);
				index_segment p_idx_seg(p_seg);
				if (r) {
					seg_map.insert(
							std::pair<unsigned long, index_segment>(p_id,
									p_idx_seg));
					return p_idx_seg.read_page(&pre);
				} else {
					return r;
				}
			}
		} else {
			return p.idx_seg->read_page(&pre);
		}
	} else {
		return NO_PRE_BLK;
	}
}

int bptree::fetch_next_page(page &p, page & next) {
	next.offset = p.header->next_blk_off;
	if (p.test_flag(PRE_BLK_BIT)) {
		if (p.test_flag(PRE_BLK_SPAN_DFILE_BIT)
				|| p.test_flag(PRE_BLK_SPAN_SEG_BIT)) {
			int p_id = p.idx_seg->pre_seg_id;
			auto it = seg_map.find(p_id);
			if (it != seg_map.end()) {
				return (*it).second.read_page(&next);
			} else {
				segment p_seg;
				int r = p.idx_seg->pre_seg(p_seg);
				index_segment p_idx_seg(p_seg);
				if (r) {
					seg_map.insert(
							std::pair<unsigned long, index_segment>(p_id,
									p_idx_seg));
					r = p_idx_seg.read_page(&next);
				}
			}
		} else {
			return p.idx_seg->read_page(&next);
		}
	} else {
		return NO_NEXT_BLK;
	}
}

int bptree::find_page(page &f, const key &k, page & t, key_test_enum & kt) {
	if (f.is_leaf_page()) {
		t = f;
		return sdb::SUCCESS;
	} else {
		node2 n;
		kt = f.test_key(k, n);

		// non-leaf pages
		if (kt == key_within_page) {
			if (n.test_flag(ND2_LP_BIT)) {
				page lp;
				fetch_left_page(n, lp);
				return find_page(lp, k, t, kt);
			}
		} else if (kt == key_less) {
			if (n.test_flag(ND2_LP_BIT)) {
				page lp;
				fetch_left_page(n, lp);
				return find_page(lp, k, t, kt);
			}
		} else if (kt == key_greater || kt == key_equals) {
			if (n.test_flag(ND2_RP_BIT)) {
				page rp;
				fetch_right_page(n, rp);
				return find_page(rp, k, t, kt);
			}
		}
		return sdb::FAILURE;
	}
}

int bptree::add_key(key & k, val& v) {
	key_test_enum kt = not_defenition;
	page p;

	if (find_page(root_page, k, p, kt) == sdb::SUCCESS) {
		return this->add_key(p, k, v, kt);
	} else {
		return sdb::FAILURE;
	}
}

void bptree::split_2(page &p, page &left, page &right) {
	int h = (p.header->node_count + 1) / 2;
	int h_off = h * p.header->node_size;
	int r_len = p.header->node_size * p.header->node_count - h_off;
	memcpy(left.buffer, p.buffer, h_off);
	left.header->node_count = h;

	memcpy(right.buffer, p.buffer + h_off, r_len);
	right.header->node_count = p.header->node_count - h;
	right.header->node_size = p.header->node_size;

	node2 t; // the middle key
	t.offset = h_off;
	p.read_node(&t);
	key mk;
	mk.v = new char[t.k.len];
	mk.set_val(t.k.v, t.k.len);

	p.purge_node();
	p.set_flag(PAGE_HAS_LEAF_BIT); //  set the page flag has children page
	node2 nn;  // the new first node of root page
	p.assign_node(&nn);
	nn.set_key(mk);
	delete[] mk.v;

	// bind left and right page for node2, bind previous and next page for children
	nn.set_left(&left);
	nn.set_right(&right);
	left.set_next_page(&right);
}

void bptree::split_2_3(page &p1, page &p2, page & target) {
	int c1 = p1.header->node_count;
	int c2 = p2.header->node_count;
	int p = (c1 + c2) / 3;
	int f1 = p;
	int f2 = c2 - p;

	transfer(p2, f2, p, target);
	transfer(p1, f1, c1 - f1, p2);

	p2.sort_in_mem();
	target.sort_in_mem();

	page * parent_page = p1.parent->ref_page;

	node2 n1;
	n1.offset = 0;
	p2.read_node(&n1);
	node2 n2;
	n2.offset = 0;
	target.read_node(&n2);
	node2 nt;
	key_test_enum kt = parent_page->test_key(n2.k, nt);

	node2 nn; // the new node to be add parent;
	parent_page->assign_node(&nn);

	// the parent node is last key of its page
	if (*p1.parent == *p2.parent) {
		p1.parent->set_key(n1.k);
		p1.parent->remove_flag(ND2_RP_BIT);  //remove right page flag
		p1.set_next_page(&p2);

		nn.set_left(&p2);
		nn.set_right(&target);
		p2.set_next_page(&target);
		check_split(*parent_page, kt);
	} else {
		// the new key n2 is greater than n1 but less than next key of n1
		// also, the new key only has left page
		p1.parent->set_key(n1.k);
		n2.remove_flag(ND2_RP_BIT);
		check_split(*parent_page, kt);
	}
}

int bptree::check_split(page &p, key_test_enum kt) {
	if (p.test_flag(PAGE_HAS_LEAF_BIT)) {
		p.sort_in_mem();
	}

	if (p.is_full()) {
		if (p.is_root()) {  // the root page only 2-split
			page left, right;
			if (curr_seg->assign_page(&left) == sdb::SUCCESS
					&& curr_seg->assign_page(&right) == sdb::SUCCESS) {
				p.sort_in_mem();
				split_2(p, left, right);
			} else {
				return sdb::FAILURE;
			}
		} else {
			if (kt == key_less || kt == key_within_page) {
				p.parent->has_pre_nd2() ? to_left(p) : to_right(p);
			} else if (kt == key_greater) {
				to_left(p);
			}
		}
	}

	return sdb::SUCCESS;
}
int bptree::add_key(page & p, key &k, val& v, key_test_enum kt) {
	node2 n;
	p.assign_node(&n);
	n.set_key_val(k, v);

	if (p.test_flag(PAGE_HAS_LEAF_BIT)) {
		p.sort_in_mem();
	}

	if (p.is_full()) {
		if (p.is_root()) {  // the root page only 2-split
			page left, right;
			if (curr_seg->assign_page(&left) == sdb::SUCCESS
					&& curr_seg->assign_page(&right) == sdb::SUCCESS) {
				p.sort_in_mem();
				split_2(p, left, right);
			} else {
				return sdb::FAILURE;
			}
		} else {
			if (kt == key_less || kt == key_within_page) {
				p.parent->has_pre_nd2() ? to_left(p) : to_right(p);
			} else if (kt == key_greater) {
				to_left(p);
			}
		}
	}
	return sdb::SUCCESS;
}

/*
 * assign page in the data file which bptree lays on.
 *
 * 1) if the bptree's current segment has enough space, assign space for the page
 * at the end of the segment.
 *
 * 2) if current segment doesn't have enough space, assign an index segment first,
 * afterward assign space for the page in the new segment.
 *
 * 3) currently, we DO NOT support global index segment assign, whereas we can not
 * assign an index segment in another data file, instead of try to assign segment
 * in a data file always. or return sdb::FAILURE
 *
 * 4) before assign a page, we scan the current segment to find the deleted page.
 * this manner can improve space usage locally.
 *
 */
int bptree::assign_page(page &p) {
	if (curr_seg->assign_page(&p) == sdb::SUCCESS) {
		return sdb::SUCCESS;
	} else {
		//TODO assign a new seg id
		unsigned long seg_id = 0x7a7a7a7a7a7a7a7a;
		index_segment seg(seg_id);
		if (root_seg->d_file->assgin_segment(seg) == sdb::SUCCESS) {
			auto it = seg_map.insert(
					std::pair<unsigned long, index_segment>(seg_id, seg));
			curr_seg = &(*it.first).second;
			return seg.assign_page(&p);
		}
	}
	return sdb::FAILURE;
}

void bptree::to_right(page &p) {
	page nxt_page;
	fetch_next_page(p, nxt_page);
	if (p.parent->has_nxt_nd2()) {
		node2 n;
		p.parent->nxt_nd2(n);
		nxt_page.parent = &n;
	} else {
		nxt_page.parent = p.parent;
	}

	if (nxt_page.is_full()) {
		page empty;
		if (assign_page(empty) == sdb::SUCCESS) {
			split_2_3(p, nxt_page, empty);
		} else {
			//TODO throw errors
		}
	} else {
		// transfer the greater key of the page to right/next page,
		// the next page lower key and the page greater key become more lower
		int from = (p.header->node_count + nxt_page.header->node_count) / 2;
		int count = p.header->node_count - from;
		transfer(p, from, count, nxt_page);

		// the next page is not sorted after transfer node2
		nxt_page.sort_in_mem();
		node2 mk;
		mk.offset = 0;
		nxt_page.read_node(&mk);
		p.parent->set_key(mk.k);

//		node2 lk = nxt_page.get(0);  // lowest && greatest key
//		std::cout << "lowest key after sort:" << to_int(lk.k.v) << std::endl;
//		node2 gk = nxt_page.get(nxt_page.header->node_count - 1);
//		if (p.parent == nxt_page.parent) {
//			nxt_page.parent->set_key(lk.k);
//		} else {
//			p.parent->set_key(lk.k);
//			nxt_page.parent->set_key(gk.k);
//		}
	}
}

void bptree::to_left(page &p) {
	page pre_page;
	fetch_pre_page(p, pre_page);
	if (p.parent->has_pre_nd2()) {
		node2 n;
		p.parent->pre_nd2(n);
		pre_page.parent = &n;
	} else {
		pre_page.parent = p.parent;
	}

	if (pre_page.is_full()) {
		page empty;
		if (assign_page(empty) == sdb::SUCCESS) {
			split_2_3(pre_page, p, empty);
		} else {
			//TODO throw errors
		}
	} else {
		//lower key to previous page, the first node of the page is change
		// the previous greater key becomes more greater
		int div = (p.header->node_count + pre_page.header->node_count) / 2;
		int count = p.header->node_count - div;
		transfer(p, 0, count, pre_page);
		node2 mk;  // the middle key
		mk.offset = 0;
		p.read_node(&mk);
		pre_page.parent->set_key(mk.k);
	}
}

void bptree::transfer(page &p1, int from, int count, page & p2) {
	p1.sort_in_mem();
	int ns = p1.header->node_size;
	int src_off = ns * from;
	int dest_off = ns * p2.header->node_count;  // the end of page p2
	memcpy(p2.buffer + dest_off, p1.buffer + src_off, count * ns);
	p2.header->node_count += count;
	p1.purge_node(from, count);  // purge node in p1
}

/*
 * to avoid a full page split, re-organize nodes between the full page
 * and its sibling. the process must make sure their parent node2 has
 * correct key.
 *
 */
//void bptree::re_organize(page &full, page &sibling) {
//	int from = (full.header->node_count + sibling.header->node_count) / 2;
//	transfer(full, from, sibling);
//	node2 lk = sibling.get(0);  // lowest && greatest key
//	std::cout << "lowest key after sort:" << to_int(lk.k.v) << std::endl;
//	node2 gk = sibling.get(sibling.header->node_count - 1);
//	if (full.parent == sibling.parent) {
//		sibling.parent->set_key(lk.k);
//	} else {
//		full.parent->set_key(lk.k);
//		sibling.parent->set_key(gk.k);
//	}
//}
bptree::~bptree() {
	root_seg->free_buffer();
	for (auto it = seg_map.begin(); it != seg_map.end(); ++it) {
		(*it).second.free_buffer();
	}
}

/*
 * test the relationship of a key within current non-leaf page node2s. if return
 * 1) key_found, the parameter n references the node2 in the page
 *
 * 2) key_within, the parameter n references the node2 in the page, which is
 *  just greater than the key, its previous node2 is less than the key.
 *
 *  3) key_less, the fist node is greater than the key. the parameter n references
 *  the first node of page.
 *
 *  4) key_greater, the key is greater the last node2 of page, the parameter
 *  n references the last node2.
 */
key_test_enum fixed_size_index_block::test_key(const key & k, node2 & n) {
	if (is_leaf_page()) {
		return key_invalid_test;
	}

	int off;
	int i = 0;
	key_test_enum flag = not_defenition;
	while (i < header->node_count) {
		off = i * header->node_size;
		n.offset = off;
		read_node(&n);

		int t = n.k.compare(k);
		if (t == 0) {
			return key_equals;
		} else if (t > 0) {  // key less
			if (i == 0) {
				return key_less;
			} else if (flag == key_greater) {
				return key_within_page;
			}
			flag = key_less;
		} else if (t < 0) { // key greater
			if (i == header->node_count) {
				return key_greater;
			}
			flag = key_greater;
		}
		i++;
	}

	return flag;
}

int fixed_size_index_block::assign_node(node2 *n) {
	int off = 0;
	for (int i = 0; i < header->node_count; i++) {
		n->ref(buffer + off, off, header->node_size - node2_header_size);
		if (n->test_flag(ND2_DEL_BIT)) {
			n->ref(buffer + off, off, header->node_size - node2_header_size,
					idx_seg->get_tree()->get_key_len(),
					idx_seg->get_tree()->get_val_len());
			n->header->flag = 0;  // reset node2 flag
			n->ref_page = this;
			return sdb::SUCCESS;
		}
	}
	if (header->node_count > idx_seg->get_tree()->get_m()) {
		return NODE2_OVERFLOW_MAX_ORDER;
	}
	off = header->node_count * header->node_size;
	if (length >= off + header->node_size) {
		n->ref(buffer + off, off, header->node_size - node2_header_size,
				idx_seg->get_tree()->get_key_len(),
				idx_seg->get_tree()->get_val_len());
		n->header->flag = 0;
		n->ref_page = this;
		header->node_count++;
		return sdb::SUCCESS;
	} else {
		return NODE2_OVERFLOW_OFFSET;
	}
}

int fixed_size_index_block::read_node(node2 *n) {
	int off = n->offset;
	if (length >= off + header->node_size) {
		if (off % header->node_size == 0) {
			n->ref(buffer + off, off, header->node_size - node2_header_size,
					idx_seg->get_tree()->get_key_len(),
					idx_seg->get_tree()->get_val_len());
			n->ref_page = this;
			return sdb::SUCCESS;
		} else {
			return NODE2_INVALID_OFFSET;
		}
	} else {
		return NODE2_OVERFLOW_OFFSET;
	}
}

void fixed_size_index_block::set_pre_page(fixed_size_index_block *pre) {
//	pre_page_header = pre->header;
//	pre->next_page_header = this->header;
	header->pre_blk_off = pre->offset;
	pre->header->next_blk_off = offset;
	set_flag(PRE_BLK_BIT);
	pre->set_flag(NEXT_BLK_BIT);
	if (idx_seg != pre->idx_seg) {
		set_flag(PRE_BLK_SPAN_SEG_BIT);
		pre->set_flag(NEXT_BLK_SPAN_SEG_BIT);
		if (idx_seg->get_file() != pre->idx_seg->get_file()) {
			set_flag(PRE_BLK_SPAN_DFILE_BIT);
			pre->set_flag(NEXT_BLK_SPAN_DFILE_BIT);
		} else {
			remove_flag(PRE_BLK_SPAN_DFILE_BIT);
			pre->remove_flag(NEXT_BLK_SPAN_DFILE_BIT);
		}
	} else {
		remove_flag(PRE_BLK_SPAN_SEG_BIT);
		pre->remove_flag(NEXT_BLK_SPAN_SEG_BIT);
	}
}

void fixed_size_index_block::set_next_page(fixed_size_index_block *next) {
//	next_page_header = next->header;
//	next->pre_page_header = this->header;
	header->next_blk_off = next->offset;
	next->header->pre_blk_off = offset;
	set_flag(NEXT_BLK_BIT);
	set_flag(PRE_BLK_BIT);
	if (idx_seg != next->idx_seg) {
		set_flag(NEXT_BLK_SPAN_SEG_BIT);
		next->set_flag(PRE_BLK_SPAN_SEG_BIT);
		if (idx_seg->get_file() != next->idx_seg->get_file()) {
			set_flag(NEXT_BLK_SPAN_DFILE_BIT);
			next->set_flag(PRE_BLK_SPAN_DFILE_BIT);
		} else {
			remove_flag(NEXT_BLK_SPAN_DFILE_BIT);
			next->remove_flag(PRE_BLK_SPAN_DFILE_BIT);
		}
	} else {
		remove_flag(NEXT_BLK_SPAN_SEG_BIT);
		next->remove_flag(PRE_BLK_SPAN_SEG_BIT);
	}
}

//void fixed_size_index_block::purge_node() {
//	purge_node(0);
//}

void fixed_size_index_block::purge_node(int idx) {
	node2 n;
	for (int i = idx; i < header->node_count; i++) {
		n.offset = i * header->node_size;
		read_node(&n);
		n.set_flag(ND2_DEL_BIT);
	}
	header->node_count = idx;
}

void fixed_size_index_block::purge_node(int idx, int count) {
	int nc = header->node_count;
	int ns = header->node_size;
	int dest = idx * ns;
	int from = dest + count * ns;
	int len = ns * (nc - count - idx);
	memmove(buffer + dest, buffer + from, len);
	header->node_count -= count;
}

//node2 fixed_size_index_block::get(int i) {
//	node2 n;
//	n.offset = i * header->node_size;
//	read_node(&n);
//	return n;
//}

bool fixed_size_index_block::is_left() {
	return offset == parent->header->left_pg_off;
}

void fixed_size_index_block::print_all() {
	node2 n;
	for (int i = 0; i < header->node_count; i++) {
		n.offset = header->node_size * i;
		read_node(&n);
		std::cout << "node(" << i << ").key:" << to_int(n.k.v) << std::endl;
	}
}

bool fixed_size_index_block::is_right() {
	return offset == parent->header->right_pg_off;
}

bool fixed_size_index_block::is_full() {
	return header->node_count >= idx_seg->get_tree()->get_m()
			|| length - header->node_count * header->node_size
					< header->node_size;
}

int fixed_size_index_block::pre_page(fixed_size_index_block &p) {
	if (test_flag(PRE_BLK_BIT)) {
		p.offset = header->pre_blk_off;
		return sdb::SUCCESS;
	}
	return NO_PRE_BLK;
}
int fixed_size_index_block::next_page(fixed_size_index_block &p) {
	if (test_flag(NEXT_BLK_BIT)) {
		p.offset = header->next_blk_off;
		return sdb::SUCCESS;
	}
	return NO_NEXT_BLK;

}
int fixed_size_index_block::parent_page(fixed_size_index_block &p) {
	if (test_flag(PAGE_PP_BIT)) {
		p.offset = header->parent_blk_off;
	}
	return NO_PARENT_BLK;
}

/*
 * TODO CAUTION, sometimes the page's referencing segment free buffer first.
 * in the situation the ref_flag is false and result in in-correct de-const
 * -truct action.
 *
 * the current solution comment the code in the de-constructor.
 * this type of case is found in data_block, key and val struct
 */
fixed_size_index_block::~fixed_size_index_block() {
//	if (!ref_flag) {
//		delete header;
//		delete[] buffer;
//	}
}
//
//void swap_node2(node2& n1, node2& n2) {
//	node2 t = n2;
//	n2 = n1;
//	n1 = t;
//}

void swap_buff(char *buff1, char *buff2, char *tmp, int len) {
	memcpy(tmp, buff1, len);
	memcpy(buff1, buff2, len);
	memcpy(buff2, tmp, len);
}

/*
 * insert sort in memory
 */
void fixed_size_index_block::sort_in_mem() {
	short int ns = header->node_size;
	char * tmp = new char[ns];
	for (int i = 0; i < header->node_count; i++) {
		for (int j = i - 1; j >= 0; j--) {
			node2 a;
			a.offset = (j + 1) * ns;
			read_node(&a);
			if (a.test_flag(ND2_DEL_BIT))
				continue;

			node2 b;
			b.offset = j * ns;
			read_node(&b);
			if (b.test_flag(ND2_DEL_BIT))
				continue;
			if (a < b) {
				char *buff_a = buffer + a.offset;
				char *buff_b = buffer + b.offset;
				swap_buff(buff_a, buff_b, tmp, ns);
			}
		}
	}
	delete[] tmp;
}

void fixed_size_index_block::set_parent_node2(node2 *n) {
	parent = n;
//	parent_page_header = n->ref_page->header;
	header->parent_blk_off = n->ref_page->offset;
	header->parent_nd2_off = n->offset;
	set_flag(PAGE_PP_BIT);

	if (idx_seg != n->ref_page->idx_seg) {
		set_flag(PAGE_PP_SEG_BIT);
		if (idx_seg->get_file() != n->ref_page->idx_seg->get_file()) {
			set_flag(PAGE_PP_DF_BIT);
		} else {
			remove_flag(PAGE_PP_DF_BIT);
		}
	} else {
		remove_flag(PAGE_PP_SEG_BIT);
		remove_flag(PAGE_PP_DF_BIT);
	}
}

void node2::set_left(page *p) {
	p->set_parent_node2(this);
//	left_page = (*p);
	header->left_pg_off = p->offset;
	set_flag(ND2_LP_BIT);

	if (test_flag(ND2_RP_BIT)) {
		page rp;
		ref_page->idx_seg->fetch_right_page(*this, rp);
		p->set_next_page(&rp);
	}

	if (has_pre_nd2()) {
		node2 pre_node;
		pre_nd2(pre_node);
		if (pre_node.test_flag(ND2_RP_BIT)) {
			page pre_right_page;
			pre_node.ref_page->idx_seg->fetch_right_page(pre_node,
					pre_right_page);
			p->set_pre_page(&pre_right_page);
		}
	}
}

void node2::set_right(page * p) {
	p->set_parent_node2(this);
//	right_page = *p;
	header->right_pg_off = p->offset;
	set_flag(ND2_RP_BIT);

	if (test_flag(ND2_LP_BIT)) {
		page lp;
		ref_page->idx_seg->fetch_left_page(*this, lp);
		p->set_pre_page(&lp);
	}

	if (has_nxt_nd2()) {
		node2 n;
		nxt_nd2(n);
		if (n.test_flag(ND2_LP_BIT)) {
			page nxt_left_page;
			ref_page->idx_seg->fetch_left_page(n, nxt_left_page);
			p->set_next_page(&nxt_left_page);
		}
	}
}

void node2::pre_nd2(node2 &pre) {
	int ns = ref_page->header->node_size;
	int nc = ref_page->header->node_count;
	int kl = ref_page->idx_seg->get_tree()->get_key_len();
	int vl = ref_page->idx_seg->get_tree()->get_val_len();
	pre.ref(buffer - ns, offset - ns, nc - node2_header_size, kl, vl);
	pre.ref_page = ref_page;

}
void node2::nxt_nd2(node2 &nxt) {
	int ns = ref_page->header->node_size;
	int nc = ref_page->header->node_count;
	int kl = ref_page->idx_seg->get_tree()->get_key_len();
	int vl = ref_page->idx_seg->get_tree()->get_val_len();
	nxt.ref(buffer + ns, offset + ns, nc - node2_header_size, kl, vl);
	nxt.ref_page = ref_page;
}

bool node2::operator==(const node2 &other) {
	if (this == &other) {
		return true;
	}
	if (offset == other.offset && length == other.length) {
		if (header == other.header) {
			return true;
		} else if (other.header != nullptr && header != nullptr) {
			return header->flag == other.header->flag
					&& header->left_pg_off == other.header->left_pg_off
					&& header->right_pg_off == other.header->right_pg_off;
		} else {
			return false;
		}
	}
}

index_segment::index_segment(unsigned long _id) :
		segment(_id) {
	seg_type = sdb::storage::index_segment_type;
}

index_segment::index_segment(unsigned long _id, data_file *_f,
		unsigned int _off) :
		segment(_id, _f, _off) {
	seg_type = sdb::storage::index_segment_type;
}

index_segment::index_segment(const index_segment & another) :
		segment(another) {
	seg_type = sdb::storage::index_segment_type;
}

index_segment::index_segment(segment & other) :
		segment(other) {
	seg_type = sdb::storage::index_segment_type;
}

index_segment::index_segment(index_segment && another) {
	magic = another.magic;
	id = another.id;
	status = another.status;
	length = another.length;
	seg_type = another.seg_type;
	offset = another.offset;
	next_seg_offset = another.next_seg_offset;
	pre_seg_offset = another.pre_seg_offset;

	assign_time = another.assign_time;
	create_time = another.create_time;
	update_time = another.update_time;

	block_size = another.block_size;
	block_count = another.block_count;

	pre_seg_dfile_id = another.pre_seg_dfile_id;
	pre_seg_dfile_path = another.pre_seg_dfile_path;
	next_seg_dfile_id = another.next_seg_dfile_id;
	next_seg_dfile_path = another.next_seg_dfile_path;

//move another content buffer to this
	content_buffer = another.content_buffer;
	another.content_buffer = nullptr;
}

index_segment & index_segment::operator=(index_segment & another) {
	if (this != &another) {
		magic = another.magic;
		id = another.id;
		status = another.status;
		length = another.length;
		seg_type = another.seg_type;
		offset = another.offset;
		next_seg_offset = another.next_seg_offset;
		pre_seg_offset = another.pre_seg_offset;

		assign_time = another.assign_time;
		create_time = another.create_time;
		update_time = another.update_time;

		block_size = another.block_size;
		block_count = another.block_count;

		pre_seg_dfile_id = another.pre_seg_dfile_id;
		pre_seg_dfile_path = another.pre_seg_dfile_path;
		next_seg_dfile_id = another.next_seg_dfile_id;
		next_seg_dfile_path = another.next_seg_dfile_path;

		//move another content buffer to this
		content_buffer = another.content_buffer;
		another.content_buffer = nullptr;
	}
	return *this;
}

int index_segment::assign_page(page *p) {

// assign to p if found a deleted flag page
	for (int i = 0; i < block_count; i++) {
		int off = block_size * block_count;
		p->ref(off, content_buffer + off,
				block_size - sdb::tree::index_block_header_size);
		if (p->header->blk_magic == block_magic
				&& p->test_flag(REMOVED_BLK_BIT)) {
			p->header->flag = 0; // reset header flag
			p->init_header();
			p->idx_seg = this;
			time(&p->header->assign_time);
			p->header->node_size = node2_header_size + tree->key_len
					+ tree->val_len;

			return sdb::SUCCESS;
		}
	}

//assign rest of buffer
	int r = length - segment_head_size - block_size * block_count;
	if (r > block_size) {
		int off = block_size * block_count;
		if (has_buffer()) {
			p->ref(off, content_buffer + off,
					block_size - sdb::tree::index_block_header_size);
			p->init_header();
		} else {
			p->init(off, block_size - sdb::tree::index_block_header_size);
		}
		p->idx_seg = this;
		time(&p->header->assign_time);
		p->header->node_size = node2_header_size + tree->key_len
				+ tree->val_len;
		block_count++;
		return sdb::SUCCESS;
	} else {
		return SEGMENT_IS_FULL;
	}
}

int index_segment::read_page(page *p) {
	int off = p->offset;
	if (off % block_size == 0 && off + block_size <= length) {
		p->ref(off, content_buffer + off, block_size - index_block_header_size);
		p->idx_seg = this;
		return sdb::SUCCESS;
	} else {
		return BLK_OFF_INVALID;
	}
}

int index_segment::fetch_left_page(node2 &n, page & lp) {
	return tree->fetch_left_page(n, lp);
}
int index_segment::fetch_right_page(node2 &n, page & rp) {
	return tree->fetch_right_page(n, rp);
}
int index_segment::fetch_parent_page(node2 &n, page &pp) {
	return tree->fetch_parent_page(n, pp);
}

int index_segment::fetch_next_page(page &p, page &nxt) {
	return tree->fetch_next_page(p, nxt);
}
int index_segment::fetch_pre_page(page &p, page &pre) {
	return tree->fetch_pre_page(p, pre);
}

} /* namespace tree */
} /* namespace sdb */
