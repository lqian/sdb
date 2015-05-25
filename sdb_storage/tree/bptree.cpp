/*
 * bp.cpp
 *
 *  Created on: May 12, 2015
 *      Author: linkqian
 */

#include "bptree.h"

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

int bptree::fetch_left_page(node2 &n, page & lp) {
	int off = n.header->left_pg_off;
	lp.offset = off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_LP_SEG_BIT)) {
		segment seg;
		if (ref_seg->next_seg(seg)) {
			index_segment idx_seg(seg);
			return idx_seg.read_page(&lp); // initialized the pointer
		}
	} else {
		return n.ref_page->idx_seg->read_page(&lp);
	}
}

int bptree::fetch_right_page(node2 &n, page & rp) {
	int off = n.header->right_pg_off;
	rp.offset = off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_RP_SEG_BIT) || n.test_flag(ND2_RP_DF_BIT)) {
		segment seg;
		if (ref_seg->next_seg(seg)) {
			index_segment idx_seg(seg);
			return idx_seg.read_page(&rp);
		}
	} else {
		return n.ref_page->idx_seg->read_page(&rp);
	}
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
	return sdb::FAILURE;
}

page & bptree::find_page(page & p, const key &k) {
	if (p.is_leaf_page()) {
		return p;
	}
	node2 n;
	int t = p.test_key(k, n);

	// non-leaf pages
	if (t == key_within_page) {
		if (n.test_flag(ND2_LP_BIT)) {
			return find_page(n.left_page, k);
		}
	} else if (t == key_less) {
		if (n.test_flag(ND2_LP_BIT)) {
			return find_page(n.left_page, k);
		}
	} else if (t == key_greater) {
		if (n.test_flag(ND2_RP_BIT)) {
			return find_page(n.right_page, k);
		}
	} else if (t == key_found) {
		if (n.test_flag(ND2_RP_BIT)) {
			return find_page(n.right_page, k);
		}
	}
}

int bptree::add_key(key &k, val& v) {
	return this->add_key(root_page, k, v);
}

void bptree::split_2(page &p, page &left, page &right) {
	std::cout << "-----p.sort_in_mem();" << std::endl;
	p.sort_in_mem();
	std::cout << "p.sort_in_mem();" << std::endl;
	int h = p.header->node_count / 2 + 1;
	int h_off = h * p.header->node_size;

	memcpy(left.buffer, p.buffer, h_off);
	memcpy(right.buffer, p.buffer, root_seg->length - h_off);

	node2 t; // the middle key
	p.read_node(&t);
	key mk;
	mk.set_val(t.k.v, t.k.len);

	p.purge_node();
	p.set_flag(PAGE_NODE_TYPE_BIT); //  set the page flag has children page
	node2 nn;  // the new first node of root page
	p.assign_node(&nn);
	nn.set_key(mk);

	// bind left and right page for node2, bind previous and next page for children
	nn.set_left(&left);
	nn.set_right(&right);
	left.set_next_page(&right);
}
void bptree::split_2_3(page &p1, page &p2, page & target) {
	p1.sort_in_mem();
	p2.sort_in_mem();

	int c1 = p1.header->node_count;
	int c2 = p2.header->node_count;
	int p = (c1 + c2) / 3 + 1;
	int f1 = c1 - p;
	int f2 = c2 - p;

	transfer(p1, f1, target);
	transfer(p2, f2, target);
}

int bptree::add_key(page & p, key &k, val& v) {
	if (p.is_leaf_page()) {
		if (p.is_full()) {
			if (p.is_root()) {  // the root page only 2-split
				page left, right;
				split_2(p, left, right);
			} else {
				if (p.is_left()) {
					p.parent->has_pre_nd2() ? to_left(p) : to_right(p);
				} else if (p.is_right()) {
					to_left(p);
				}
			}
		} else {  // node is not full, add key/val directly
			node2 n;
			p.assign_node(&n);
			n.set_key_val(k, v);
			return sdb::SUCCESS;
		}
	} else {
		node2 n;
		int c = p.test_key(k, n);
		switch (c) {
		case key_less:
			return add_key(n.left_page, k, v);
		case key_greater:
			return add_key(n.right_page, k, v);
		case key_within_page:
			return add_key(n.left_page, k, v);
		default:
			//TODO current do not permit duplicated key
			return sdb::FAILURE;
		}
	}
	return sdb::FAILURE;
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
	if (nxt_page.is_full()) {
		page empty;
		if (assign_page(empty)) {
			split_2_3(p, nxt_page, empty);
		} else {
			//TODO throw errors
		}
	} else {
		re_organize(p, nxt_page);
	}
}

void bptree::to_left(page &p) {
	page pre_page;
	fetch_pre_page(p, pre_page);
	if (pre_page.is_full()) {
		page empty;
		if (assign_page(empty)) {
			split_2_3(pre_page, p, empty);
		} else {
			//TODO throw errors
		}
	} else {
		re_organize(pre_page, p);
	}
}

void bptree::transfer(page &p1, int from, page & p2) {
	p1.sort_in_mem();
	p2.sort_in_mem();

	int trans_size = p1.header->node_count - from;
	p2.header->node_count += trans_size;
	int src_off = p1.header->node_size * from;
	int dest_off = p2.header->node_size * p1.header->node_count;
	memcpy(p1.buffer + dest_off, p1.buffer + src_off,
			trans_size * p2.header->node_size);

	p1.purge_node(from);

}

void bptree::re_organize(page &full, page &sibling) {
	int from = full.header->node_count + sibling.header->node_count / 2 + 1;
	transfer(full, from, sibling);
}

bptree::~bptree() {
	root_seg->free_buffer();
	for (auto it = seg_map.begin(); it != seg_map.end(); ++it) {
		(*it).second.free_buffer();
	}
}

/*
 * test the relationship of a key within current non-leaf page node2s. if return
 * 1) key_found, the parameter n references the node2 in the page
 * 2) key_within, the parameter n references the node2 in the page, which is
 *  just greater than the key, its previous node2 is less than the key.
 *
 *  3) key_less, the fist node is greater than the key. the parameter references
 *  the first node of page.
 *
 *  4) key_greater, the key is greater the last node2 of page, the parameter
 *  references the last node2.
 */
key_test_numu fixed_size_index_block::test_key(const key & k, node2 & n) {
	if (is_leaf_page()) {
		return sdb::tree::invalid_test;
	}

	int kl = idx_seg->get_tree()->get_key_len();
	int vl = idx_seg->get_tree()->get_val_len();
	int len = header->node_size - node2_header_size;

	int off;
	int i = 0;
	key_test_numu flag;
	do {
		off = i * header->node_size;
		i++;
		n.ref(buffer + off, off, len, kl, vl);
		n.ref_page = this;
		int t = n.k.compare(k);
		if (t == 0) {
			return key_found;
		} else if (t > 0) {  // node greater
			if (i == 0) {
				return key_less;
			} else if (flag == key_greater) {
				return key_within_page;
			}
			flag = key_less;
		} else if (t < 0) { // node less
			if (i == header->node_count - 1) {
				return key_greater;
			} else if (flag == key_less) {
				return key_within_page;
			}
			flag = key_greater;
		}
	} while (i < header->node_count);
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
			return sdb::SUCCESS;
		} else {
			return NODE2_INVALID_OFFSET;
		}
	} else {
		return NODE2_OVERFLOW_OFFSET;
	}
}

void fixed_size_index_block::set_pre_page(fixed_size_index_block *pre) {
	pre_page_header = pre->header;
	pre->next_page_header = this->header;
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
	next_page_header = next->header;
	next->pre_page_header = this->header;
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
	char * buff = buffer;
	int off = 0;
	for (int i = idx; i < header->node_count; i++) {
		n.ref(buff, off, header->node_size - index_block_header_size);
		n.set_flag(ND2_DEL_BIT);
		off += header->node_size;
		buff += header->node_size;
	}
	header->node_count = idx;
}

bool fixed_size_index_block::is_left() {
	return offset == parent->header->left_pg_off;
}

bool fixed_size_index_block::is_right() {
	return offset == parent->header->right_pg_off;
}

bool fixed_size_index_block::is_full() {
	return header->node_count >= idx_seg->get_tree()->get_m()
			|| length - header->node_count * header->node_size
					< header->node_size;
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

void swap_node2(node2& n1, node2& n2) {
	node2 t = n2;
	n2 = n1;
	n1 = t;
}

/*
 * insert sort in memory
 */
void fixed_size_index_block::sort_in_mem() {
	int off_a = 0;
	for (int i = 0; i < header->node_count; i++) {
		node2 a;
		a.ref(buffer + off_a, off_a, header->node_size - node2_header_size,
				idx_seg->get_tree()->get_key_len(),
				idx_seg->get_tree()->get_val_len());
		if (a.test_flag(ND2_DEL_BIT))
			continue;
		for (int j = i + 1; j > 1; j--) {
			node2 b;
			int off_b = j * header->node_size;
			a.ref(buffer + off_b, off_b, header->node_size - node2_header_size,
					idx_seg->get_tree()->get_key_len(),
					idx_seg->get_tree()->get_val_len());
			if (b.test_flag(ND2_DEL_BIT))
				continue;
			if (a < b) {
				swap(a, b);
			}
		}
		off_a += header->node_size;
	}
}

void fixed_size_index_block::set_parent_node2(node2 *n) {
	parent = n;
	parent_page_header = n->ref_page->header;
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
	}
}

void node2::set_left(page *p) {
	p->set_parent_node2(this);
	left_page = (*p);
	header->left_pg_off = p->offset;
	set_flag(ND2_LP_BIT);

	if (test_flag(ND2_RP_BIT)) {
		p->set_next_page(&right_page);
	}
}

void node2::set_right(page * p) {
	p->set_parent_node2(this);
	right_page = *p;
	header->right_pg_off = p->offset;
	set_flag(ND2_RP_BIT);

	if (test_flag(ND2_LP_BIT)) {
		p->set_pre_page(&left_page);
	}
}

node2 node2::pre_nd2() {
	int ns = ref_page->header->node_size;
	int nc = ref_page->header->node_count;
	int kl = ref_page->idx_seg->get_tree()->get_key_len();
	int vl = ref_page->idx_seg->get_tree()->get_val_len();
	node2 pre;
	pre.ref(buffer - ns, offset - ns, nc - node2_header_size, kl, vl);
	return pre;

}
node2 node2::nxt_nd2() {
	int ns = ref_page->header->node_size;
	int nc = ref_page->header->node_count;
	int kl = ref_page->idx_seg->get_tree()->get_key_len();
	int vl = ref_page->idx_seg->get_tree()->get_val_len();
	node2 nxt;
	nxt.ref(buffer + ns, offset + ns, nc - node2_header_size, kl, vl);
	return nxt;
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
		p->ref(off, content_buffer + off,
				block_size - sdb::tree::index_block_header_size);
		p->idx_seg = this;
		return sdb::SUCCESS;
	} else {
		return BLK_OFF_INVALID;
	}
}

} /* namespace tree */
} /* namespace sdb */
