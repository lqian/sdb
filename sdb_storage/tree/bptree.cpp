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
	this->key_len = kl;
	this->val_len = vl;
	this->block_size = bs * kilo_byte;
	this->m = (block_size - block_header_size)
			/ (key_len + val_len + node2_header_size);
}

int bptree::fetch_left_page(node2 &n) {
	int off = n.header->left_pg_off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_LP_SEG_BIT)) {
		segment seg = ref_seg->next_seg();
		index_segment idx_seg(seg);

		if (!idx_seg.has_buffer()) {
			if (idx_seg.get_file() == ref_seg->get_file()) {
				if (ref_seg->get_file()->fetch_segment(idx_seg)) {
					return idx_seg.read_page(&n.left_page);  // initialized the pointer
				}
			} else {
				//TODO open another file and fetch buffer
				return sdb::FAILURE;
			}
		}
	} else {
		n.left_page.offset = off;
		return n.ref_page->idx_seg->read_page(&n.left_page);
	}
}

int bptree::fetch_right_page(node2 &n) {
	int off = n.header->right_pg_off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.test_flag(ND2_RP_SEG_BIT)) {
		segment seg = ref_seg->next_seg();
		index_segment idx_seg(seg);

		if (!idx_seg.has_buffer()) {
			if (idx_seg.get_file() == ref_seg->get_file()) {
				if (ref_seg->get_file()->fetch_segment(idx_seg)) {
					return idx_seg.read_page(&n.right_page);
				}
			} else {
				//TODO open another file and fetch buffer
				return sdb::FAILURE;
			}
		}
	} else {
		n.left_page.offset = off;
		return n.ref_page->idx_seg->read_page(&n.right_page);
	}
}

int bptree::fetch_parent_page(node2 &n) {
	int off = n.ref_page->header->parent_blk_off;
	index_segment *ref_seg = n.ref_page->idx_seg;
	if (n.ref_page->test_flag(PAGE_PP_SEG_BIT)) {
		segment seg = ref_seg->next_seg();
		index_segment idx_seg(seg);
		if (idx_seg.get_file() == ref_seg->get_file()) {
			if (idx_seg.get_file()->fetch_segment(idx_seg)) {
				return idx_seg.read_page(n.ref_page->parent_page);
			}
		}
		return sdb::FAILURE;
	} else {
		n.ref_page->parent_page->offset = off; //TODO not initialized pointer
		n.ref_page->idx_seg->read_page(n.ref_page->parent_page);
	}

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
	if (root_page == nullptr) {
		root_seg->assign_page(root_page);
	}

	page *p = root_page;
	for (;;) {

	}

	if (!root_page->is_leaf_page()) {  // has child
		page left, right;

	} else if (!root_page->is_full()) {
		node2 n;
		root_page->assign_node(&n);
		n.set_key_val(k, v);
	}
}

int bptree::add_key(page * p, key &k, val& v) {
	if (p->is_leaf_page()) {
		if (p->is_full()) {
			if (p->is_root()) {  // the root page only 2-split
				page left, right;
				root_seg->assign_page(&left);
				root_seg->assign_page(&right);
				int h = root_page->header->node_count / 2 + 1;
				int h_off = h * root_page->header->node_size;
				memcpy(left.buffer, root_page->buffer, h);
				memcpy(right.buffer, root_page->buffer,
						root_seg->length - h_off);

				node2 t; // the middle key
				root_page->read_node(&t);
				key mk;
				mk.set_val(t.k.v, t.k.len);

				root_page->purge_node();
				root_page->set_flag(PAGE_NODE_TYPE_BIT); //  set the page flag has children page
//				root_page->header->node_size = index_block_header_size + k.len;
				node2 nn;  // the new first node of root page
				root_page->assign_node(&nn);
				nn.set_key(mk);

				// bind left and right page for node2, bind previous and next page for children
				nn.set_left(&left);
				nn.set_right(&right);
				left.set_next_page(&right);

			} else {  // 2-3 split

			}
		} else {
			node2 n;
			root_page->assign_node(&n);
			n.set_key_val(k, v);
		}
	} else {
		node2 n;
		int c = p->test_key(k, n);
		switch (c) {
		case key_less:
			return add_key(&n.left_page, k, v);
		case key_greater:
			return add_key(&n.right_page, k, v);
		case key_within_page:
			return add_key(&n.left_page, k, v);
		default:
			//TODO current do not permit duplicated key
			return sdb::FAILURE;
			break;
		}
	}

	return sdb::FAILURE;
}

bptree::~bptree() {

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

void fixed_size_index_block::assign_node(node2 & n) {
	int off = header->node_count * header->node_size;
	n.ref(buffer + off, off, header->node_size - node2_header_size,
			idx_seg->get_tree()->get_key_len(),
			idx_seg->get_tree()->get_val_len());
	header->node_count++;
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
	pre_page = pre;
	pre->next_page = this;
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
	next_page = next;
	next->pre_page = this;
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

void fixed_size_index_block::purge_node() {
	node2 n;
	char * buff = buffer;
	int off = 0;
	for (int i = 0; i < header->node_count; i++) {
		n.ref(buff, off, header->node_size - index_block_header_size);
		n.set_flag(ND2_DEL_BIT);
		off += header->node_size;
		buff += header->node_size;
	}
	header->node_count = 0;
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
	std::vector<node2> nodes;
	int off_a = 0;
	for (int i = 0; i < header->node_count; i++) {
		node2 a;
		a.ref(buffer + off_a, off_a, header->node_size - node2_header_size,
				idx_seg->get_tree()->get_key_len(),
				idx_seg->get_tree()->get_val_len());
		for (int j = i + 1; j > 1; j--) {
			node2 b;
			int off_b = j * header->node_size;
			a.ref(buffer + off_b, off_b, header->node_size - node2_header_size,
					idx_seg->get_tree()->get_key_len(),
					idx_seg->get_tree()->get_val_len());

			if (a < b) {
				swap(a, b);
			}
		}
		off_a += header->node_size;
	}
}

void fixed_size_index_block::set_parent_node2(node2 *n) {
	parent = n;
	parent_page = n->ref_page;
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

	if (pre_nd2 != nullptr && pre_nd2->test_flag(ND2_RP_BIT)) {
		p->set_pre_page(&pre_nd2->right_page);
	}

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

	if (nxt_nd2 != nullptr && nxt_nd2->test_flag(ND2_LP_BIT)) {
		p->set_next_page(&nxt_nd2->left_page);
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

	std::cout << "std::move " <<std::endl;
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
		if (p->test_flag(REMOVED_BLK_BIT)) {
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
