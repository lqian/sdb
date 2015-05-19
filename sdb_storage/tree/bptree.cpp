/*
 * bp.cpp
 *
 *  Created on: May 12, 2015
 *      Author: linkqian
 */

#include "bptree.h"

namespace sdb {
namespace tree {

bptree::bptree(index_segment * _root_seg, int kl, int vl, int bs) {
	this->root_seg = _root_seg;
	this->root_seg->tree = this;
	this->key_len = kl;
	this->val_len = vl;
	this->block_size = bs * kilo_byte;
	this->m = (block_size - block_header_size)
			/ (key_len + val_len + node2_header_size);
}

bptree::~bptree() {

}

int fixed_size_index_block::assign_node(node2 *n) {
	int off = header->node_count * header->node_size;
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
	left_page = p;
	header->left_pg_off = p->offset;
	set_flag(ND2_LP_BIT);

	if (pre_nd2 != nullptr && pre_nd2->test_flag(ND2_RP_BIT)) {
		p->set_pre_page(pre_nd2->right_page);
	}

	if (test_flag(ND2_RP_BIT) && right_page != nullptr) {
		p->set_next_page(right_page);
	}
}

void node2::set_right(page * p) {
	p->set_parent_node2(this);
	right_page = p;
	header->right_pg_off = p->offset;
	set_flag(ND2_RP_BIT);

	if (test_flag(ND2_LP_BIT) && left_page != nullptr) {
		p->set_pre_page(left_page);
	}

	if (nxt_nd2 != nullptr && nxt_nd2->test_flag(ND2_LP_BIT)) {
		p->set_next_page(nxt_nd2->left_page);
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
int index_segment::assign_page(page *p) {
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

int index_segment::read_page(page &p) {
	int off = p.offset;
	if (off % block_size == 0 && off + block_size <= length) {
		p.ref(off, content_buffer+off, block_size - sdb::tree::index_block_header_size);
		p.idx_seg = this;
		return sdb::SUCCESS;
	} else {
		return BLK_OFF_INVALID;
	}
}

} /* namespace tree */
} /* namespace sdb */
