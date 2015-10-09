/*
 * a disk B+Tree read a disk block which contains multiple pages from segment.
 * a page contains multiple node2. a node2 has left page and right page. node2
 * key within the left page is less than the page's parent node2's key. meanwhile
 * node2 key within the right page is equal or greater than the page's parent
 * node2 key. a page has previous page pointed a page in which node2 key is less
 * than the first node2. and a next page that pointer a page in which node2 is
 * greater than the last node2.
 *
 * in the disk B+Tree, a block contains the same hierarchical pages, maybe the
 * same hierarchical pages span multiple blocks.
 *
 * when insert a node2 to a page, it DOES NOT require node2 in page is sorted.
 * whereas avoid disk node moving. but sorted node2 page is a optimized page.
 * these node2 were sorted after a page are load into memory.
 *
 * a segment stores multi-page-blocks was called index-segment. multi-page-block
 * is fixed size that stores block-head and variant fixed-size page that stores
 * page-head and variant fixed-size node2.
 *
 * a non-root multi-page-block internal:
 *
 * +------+---------+---------+---------+---------+---------+
 * | 	  |			|		  | 		|		  |		    |
 * | HEAD |  PAGE1  |  PAGE2  |  PAGE3  |   ...   | UN-USED |
 * |      |         |         |         |         |         |
 * +------+---------+---------+---------+---------+---------+
 *
 *
 * when create a disk B+Tree, it should denote the key length, value length,
 * block size and max node2s in a page. after create a b+tree, its data stores
 * in buffer instead of stores in disk it should invoke update and flush methods
 * to write index data to disk.
 *
 * the first block of the first segment of an index is the root page, which
 * contains only one page.
 *
 *  Created on: May 12, 2015
 *      Author: Link Qian
 */

#ifndef BPTREE_H_
#define BPTREE_H_

#include <list>
#include <cstring>
#include <map>
#include "node.h"
#include "../common/char_buffer.h"
#include "../storage/datafile.h"

namespace sdb {
namespace tree {

const int storage_page_head_size = 8;
const int index_block_header_size = 72;
const int node2_header_size = 10;

/*
 * page head flag bits definition the 0 ~ 7 bit see datafile.h definition
 */
const int PAGE_HAS_LEAF_BIT = 8; /* when the bit is 0 indicates the page contains leaf node,
 else contains  internal node */
const int PAGE_PP_BIT = 9;
const int PAGE_PP_SEG_BIT = 10; /* parent page's segment is another */
const int PAGE_PP_DF_BIT = 11; /* parent page in another data file */

/* node 2 head flag bits definition */
const int ND2_DEL_BIT = 0;
const int ND2_PD_BIT = 1;
const int ND2_LP_BIT = 2;
const int ND2_RP_BIT = 3;
const int ND2_LP_SEG_BIT = 4;
const int ND2_RP_SEG_BIT = 5;
const int ND2_LP_DF_BIT = 6;
const int ND2_RP_DF_BIT = 7;
const int ND2_PP_SEG_BIT = 8;
const int ND2_PP_DF_BIT = 9;

const int NODE2_OVERFLOW_OFFSET = -2;
const int NODE2_INVALID_OFFSET = -3;
const int NODE2_REF_INVALID_BLOCK = -4;
const int NODE2_OVERFLOW_MAX_ORDER = -5;
const int PAGE_NO_PARENT_PAGE = -100;
const int KEY_NOT_FOUND = -200;
const int KEY_NOT_IN_RANGE = -201;
const int BORROW_NOT_SUPPORT = -300;

using namespace sdb::storage;

enum key_test_enum {
	key_invalid_test = 0,
	key_equal,
	key_less,
	key_greater,
	key_within_page = 0x10,
	not_defenition
};

enum scan_type {
	scan_equal = 1,
	scan_less = 2,
	scan_greater = 4,
	scan_less_equal = scan_equal | scan_less,
	scan_greater_equal = scan_equal | scan_greater
};

struct node2;

class index_segment;

struct val {
	bool ref = false;
	short len = 0;
	char * v = nullptr;

	void set_val(char *buff, short l) {
		memcpy(v, buff, l);
		len = l;
	}

	void ref_val(char *buff, short l) {
		v = buff;
		len = l;
		ref = true;
	}

	bool is_empty() {
		return len == 0;
	}

	~val() {
//		if (!ref && len > 0) {
//			delete[] v;
//		}
	}
};

/*
 * the key struct define a key contains a special length of char array.
 *
 * also, the compare function define the key comparator with another key.
 * return a int value >0 means that this key is greater than another key. 0 means that
 * they equal. <0 mean that this key is less than another key.
 *
 * the key struct uses memcmp as the default comparator. the sub-class should implement
 * its own compare method if its comparator does not match the memory comparator.
 */
struct key: val {
	virtual int compare(const key & other) {
		return memcmp(v, other.v, len);
	}

//	virtual int compare(const key * other) {
//		return memcmp(v, other->v, len);
//	}

	virtual ~key() {

	}
}
;

/*
 * a block contains fixed-size of node2. its internal as
 * +------+---+---+---+----------------------------+------+------+------+
 * |      |   |   |   |                                                 |
 * |      | N | N | N |                                                 |
 * | ROW  | O | O | O |            UN-UTILIZAION SPACE                  |
 * | HEAD | D | D | D |                                                 |
 * |      | 1 | 2 | 3 |                                                 |
 * +------+---+---+---+----------------------------+------+------+------+
 *
 * a index space respects its key length and value length, when create a
 * fixed-size index. all node2 of the index has the same length.
 *
 * in order to operate on an index block, must load it from disk. any operation
 * only store in memory. outer side must flush the block to disk before close it
 * or evict it from memory.
 *
 */
struct fixed_size_index_block: data_block {
	struct head: data_block::head {
		short node_count = 0; /* node count */
		short node_size; /* node size */
		unsigned long parent_seg_id;
		int parent_blk_off; /* parent block offset within a segment */
		short parent_nd2_off; /* parent node2 offset of parent block if the block has parent node*/
	}* header;

	//define programmatic stage
	node2 * parent;
//	fixed_size_index_block * pre_page;
//	fixed_size_index_block * next_page;
//	fixed_size_index_block * parent_page;

//	head * pre_page_header;
//	head * next_page_header;
//	head * parent_page_header;

	index_segment* idx_seg;

	virtual void ref(int off, char *buff, short len) {
		offset = off;
		length = len;
		header = (head *) buff;
		buffer = buff + index_block_header_size;
		ref_flag = true;
		u_off_start = 0;
		u_off_end = 0;
	}

	virtual void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		header->node_count = 0;
		time(&header->create_time);
	}

	bool operator==(const fixed_size_index_block & other);
	bool operator!=(const fixed_size_index_block & other);

	bool less_half_full();

	int assign_node(node2 *n);
	int read_node(node2 *n);
	void set_pre_page(fixed_size_index_block *pre);
	int pre_page(fixed_size_index_block &p);
	int next_page(fixed_size_index_block &p);
	int parent_page(fixed_size_index_block &p);
	void set_next_page(fixed_size_index_block *next);
	void set_parent_node2(node2 *n);
	int find(const key &k, node2 &n);
//	void purge_node();

	/*
	 * purge node from a give index of node2 in the page
	 */
	void purge_node(int idx = 0);
	void purge_node(int idx, int len);
	void delete_node(node2 &n);
	void sort_in_mem();
	key_test_enum test_key(const key & k, node2 & n);
	key_test_enum test_key(const key & k);

	bool is_full();

	bool at_least_full(int &r);

	bool is_root() {
		return test_flag(PAGE_PP_BIT) == false;
	}

	bool is_leaf_page() {
		return test_flag(PAGE_HAS_LEAF_BIT) == false;
	}

	bool is_left();

	bool is_right();

	void print_all();

	virtual void set_flag(int bit) {
		header->flag |= (1 << bit);
	}

	virtual void remove_flag(int bit) {
		header->flag &= ~(1 << bit);
	}

	virtual bool test_flag(int b) {
		return (header->flag >> b & 1) == 1;
	}

	virtual ~ fixed_size_index_block();
};

typedef fixed_size_index_block page;

/*
 *  the node2 type is not compitable with sdb::tree::node, as node2 use struct key and val
 *  for storing node key and value, instead of template.
 *
 *  a node2 object can stay in memory, also can stay in disk or both of them.
 *
 *	the flag has 8 bits,  list as
 *  the 0th bit, 1 denote a node2 is deleted.
 *  the 1rd bit, 1 denote a node2 has parent page
 *  the 2st bit, 1 denote a node2 has left page
 *  the 3rd bit, 1 denote a node2 has right page
 *  the 4th bit, 1 denote a node2's left page is in another segment
 *  the 5th bit, 1 denote a node2's left page is in another segment of another data file
 *  the 6th bit, 1 denote a node2's right is in another segment
 *  the 7th bit, 1 denote a node2's right is in another segment of another data file
 */
struct node2 {

	struct head {
		short flag = 0;
		unsigned short left_pg_off, right_pg_off; /* left page offset, right page offset */
		unsigned short lpwp, rpwp; /* left page write pointer, right page write pointer */
	}* header = nullptr;

	/* the storage define */
	int offset; /* the offset behind start of the block  */
	char * buffer = nullptr;
	int length; /* total length of the node2 bytes, excludes header */

	bool ref_flag = false;
	key k;
	val v;

	page * ref_page = nullptr; /* this node's reference page which contains it */
//	page left_page; /* the left page, which nodes key is less then this key */
//	page right_page; /* the right page, which nodes key is equal or greater than this key */

	void set_key(key & _k) {
		k.set_val(_k.v, _k.len);
	}

	void set_key_val(key & _k, val &_v) {
		k.set_val(_k.v, _k.len);
		v.set_val(_v.v, _v.len);
	}

	void set_key_val(char *kb, short kl, char *vb, short vl) {
		k.set_val(kb, kl);
		v.set_val(vb, vl);
	}

	void init(int off, int len) {
		buffer = new char[index_block_header_size + len];
		header = (head *) buffer;
		buffer += index_block_header_size;
	}

	void ref(char *buff, int off, int len) {
		ref_flag = true;
		offset = off;
		this->length = len;

		header = (head *) buff;
		buffer = buff + node2_header_size;
	}

	/*
	 * the buff first part is node header, the rest of the buffer
	 * is content includes key and value
	 */
	void ref(char *buff, int off, int len, short kl, short vl) {
		ref_flag = true;
		offset = off;
		this->length = len;

		header = (head *) buff;
		buffer = buff + node2_header_size;

		k.ref_val(buffer, kl);
		v.ref_val(buffer + kl, vl);
	}

	inline void set_flag(int bit) {
		header->flag |= (1 << bit);
	}

	inline void remove_flag(int bit) {
		header->flag &= ~(1 << bit);
	}

	inline bool test_flag(int b) {
		return (header->flag >> b & 1) == 1;
	}

	void set_left(page *p);
	void set_right(page *p);

//	inline bool has_pre_nd2() {
//		return offset > 0;
//	}
//
//	inline bool has_nxt_nd2() {
//		int ns = ref_page->header->node_size;
//		int nc = ref_page->header->node_count;
//		return offset / ns < nc - 1;
//	}

	/*
	 * if current node has next next, fill it to next node parameter
	 * and return 1, else return 0;
	 */
	bool nxt_nd2(node2 &nxt);

	/*
	 * if current node has previous next, fill it to nxt node parameter
	 * and return 1, else return 0;
	 */
	bool pre_nd2(node2 &pre);

	bool operator==(const node2 &other);

	void copy_from(const node2 & other) {
		memcpy(header, other.header, other.length);
		memcpy(buffer, other.buffer, other.length);
	}

	node2 & operator=(const node2& other) {
		if (&other != this) {
			header = other.header;
			length = other.length;
			ref_page = other.ref_page;
			buffer = new char[length];
			memcpy(buffer, other.buffer, other.length);
			ref_flag = false;
		}
		return *this;
	}

	bool operator<(const node2 & other) {
		return k.compare(other.k) < 0;
	}

	bool operator>(const node2 & other) {
		return k.compare(other.k) > 0;
	}

	~ node2() {
		if (!ref_flag) {
			if (header) {
				delete header;
			}
			if (buffer) {
				delete[] buffer;
			}
		}
	}
}
;

class bptree {
	friend class index_segment;
public:
	int load();
	/*
	 * find a correct page for a key from a given page, and tell the key test
	 * to key test enum parameter, tell the page to target page parameter if
	 * found page successfully, else return FAILURE
	 */
	int find_page(page &f, const key &k, page & t, key_test_enum & kt);
	int add_key(key &k, val& v);
	int add_node2(node2 &n);
	int remove_key(const key &k);
	int update_val(const key &k, const val& v);

	/*
	 * scan the tree to find a key which relations ship denoted by kt.
	 * if find return the first page and offset denote the corresponding
	 * nodes.
	 */
	int scan(const key&k, const scan_type & st, page &p, node2 & n);
	int scan(const key&k1, const scan_type & st1, const key&k2,
			const scan_type & st2);

	bptree(index_segment * _root_seg, int kl, int vl, int bs);
	bptree(index_segment * _root_seg, int m, int kl, int vl, int bs);
	virtual ~bptree();

	int get_block_size() const {
		return block_size;
	}

	int get_key_len() const {
		return key_len;
	}

	int get_m() const {
		return m;
	}

	index_segment* get_root_seg() {
		return root_seg;
	}

	int get_val_len() const {
		return val_len;
	}

private:
	int m; /* the max node in a page */
	int key_len; /* the key length in byte */
	int val_len; /* the value length  in byte */
	int block_size; /* a block size in bytes */

	index_segment * root_seg, *curr_seg;

	std::map<unsigned long, index_segment> seg_map;
	page root_page;

	/*
	 * fetch the node's left page
	 */
	int fetch_left_page(node2 &n, page & lp);

	/*
	 * fetch the node's right page
	 */
	int fetch_right_page(node2 &n, page & rp);

	/*
	 * fetch a page's parent page
	 */
	int fetch_parent_page(page &p, page &parent);

	/*
	 * fetch a page's parent node
	 */
	int fetch_parent_node(page &p, node2 &n);

	int fetch_parent_node( page & parent, page &p, node2 & pn);

	/*
	 * fetch the page's next page if it has
	 */
	int fetch_next_page(page &p, page &nxt);

	/*
	 * fetch the page's previous page if it has
	 */
	int fetch_pre_page(page &p, page &pre);

	/*
	 * transfer node2 from a given position to the tail of another page without
	 * range check
	 */
	void transfer(page &p1, int from, int count, page & p2);
	int add_key(page & p, key &k, val& v, key_test_enum kt = not_defenition);
	int add_node2(page &p, node2 &n, key_test_enum kt = not_defenition);
	int assign_page(page &p);
	int expand_organize(page &p, key_test_enum kt = not_defenition);
	void split_1_2(page &p, page &left, page &right);
	void split_2_3(page &p1, page &p2, page & empty);

	int remove_key(page &p, const key &k, const key_test_enum &pkt);
	int shrink_organize(page &p, const key_test_enum & kt);

	/*
	 * if both p1 and p2 are less or equals half full, where p1i < p2i, i=0,1,,,k-1.
	 *  merge them to the first page p1 and remove the page p2
	 */
	int merge_2_1(page &p1, page &p2);

	/*
	 * if p1, p2, p3 are less or equals half full, merge them to two pages p1, p2, and remove
	 * the page p3, the function assumes that k1i < k2i < k3i, which k1i means key in p1 and
	 * i = 0, 1, m-1, as same as k2i, k3i respects for p2, p3.
	 */
	int merge_3_2(page &p1, page &p2, page &p3);

	int borrow_left(page &p);
	int borrow_right(page &p);

	void to_right(page &p);
	void to_left(page &p);
};

class index_segment: public segment {
	friend class bptree;
public:
	int assign_page(page *p);
	int read_page(page *p);
	int remove_page(page *p);

	index_segment(unsigned long _id);
	index_segment(unsigned long _id, data_file *_f, unsigned int _off = 0);
	index_segment(const index_segment & other);
	index_segment(index_segment && other);
	index_segment(segment & other);
	index_segment & operator=(index_segment & other);

	int fetch_left_page(node2 &n, page & lp);
	int fetch_right_page(node2 &n, page & rp);
	int fetch_parent_page(node2 &n, page &pp);

	int fetch_next_page(page &p, page &nxt);
	int fetch_pre_page(page &p, page &pre);

	const bptree* get_tree() const {
		return tree;
	}

private:
	bptree * tree = nullptr;

};

} /* namespace tree */
} /* namespace sdb */

#endif /* BPTREE_H_ */
