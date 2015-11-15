/*
 * bpt2.h
 *
 *  Created on: Oct 26, 2015
 *      Author: lqian
 */

#ifndef BPT2_H_
#define BPT2_H_

#include <list>
#include <string>
#include <condition_variable>

#include "../enginee/trans_def.h"
#include "../storage/datafile.h"
#include "../common/char_buffer.h"
#include "../enginee/sdb.h"
#include "../enginee/seg_mgr.h"

#include "kv.h"

/*
 * a b+tree has the feature:
 * 1) a page has many nodes, a page only contains one type of node, index node or leaf node,
 * 	index node only has key, leaf node has key and value which point a data item.
 *  a page contains index node call index page, abb _ipage, a page only contains leaf node
 *  call leaf page, abb _lpage.
 *
 * 2) an index node has left page that all node key is less than parent index node key
 *  or right page that all node key is equal or greater than the page index node key.
 * 	in the index page, only the last index node has both left page and right page. it previous
 * 	index node only has left page. where is
 * 		in[i] < i[n+1],
 * 		k[i] ={k1, k2,, kn} belong i[i]->left_page,
 * 		k[i+1] = {k1, k2, kn} belong i[i+1]->left_page
 * 	so, any element k < k', where k in k[i], k' in k[i+1], k < i[n], k' >= i[n], k'< i[n+1]
 */

namespace sdb {
namespace index {
using namespace std;
using namespace sdb::common;
using namespace sdb::enginee;
using namespace sdb::storage;

const int VS_PAGE_DIR_ENTRY_LENGTH = 3;
const int NODE_DIR_ENTRY_LENGTH = 18;
const int VAL_LEN = 14;

const int NODE_REMOVE_BIT = 15;
const int NODE_LEFT_PAGE_BIT = 14;
const int NODE_RIGHT_PAGE_BIT = 13;

const int PAGE_REMOVE_BIT = 15;
const int PAGE_HAS_PARENT_BIT = 14;
const int PAGE_HAS_LEAF_BIT = 13;
const int PAGE_FULL_BIT = 12;

const int LNODE_REMOVE_BIT = 7;

const int INVALID_INDEX_SEGMENT_ID = -0x600;

struct _page;
struct _ipage;
struct _lpage;
struct fs_page;
struct vs_page;
struct _node;
struct _inode;
struct _lnode;

enum key_test {
	key_invalid_test = 0,
	key_equal,
	key_less,
	key_greater,
	key_within_page = 0x10,
	not_defenition
};

enum page_type {
	pt_fs_ipage, pt_vs_ipage, pt_vs_lpage, pt_fs_lpage
};

bool test_flag(ushort &s, const int bit);
void set_flag(char & c, const int bit);
void remove_flag(char & c, const int bit);

void set_flag(ushort &s, const int bit);
void remove_flag(ushort &s, const int bit);
void set_flag(short &s, const int bit);
void remove_flag(short &s, const int bit);

key_test test(_page *p, _key &k, _node *n);

_page * new_page(_page * sibling, bool fixed_size = true);
_lpage * new_lpage(bool fixed_size = true);
_ipage * new_ipage(bool fixed_size = true);
_lnode * new_lnode(bool fixed_size = true);
_inode * new_inode(bool fixed_size = true);

struct _node {
	struct head {
		ushort flag;
	}*header;

	_page * cp; // container page;

	uint offset;
	char * buffer;
	ushort len;

	virtual void ref(uint off, char * buff, ushort w_len)=0;

	virtual inline void set_flag(const int& bit) {
		ushort s = 1 << bit;
		header->flag |= s;
	}

	virtual inline void clean_flag(const int& bit) {
		ushort s = ~(1 << bit);
		header->flag &= s;
	}

	virtual inline bool test_flag(const int & bit) {
		ushort s = 1 << bit;
		return (header->flag & s) == s;
	}

	virtual void remove() {
		clean_flag(NODE_REMOVE_BIT);
	}

	void write_key(const _key & k) {
		memcpy(buffer, k.buff, k.len);
	}

	void write_key(const _key * k) {
		memcpy(buffer, k->buff, k->len);
	}
	void read_key(_key &k) {
		k.ref(buffer, len);
		k.kv_off = 0;
	}
	void read_key(_key *k) {
		k->ref(buffer, k->len);
		k->kv_off = 0;
	}

	virtual ~_node () {}
};

// leaf node,
struct _lnode: virtual _node {
	inline void write_val(const _val &v) {
		memcpy(buffer + len - VAL_LEN, v.buff, VAL_LEN);
	}
	inline void write_val(const _val *v) {
		memcpy(buffer + len - VAL_LEN, v->buff, VAL_LEN);
	}
	inline void read_key(_key &k) {
		k.ref(buffer, len - VAL_LEN);
		k.kv_off = 0;
	}
	void read_key(_key *k) {
		k->ref(buffer, len - VAL_LEN);
		k->kv_off = 0;
	}
	inline void read_val(_val & v) {
		v.ref(buffer + len - VAL_LEN, VAL_LEN);
	}
	inline void read_val(_val *v) {
		v->ref(buffer + len - VAL_LEN, VAL_LEN);
	}

	virtual void remove() {
		clean_flag(LNODE_REMOVE_BIT);
	}
};

//fixed size leaf node, include key and value
struct fs_lnode: _lnode {
	inline virtual void ref(uint off, char *buff, ushort w_len) {
		this->offset = off;
		header = (head *) buff;
		this->buffer = buff + sizeof(head);
		this->len = w_len - sizeof(head);
	}
};

//variant size leaf node
struct vs_lnode: _lnode {
	inline virtual void ref(uint off, char *buff, ushort w_len) {
		this->offset = off;
		header = (head *) buff;
		this->buffer = buff + sizeof(head);
		this->len = w_len - sizeof(head);
	}
};

/*
 * a index node only has store key, left page offset and right page offset
 */
struct _inode: _node {
	struct head: _node::head {
		uint left_pg_off;
		uint right_pg_off;
		ulong left_pg_seg_id;
		ulong right_pg_seg_id;
	}* header = nullptr;

	_page * left_page = nullptr;
	_page * right_page = nullptr;

	virtual void ref(uint off, char * buff, ushort w_len) {
		this->offset = off;
		header = (head *) buff;
		this->buffer = buff + sizeof(head);
		this->len = w_len - sizeof(head);

	}

	virtual inline void set_flag(const int& bit) {
		ushort s = 1 << bit;
		header->flag |= s;
	}

	virtual inline void clean_flag(const int& bit) {
		ushort s = ~(1 << bit);
		header->flag &= s;
	}

	virtual inline bool test_flag(const int & bit) {
		ushort s = 1 << bit;
		return (header->flag & s) == s;
	}

	virtual void remove() {
		clean_flag(NODE_REMOVE_BIT);
	}

	void set_left_page(_page * p);
	void set_right_page(_page *p);
};

struct fs_inode: virtual _inode {
	void ref(uint off, char *buff, ushort len) {
		header = (head *) buff;
		offset = off;
		this->buffer = buff + sizeof(head);
		this->len = len - sizeof(head);
	}

};

struct vs_inode: virtual _inode {
	void ref(uint off, char *buff, ushort len) {

	}
};

struct _page: virtual data_block {
	struct head: data_block::head {
		uint parent_in_idx;
		uint parent_ipg_off;
		ulong parent_pg_seg_id;
		ushort active_node_count = 0;
	}*header;

	page_type type;

	virtual int assign_node(_node * n)=0;
	virtual int read_node(ushort idx, _node *in)=0;
	virtual int remove_node(ushort idx)=0;
	virtual void sort_nodes(list<_key_field> key_fields, bool ascend = true)=0;
	virtual int insert_node(list<_key_field> key_fields, _node *n, bool ascend =
			true)=0;
	virtual void clean_nodes()=0;

	/*
	 * return the node count in the page, include deleted and active node
	 */
	virtual int count()=0;

	virtual inline void clean_flag(const int& bit) {
		ushort s = ~(1 << bit);
		header->flag &= s;
	}

	virtual void set_parent(const _inode * n);

	virtual inline bool is_full() {
		return test_flag(PAGE_FULL_BIT);
	}

	virtual inline bool has_leaf() {
		return test_flag(PAGE_HAS_LEAF_BIT);
	}

	virtual inline bool has_parent() {
		return test_flag(PAGE_HAS_PARENT_BIT);
	}
};

struct fs_page: virtual _page {
	struct head: _page::head {
		ushort node_count;
		ushort node_size;
	}*header = nullptr;

	int assign_node(fs_page * p, _node *);
	int read_node(fs_page *p, ushort idx, _node *in);
	int remove_node(fs_page *p, ushort idx);
	void clean_node(fs_page *p);
	void sort_nodes(list<_key_field> key_fields, fs_page *p,
			bool ascend = true);
	int insert_node(list<_key_field> key_fields, fs_page *p, _node *n,
			bool ascend = true);
	void init_header(fs_page *p);

	void move_nodes(ushort from_idx, ushort dest_idx);

	inline virtual void set_flag(int bit) {
		header->flag |= (1 << bit);
	}

	inline virtual void remove_flag(int bit) {
		header->flag &= ~(1 << bit);
	}

	inline virtual bool test_flag(int b) {
		return (header->flag >> b & 1) == 1;
	}

};

struct vs_page: virtual _page {
	struct head: _page::head {
		uint writing_entry_off;
		uint writing_data_off;
	}*header = nullptr;

	int assign_node(vs_page * p, _node *);
	int read_node(vs_page *p, ushort idx, _node *in);
	int remove_node(vs_page *p, ushort idx);
	void clean_node(vs_page *p);
	void sort_nodes(list<_key_field> key_fields, vs_page *p,
			bool ascend = true);
	int insert_node(list<_key_field> key_fields, vs_page *p, _node *n,
			bool ascend = true);
	void init_header(vs_page *p);
};

struct _ipage: virtual _page {

};

// fixed size node page
struct fs_ipage: fs_page, virtual _ipage {
	virtual void ref(uint off, char*buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual void init_header() {
		sdb::index::set_flag(fs_page::header->flag, PAGE_HAS_LEAF_BIT);
	}

	virtual int assign_node(_node * n) {
		return fs_page::assign_node(this, n);
	}
	virtual int read_node(ushort idx, _node *in) {
		return fs_page::read_node(this, idx, in);
	}
	virtual int remove_node(ushort idx) {
		return fs_page::remove_node(this, idx);
	}
	virtual void sort_nodes(list<_key_field> key_fields, bool ascend = true) {
		return fs_page::sort_nodes(key_fields, this, ascend);
	}
	virtual int insert_node(list<_key_field> key_fields, _node *n, bool ascend =
			true) {
		return fs_page::insert_node(key_fields, this, n, ascend);
	}
	virtual void clean_nodes() {
		return fs_page::clean_node(this);
	}

	virtual int count() {
		return header->node_count;
	}
};

struct vs_ipage: vs_page, _ipage {
	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	void init_header() {
		vs_page::init_header(this);
	}

	virtual int assign_node(_node * n) {
		return vs_page::assign_node(this, n);
	}
	virtual int read_node(ushort idx, _node *in) {
		return vs_page::read_node(this, idx, in);
	}
	virtual int remove_node(ushort idx) {
		return vs_page::remove_node(this, idx);
	}
	virtual void sort_nodes(list<_key_field> key_fields, bool ascend = true) {
		return vs_page::sort_nodes(key_fields, this, ascend);
	}
	virtual int insert_node(list<_key_field> key_fields, _node *n, bool ascend =
			true) {
		return vs_page::insert_node(key_fields, this, n, ascend);
	}

	virtual void clean_nodes() {
		return vs_page::clean_node(this);
	}

	virtual int count() {
		return header->writing_entry_off / VS_PAGE_DIR_ENTRY_LENGTH;
	}

};

struct _lpage: virtual _page {

};

/*
 * fixed size page that only contains fixed size leaf node
 */
struct fs_lpage: virtual fs_page, virtual _lpage {
	void init_header() {
		fs_page::init_header(this);
	}
	void ref(uint off, char * buff, ushort w_len);
	int assign_node(_node * n);
	int read_node(ushort idx, _node *);
	int remove_node(ushort idx);
	int count();
	void sort_nodes(list<_key_field> key_fields, bool ascend = true);
	virtual int insert_node(list<_key_field> key_fields, _node *n, bool ascend =
			true) {
		return fs_page::insert_node(key_fields, this, n, ascend);
	}
	void clean_nodes();
};

struct vs_lpage: vs_page, _lpage {
	void init_header() {
		vs_page::init_header(this);
	}
	void ref(uint off, char * buff, ushort w_len);
	int assign_node(_node * n);
	int read_node(ushort idx, _node *);
	int remove_node(ushort idx);
	int count();
	void sort_nodes(list<_key_field> key_fields, bool ascend = true);
	virtual int insert_node(list<_key_field> key_fields, _node *n, bool ascend =
			true) {
		return vs_page::insert_node(key_fields, this, n, ascend);
	}
	void clean_nodes();
};

class bpt2 {
private:
	ulong obj_id;
	int key_size;
	/*
	 * all key's field is fixed size
	 */
	bool fixed_size = true;
	_key key;
	bool ascend = true;

	seg_mgr * sm;
	segment * first_seg;
	segment * last_seg;
	_page * root;

	bool loaded = false;

	int remove_node(const data_item *di); // a data item represent a index entry

	mutex som_mtx; // structure of modification (page merge, split, borrow) mutex

	int assign_lpage(segment * seg, _lpage * & p);
	int assign_ipage(segment *seg, _ipage * & p);

	int assign_lpage(_lpage * & p);
	int assign_ipage(_ipage * & p);

	int fetch_left_page(_inode *in, _page * p);
	int fetch_right_page(_inode *in, _page * p);
	int fetch_next_page(_page *p, _page *nxt);
	int fetch_pre_page(_page *p, _page *pre);

	int fetch_parent_inode(_page * p, _inode * in);
	int fetch_parent_page(_page *p, _page *pp);

	/*
	 * split the page's large key of half node to the another page
	 */
	void split_2(_page * p, _page * half);
	int find_page(_page *p, const _key &k, _lpage * lp, key_test & kt);
	int insert(_page *p, _key * k, _val * v = nullptr);

public:

	bpt2();
	bpt2(ulong obj_id);
	virtual ~bpt2();

	int load();

	int load(segment * fs, segment *ls, _ipage * r);

	int load(ulong first_seg_id, ulong last_seg_id, uint root_blk_off);

	int create(ulong first_seg_id);

	int insert(_key & k, _val &v);

	int remove_node(_node *n);

	inline bool is_fixed() {
		return fixed_size;
	}

	inline void set_fixed(bool f = false) {
		fixed_size = f;
	}

	inline const _key& get_key() const {
		return key;
	}

	inline void set_key(const _key& key) {
		this->key = key;
	}

	inline void set_seg_mgr(seg_mgr * p) {
		sm = p;
	}
};

} /* namespace tree */
} /* namespace sdb */

#endif /* BPT2_H_ */
