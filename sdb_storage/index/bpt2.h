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
 * and right page that all node key is equal or greater than the page index node key.
 */

namespace sdb {
namespace index {
using namespace std;
using namespace sdb::common;
using namespace sdb::enginee;
using namespace sdb::storage;

const int NODE_DIR_ENTRY_LENGTH = 18;
const int VAL_LEN = 14;

const int NODE_REMOVE_BIT = 15;
const int NODE_LEFT_PAGE_BIT = 14;
const int NODE_RIGHT_PAGE_BIT = 13;

const int PAGE_REMOVE_BIT = 15;
const int PAGE_PARENT_BIT = 14;
const int PAGE_LEAF_BIT = 13;
const int PAGE_FULL_BIT = 12;

const int LNODE_REMOVE_BIT = 7;

const int INVALID_SEGMENT_ID = -0x600;

void set_flag(char & c, const int bit);
void remove_flag(char & c, const int bit);

void set_flag(ushort &s, const int bit);
void remove_flag(ushort &s, const int bit);
void set_flag(short &s, const int bit);
void remove_flag(short &s, const int bit);

struct _page;
struct _ipage;
struct _lpage;
struct _inode;

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

struct _node {
	struct head {
		ushort flag;
	}*header;

	_page * cp; // container page;

	uint offset;
	char * buffer;
	ushort len;

	virtual void ref(uint off, char * buff, ushort w_len)=0;

	inline void set_flag(const int& bit) {
		ushort s = 1 << bit;
		header->flag |= s;
	}

	inline void clean_flag(const int& bit) {
		ushort s = ~(1 << bit);
		header->flag &= s;
	}

	inline bool test_flag(const int & bit) {
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
		k.ref(buffer, k.len);
	}
	void read_key(_key *k) {
		k->ref(buffer, k->len);
	}
};

struct _page: data_block {
	struct head: data_block::head {
		uint parent_in_off;
		uint parent_ipg_off;
		ulong parent_pg_seg_id;
		ushort active_node_count = 0;
	}*header;

	page_type type;
	bool is_root = false;

	virtual void ref(uint off, char * buff, ushort w_len)=0;
	virtual int assign_node(_node * n)=0;
	virtual int read_node(ushort idx, _node *)=0;
	virtual int remove_node(ushort idx)=0;
	virtual void sort_nodes()=0;
	virtual void clean_nodes()=0;

	/*
	 * return the node count in the page, include deleted and active node
	 */
	virtual int count()=0;

	virtual inline void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	inline void set_flag(const int& bit) {
		ushort s = 1 << bit;
		header->flag |= s;
	}

	inline void clean_flag(const int& bit) {
		ushort s = ~(1 << bit);
		header->flag &= s;
	}

	void set_parent(const _inode * n);

	inline bool is_full() {
		return test_flag(PAGE_FULL_BIT);
	}
};

struct fs_page: virtual _page {
	struct head: _page::head {
		ushort node_count;
		ushort node_size;
	}*header = nullptr;

	virtual void ref(uint off, char *buff, ushort w_len)=0;

	virtual inline void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);

	inline int count() {
		return header->node_count;
	}
};

struct vs_page: virtual _page {
	struct head: _page::head {
		uint writing_entry_off;
		uint writing_data_off;
	}*header = nullptr;

	virtual inline void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);

	inline int count() {
		return header->writing_entry_off / NODE_DIR_ENTRY_LENGTH;
	}
};

/*
 * a index node only has store key, left page offset and right page offset
 */
struct _inode: virtual _node {
	struct head: virtual _node::head {
		uint left_pg_off;
		uint right_pg_off;
		ulong left_pg_seg_id;
		ulong right_pg_seg_id;
	}* header = nullptr;

	_page * left_page = nullptr;
	_page * right_page = nullptr;

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
	void ref(uint off, char *buff, ushort len);
};

struct _ipage: virtual _page {
	key_test test(_key & k, _inode & in);
};

// fixed size node page
struct fs_ipage: fs_page, virtual _ipage {
	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		header->active_node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	void sort_nodes();

	virtual void clean_nodes();
};

struct vs_ipage: virtual vs_page, virtual _ipage {
	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	void sort_nodes();
	virtual void clean_nodes();
};

struct _lpage: virtual _page {
	virtual void ref(uint off, char * buff, ushort w_len)=0;
	virtual int assign_node(_node * n) =0;
	virtual int read_node(ushort idx, _node *)=0;
	virtual int remove_node(ushort idx) =0;
	virtual int count() =0;
	virtual void clean_nodes()=0;
};

// leaf node,
struct _lnode: virtual _node {
	struct head {
		char flag;
	}*header;

	char * buff;  // include key and value
	int len;
	inline void write_val(const _val &v) {
		memcpy(buff + len - VAL_LEN, v.buff, VAL_LEN);
	}
	inline void write_val(const _val *v) {
		memcpy(buff + len - VAL_LEN, v->buff, VAL_LEN);
	}
	inline void read_key(_key &k) {
		k.ref(buff, len - VAL_LEN);
	}
	void read_key(_key *k) {
		k->ref(buff, len - VAL_LEN);
	}
	inline void read_val(_val & v) {
		v.ref(buff + len - VAL_LEN, VAL_LEN);
	}
	inline void read_val(_val *v) {
		v->ref(buff + len - VAL_LEN, VAL_LEN);
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
		this->buff = buff + sizeof(head);
		this->len = w_len - sizeof(head);
	}
};

//variant size leaf node
struct vs_lnode: _lnode {
	inline virtual void ref(uint off, char *buff, ushort w_len);
};

/*
 * fixed size page that only contains fixed size leaf node
 */
struct fs_lpage: virtual fs_page, virtual _lpage {
	virtual void ref(uint off, char * buff, ushort w_len);
	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);
	virtual int count();
	virtual void sort_nodes();
	virtual void clean_nodes();
};

struct vs_lpage: virtual vs_page, virtual _lpage {
	virtual void ref(uint off, char * buff, ushort w_len);
	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);
	virtual void sort_nodes();

	/*
	 * return the node count in the page, include deleted and active node
	 */
	virtual int count();

	virtual void clean_nodes();

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

	seg_mgr * sm;
	segment * first_seg;
	segment * last_seg;
	_page * root;

	bool loaded = false;

	int remove_node(const data_item *di); // a data item represent a index entry

	mutex som_mtx; // structure of modification (page merge, split, borrow) mutex

	void create_lpage(_lpage *&p);
	void create_ipage(_ipage *&p);
	void craete_inode(_inode *&in);
	void create_lnode(_lnode *&ln);

	int assign_lpage(segment * seg, _lpage * & p);
	int assign_ipage(segment *seg, _ipage * & p);

	int assign_lpage(_lpage * & p);
	int assign_ipage(_ipage * & p);

	int fetch_left_page(_inode *in, _page * p);
	int fetch_right_page(_inode *in, _page * p);
	int fetch_parent_inode(_page * p, _inode * in);
	int fetch_parent_page(_page *p, _page *pp);

	/*
	 * split the page's large key of half node to the another page
	 */
	void split_2(_page * p, _page * half);
	int find_page(_page *p, const _key &k, _lpage * lp, key_test & kt);
	int add_node(_page *p, _lnode *ln);

public:

	bpt2();
	bpt2(ulong obj_id);
	virtual ~bpt2();

	int load();

	int load(segment * fs, segment *ls, _ipage * r);

	int load(ulong first_seg_id, ulong last_seg_id, uint root_blk_off);

	int create(ulong first_seg_id);

	int add_node(_lnode * n);
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
