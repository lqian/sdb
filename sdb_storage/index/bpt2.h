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
	int len;

	virtual void ref(uint off, char * buff, int w_len)=0;

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
		return header->flag & s == s;
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
	}*header;

	page_type type;
	bool is_root = false;

	virtual void ref(uint off, char * buff, ushort w_len)=0;
	virtual int assign_node(_node * n)=0;
	virtual int read_node(ushort idx, _node *)=0;
	virtual int remove_node(ushort idx)=0;

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
};

struct fs_page: virtual _page {
	struct head: _page::head {
		ushort node_count;
		ushort node_size;
	}*header = nullptr;

	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual inline void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);

	int count() {
		return header->node_count;
	}
};

struct vs_page: virtual _page {
	struct head: _page::head {
		uint dir_write_offset;
		uint data_write_offset;
	}*header = nullptr;
	virtual inline void init_header() {
		header->flag = 0;
		header->blk_magic = block_magic;
		time(&(header->create_time));
	}

	virtual int assign_node(_node * n);
	virtual int read_node(ushort idx, _node *);
	virtual int remove_node(ushort idx);

	int count() {
		return header->dir_write_offset / NODE_DIR_ENTRY_LENGTH;
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
	void ref(uint off, char *buff, int len) {
		header = (head *) buff;
		offset = off;
		this->buffer = buff + sizeof(head);
		this->len = len - sizeof(head);
	}
};

struct _ipage: virtual _page {

};

// fixed size node page
struct fs_ipage: virtual fs_page, virtual _ipage {
	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(fs_page::head);
		ref_flag = true;
	}
};

struct vs_ipage: virtual vs_page, virtual _ipage {
	virtual inline void ref(uint off, char *buff, ushort w_len) {
		offset = off;
		length = w_len - sizeof(fs_page::head);
		header = (head *) buff;
//			header->node_count = 0;
		buffer = buff + sizeof(fs_page::head);
		ref_flag = true;
	}
};

struct _lpage: virtual _page {

};

// leaf node,
struct _lnode: virtual _node {
	struct head {
		char flag;
	}*header;

	char * buff;  // include key and value
	int len;
	inline virtual void write_val(const _val &v) {
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
	inline virtual void ref(uint off, char *buff, int w_len) {
		this->offset = off;
		header = (head *) buff;
		this->buff = buff + sizeof(head);
		this->len = w_len - sizeof(head);
	}
};

//variant size leaf node
struct vs_lnode: _lnode {

};

/*
 * fixed size page that only contains fixed size leaf node
 */
struct fs_lpage: fs_page {
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
	_page * root;
	_lpage *flp, *llp; // first leaf page and last leaf page

	bool loaded = false;

	int remove_node(const data_item *di); // a data item represent a index entry

	mutex som_mtx; // structure of modification (page merge, split, borrow) mutex

	/*
	 * assign a data_item for the node and fill seg_id, blk_off, row_idx to the data_item
	 * pointer parameter and return sdb::SUCCESS if assign it
	 */
	int assign_node(_node *n, data_item *di);
	int froze(const data_item *di);

public:
	bpt2();
	bpt2(ulong obj_id);
	virtual ~bpt2();

	int load();

	int load(segment * fs, _page * r);

	int load(ulong first_seg_id, uint root_blk_off);

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
