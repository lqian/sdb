/*
 * bpt2.h
 *
 *  Created on: Oct 26, 2015
 *      Author: lqian
 */

#ifndef BPT2_H_
#define BPT2_H_

#include <list>

#include "../enginee/trans_def.h"
#include "../storage/datafile.h"
#include "../common/char_buffer.h"

namespace sdb {
namespace tree {
using namespace std;
using namespace sdb::common;
using namespace sdb::enginee;
using namespace sdb::storage;

const int INODE_REMOVE_BIT = 15;
const int INODE_LEFT_IPAGE_BIT = 14;
const int INODE_RIGHT_IPAGE_BIT = 13;
const int INODE_LEFT_LPAGE_BIT = 12;
const int INODE_RIGHT_LPAGE_BIT = 11;

const int PAGE_REMOVE_BIT = 15;
const int PAGE_PARENT_BIT = 14;

const int LNODE_REMOVE_BIT = 7;

void set_flag(char & c, const int bit);
void remove_flag(char & c, const int bit);

void set_flag(ushort &s, const int bit);
void remove_flag(ushort &s, const int bit);
void set_flag(short &s, const int bit);
void remove_flag(short &s, const int bit);

struct _ipage;
struct _lpage;

struct _key_field {
	char * buff;
	int len;

	inline void ref(char*buff, int len) {
		this->buff = buff;
		this->len = len;
	}
	virtual int compare(const _key_field & kf);
	virtual ~_key_field() {
	}
};

struct char_key_field: _key_field {
	virtual int compare(const _key_field &kf) {
		char_buffer tmp(buff, len, true);
		char v;
		tmp >> v;

		tmp.reset();
		tmp.ref_buff(kf.buff, kf.len);
		char t;
		tmp >> t;

		return v > t ? 1 : v == t ? 0 : -1;
	}
};

struct uchar_key_field: _key_field {
	virtual int compare(const _key_field &kf) {
		char_buffer tmp(buff, len, true);
		uchar v;
		tmp >> v;

		tmp.reset();
		tmp.ref_buff(kf.buff, kf.len);
		uchar t;
		tmp >> t;

		return v > t ? 1 : v == t ? 0 : -1;
	}
};

struct short_key_field: _key_field {
	virtual int compare(const _key_field &kf) {
		char_buffer tmp(buff, len, true);
		short v;
		tmp >> v;

		tmp.reset();
		tmp.ref_buff(kf.buff, kf.len);
		short t;
		tmp >> t;

		return v > t ? 1 : v == t ? 0 : -1;
	}
};

struct ushort_key_field: _key_field {
	virtual int compare(const _key_field &kf) {
		char_buffer tmp(buff, len, true);
		ushort v;
		tmp >> v;

		tmp.reset();
		tmp.ref_buff(kf.buff, kf.len);
		ushort t;
		tmp >> t;

		return v > t ? 1 : v == t ? 0 : -1;
	}
};

struct int_key_field: _key_field {
	virtual int compare(const _key_field &kf) {
		char_buffer tmp(buff, len, true);
		int v;
		tmp >> v;

		tmp.reset();
		tmp.ref_buff(kf.buff, kf.len);
		int t;
		tmp >> t;

		return v > t ? 1 : v == t ? 0 : -1;
	}
};

struct _key {
	char * k_buff;
	char * v_buff;
	int kb_len;
	int vb_len;

	list<_key_field> key_fields;
	void ref(char *k_buff, int k_len, char*v_buff, int v_len);
	int compare(const _key & an);
	void add_field(const _key_field & kf);
};

struct _val {
	char * buff;
	int len;

	void ref(char*buff, int len);
	void to_data_item(data_item &di);
	void to_data_item(data_item *pdi);
	void set_data_item(const data_item & di);
	void set_data_item(const data_item * pdi);
};

/*
 * a index node only has store key, left page offset and right page offset
 */
struct _inode {
	struct head {
		ushort flag;
		uint left_pg_off;
		uint right_pg_off;
		ulong left_pg_seg_id;
		ulong right_pg_seg_id;
	}* header;

	uint offset;
	char * buff = nullptr;
	int len = 0;

	_ipage * cp; // container page;

	// inode's left page that contains node's key less than or equals the inode
	_lpage * left_lpage = nullptr;

	// inode's right page that contains node's key greater than the inode
	_lpage * right_lpage = nullptr;

	_ipage * left_ipage = nullptr;
	_ipage * right_ipage = nullptr;

	virtual void ref(uint offset, char *buff, int len) {
	}
	virtual int write_key(const _key & k) {
		return 0;
	}
	virtual int write_key(const _key * k) {
		return 0;
	}
	virtual int read_key(_key &k) {
		return 0;
	}
	virtual int read_key(_key *k);

	void set_left_lpage(_lpage * p);
	void set_right_lpage(_lpage *p);
	void set_left_ipage(_ipage *p);
	void set_right_ipage(_ipage *p);
};

/*
 * fixed size index node
 */
struct fs_inode: _inode {

};

struct vs_inode: _inode {

};

struct _ipage: data_block {
	struct head: data_block::head {
		ushort node_count;
		uint parent_in_off;
		uint parent_ipg_off;
		ulong parent_pg_seg_id;
	}*header;

	inline virtual void ref(int off, char *buff, short w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual int assign_inode(_inode & n);
	virtual int read_inode(ushort idx, _inode &n);
	virtual int remove_inode(ushort idx);
	virtual bool is_root();
	void set_parent(const _inode * n);
};

// fixed size node page
struct fsn_ipage: _ipage {
	struct head: _ipage::head {
		ushort node_size;
	}*header;

	inline virtual void ref(int off, char *buff, short w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual int assign_inode(_inode & n);
	virtual int read_inode(ushort idx, _inode &n);
	virtual int remove_inode(ushort idx);
	virtual bool is_root();

};

// variant size node page
struct vsn_ipage: _ipage {

};

// leaf node,
struct _lnode {
	struct head {
		char flag;
	}*header;

	char * buff;  // include key and value
	int len;
	_lpage * cp; // container page

	virtual void ref(char *buff, int len) {
	}
	virtual void init_header() {
	}
	virtual void write_key(const _key & k) {
	}
	virtual void write_key(const _key * k) {
	}
	virtual void write_val(const _val &v) {
	}
	virtual void write_val(const _val *v) {
	}
	virtual void read_key(_key &k) {
	}
	virtual void read_key(_key *k) {
	}
	virtual void read_val(_val & v) {
	}
	virtual void read_val(_val *v) {
	}
	int sort();

};

//fixed size leaf node
struct fs_lnode: _lnode {

};

//variant size leaf node
struct vs_lnode: _lnode {

};

struct _lpage: data_block {
	struct head: data_block::head {
		ushort node_count;
		uint parent_in_off;
		uint parent_ipg_off;
		ulong parent_pg_seg_id;
	}*header;

	_lpage *pre_page;
	_lpage *next_page;

	inline virtual void ref(int off, char *buff, short w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual int assign_lnode(_lnode & ln) {
		return 0;
	}
	virtual int remove_lnode(ushort idx) {
		return 0;
	}
	virtual int read_lnode(ushort idx, _lnode &ln);

	void set_parent(const _inode * n);
};

/*
 * fixed size page that only contains fixed size leaf node
 */
struct fsn_lpage: _lpage {
	struct head: _lpage::head {
		ushort node_size;
	}*header;

	inline virtual void ref(int off, char *buff, short w_len) {
		offset = off;
		length = w_len - sizeof(head);
		header = (head *) buff;
		header->node_count = 0;
		buffer = buff + sizeof(head);
		ref_flag = true;
	}

	virtual int assign_lnode(_lnode & ln);

	virtual int remove_lnode(ushort idx);

	virtual int read_lnode(ushort idx, _lnode &ln);
};

class bpt2 {
public:
	bpt2();
	virtual ~bpt2();
};

} /* namespace tree */
} /* namespace sdb */

#endif /* BPT2_H_ */
