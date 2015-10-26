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

namespace sdb {
namespace tree {
using namespace std;
using namespace sdb::enginee;
using namespace sdb::storage;

struct _ipage;
struct _lpage;

struct _key_field {
	void ref(char*buff, int len);
	virtual int compare(const _key_field & kf);
	virtual ~_key_field() {
	}
};

struct _key {
	list<_key_field> kfs;
	void ref(char *buff, int len);
	int compare(const _key & an);
	void add_field(const _key_field & kf);
};

struct _val {
	char * buff;
	int len;

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
		short flag;
		uint left_lpg_off;
		uint right_lpg_off;
	}* header;

	uint offset;
	char * buff = nullptr;
	int len = 0;

	_ipage * cp; // container page;
	_lpage * left_page; // inode's left page that contains node's key less than or equals the inode
	_lpage * right_page; // inode's right page that contains node's key greater than the inode

	virtual void ref(uint offset, char *buff, int len);
	virtual void init_header();
	virtual void set_key(const _key & k);
	virtual void set_key(const _key * k);
};

struct _ipage: data_block {
	struct head: data_block::head {
		ushort node_count;
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
	virtual int write_inode(ushort idx, const _inode &n);
	virtual int add_inode(const _inode &n);
	virtual int remove_inode(ushort idx);
	virtual bool is_root();
};

// fixed size node page
struct fsn_ipage: _ipage {

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

	virtual void ref(char *buff, int len);
	virtual void ref(char *buff, int len);
	virtual void init_header();
	virtual void set_key(const _key & k);
	virtual void set_key(const _key * k);
	virtual void set_val(const _val &v);
	virtual void set_val(const _val *v);
};

struct _lpage: data_block {
	struct head: data_block::head {
		ushort node_count;
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

	virtual int assign_lnode(_lnode & ln) ;

	virtual int remove_lnode(ushort idx) ;
};

struct fs_inode: _inode {

};

struct vs_inode: _inode {

};

//variant size leaf node
struct vs_lnode: _lnode {

};

//fixed size leaf node
struct fs_node: _lnode {

};

class bpt2 {
public:
	bpt2();
	virtual ~bpt2();
};

} /* namespace tree */
} /* namespace sdb */

#endif /* BPT2_H_ */
