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

namespace sdb {
namespace tree {
using namespace std;
using namespace sdb::enginee;

struct _page;

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
 * a index node only has store key
 */
struct _inode {
	struct head {
		short flag;
		uint left_pg_off;
		uint right_pg_off;
	}* header;

	char * buff = nullptr;
	int len = 0;

	_page * cp; // container page;

	virtual void ref(char *buff, int len);
	virtual void init_header();
	virtual void set_key(const _key & k);
	virtual void set_key(const _key * k);
};

struct fs_inode: _inode {

};

struct vs_inode: _inode {

};

// leaf node
struct _lnode: _inode {
	struct head: _inode::head {
		uint parent_pg_off;
	}*header;

	virtual void ref(char *buff, int len);
	virtual void ref(char *buff, int len);
	virtual void init_header();
	virtual void set_key(const _key & k);
	virtual void set_key(const _key * k);
	virtual void set_val(const _val &v);
	virtual void set_val(const _val *v);
};

//variant size node
struct vs_node {

};

//fixed size node
struct fs_node {

};

struct _page {

};

// variant size node page
struct vsn_page: _page {

};
// fixed size node page
struct fsn_page: _page {

};

class bpt2 {
public:
	bpt2();
	virtual ~bpt2();
};

} /* namespace tree */
} /* namespace sdb */

#endif /* BPT2_H_ */
