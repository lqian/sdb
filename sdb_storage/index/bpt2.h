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

#include "../enginee/trans_def.h"
#include "../storage/datafile.h"
#include "../common/char_buffer.h"
#include "../enginee/sdb.h"

namespace sdb {
namespace index {
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
	string field_name;
	field_type type;
	int field_len;
	char_buffer buffer;

	_key_field(){}
	_key_field(const _key_field & an) {
		field_name = an.field_name;
		type = an.type;
		field_len = an.field_len;
	}

	inline void ref(char*buff, int len) {
		buffer.ref_buff(buff, len);
	}

	int compare(_key_field & kf) {
		return compare(&kf);
	}

	int compare_short(sdb::index::_key_field* kf) {
		short v;
		buffer >> v;
		short t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_ushort(sdb::index::_key_field* kf) {
		ushort v;
		buffer >> v;
		ushort t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_uint(sdb::index::_key_field* kf) {
		uint v;
		buffer >> v;
		uint t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_long(sdb::index::_key_field*& kf) {
		long v;
		buffer >> v;
		long t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_ulong(sdb::index::_key_field* kf) {
		ulong v;
		buffer >> v;
		ulong t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_float(sdb::index::_key_field* kf) {
		float v;
		buffer >> v;
		float t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_double(sdb::index::_key_field* kf) {
		double v;
		buffer >> v;
		double t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare(_key_field * kf) {
		switch (type) {
		case bool_type:
			return compare_bool(kf);
		case short_type:
			return compare_short(kf);
		case ushort_type:
			return compare_ushort(kf);
		case int_type:
			return compare_int(kf);
		case uint_type:
			return compare_uint(kf);
		case long_type:
			return compare_long(kf);
		case ulong_type:
			return compare_ulong(kf);
		case float_type:
			return compare_float(kf);
		case double_type:
			return compare_double(kf);
		case varchar_type:
			return compare_varchar(kf);
		default:
			return 0;
		}
	}

	int compare_int(_key_field *kf) {
		int v;
		buffer >> v;
		int t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	int compare_varchar(_key_field *kf) {
		string v;
		buffer >> v;
		string t;
		kf->buffer >> t;
		return v.compare(t);
	}

	int compare_bool(_key_field *kf) {
		bool v;
		buffer >> v;
		bool t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline bool operator==(const _key_field & an) {
		return an.field_name == field_name && type == an.type;
	}

	inline bool is_variant() {
		return type >= field_type::char_type;
	}
};



struct _key {
	char * buff;
	int kv_off = 0;
	int len;

	list<_key_field> key_fields;
	inline void ref(char *buff, int len) {
		this->buff = buff;
		this->len = len;
	}

	inline int next_field(_key_field &kf) {
		int r = sdb::SUCCESS;
		if (kv_off >= len) {
			r = sdb::FAILURE;
		} else {
			kf.ref(buff + kv_off, len - kv_off);
			if (kf.is_variant()) {
				char_buffer tmp;
				tmp.ref_buff(buff + kv_off, len - kv_off);
				int l;
				tmp >> l;
				kv_off += l + INT_CHARS;
			} else {
				kv_off += kf.field_len;
			}
		}
		return r;
	}

	inline int next_field(_key_field * kf) {
		int r = sdb::SUCCESS;
		if (kv_off >= len) {
			r = sdb::FAILURE;
		} else {
			kf->ref(buff + kv_off, len - kv_off);
			if (kf->is_variant()) {
				char_buffer tmp;
				tmp.ref_buff(buff + kv_off, len - kv_off);
				int l;
				tmp >> l;
				kv_off += l + INT_CHARS;
			} else {
				kv_off += kf->field_len;
			}
		}
		return r;
	}

	inline bool equal_fields(const _key & an) {
		bool equals = true;
		auto it = key_fields.begin();
		auto ait = an.key_fields.begin();
		for (; equals && it != key_fields.end() && ait != an.key_fields.end();
				++it, ++ait) {
			equals = it->field_name == ait->field_name && it->type == ait->type;
		}

		return equals;
	}

	/*
	 * the method assume that the field number equals, although a field has null value
	 */
	inline int compare(_key & an) {
		int r = 0;
		auto it = key_fields.begin();
		auto ait = an.key_fields.begin();
		_key_field th, ak;
		for (; r == 0 && it != key_fields.end() && ait != an.key_fields.end();
				it++, ait++) {
			th = *it;
			ak = *ait;
			if (next_field(th) == sdb::SUCCESS
					&& an.next_field(ak) == sdb::SUCCESS) {
				r = th.compare(ak);
			}
		}
		return r;
	}

	inline void add_key_field(const _key_field & kf) {
		for (auto & e : key_fields) {
			if (e == kf) {
				return;
			}
		}
		key_fields.push_back(kf);
	}
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
	virtual int read_key(_key *k) {
		return 0;
	}

	void set_left_lpage(_lpage * p);
	void set_right_lpage(_lpage *p);
	void set_left_ipage(_ipage *p);
	void set_right_ipage(_ipage *p);
};

/*
 * fixed size index node
 */
struct fs_inode: virtual _inode {
	virtual void ref(uint off, char *buff, int len) {
		header = (head *) buff;
		offset = off;
		this->buff = buff + sizeof(head);
		this->len = len - sizeof(head);
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
	virtual int read_key(_key *k) {
		return 0;
	}
};
//
//struct vs_inode: _inode {
//
//};

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

	virtual int assign_inode(_inode & n) {
	}
	virtual int read_inode(ushort idx, _inode &n) {
	}
	virtual int remove_inode(ushort idx) {
	}
	bool is_root() {
		return false;
	}
	;
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

};

// variant size node page
//struct vsn_ipage: _ipage {
//
//};

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
	virtual int read_lnode(ushort idx, _lnode &ln) {
		return 0;
	}

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
