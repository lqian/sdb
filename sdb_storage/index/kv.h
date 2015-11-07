/*
 * kv.h
 *
 *  Created on: Oct 29, 2015
 *      Author: lqian
 */

#ifndef KV_H_
#define KV_H_
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

struct _key_field {
	string field_name;
	field_type type;
	int field_len;
	char_buffer buffer;

	_key_field() {
	}
	_key_field(const _key_field & an) {
		field_name = an.field_name;
		type = an.type;
		field_len = an.field_len;
	}

	inline void ref(char*buff, int len) {
		buffer.ref_buff(buff, len);
	}

	inline int compare(_key_field & kf) {
		return compare(&kf);
	}

	inline int compare_short(sdb::index::_key_field* kf) {
		short v;
		buffer >> v;
		short t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_ushort(sdb::index::_key_field* kf) {
		ushort v;
		buffer >> v;
		ushort t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_uint(sdb::index::_key_field* kf) {
		uint v;
		buffer >> v;
		uint t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_long(sdb::index::_key_field*& kf) {
		long v;
		buffer >> v;
		long t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_ulong(sdb::index::_key_field* kf) {
		ulong v;
		buffer >> v;
		ulong t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_float(sdb::index::_key_field* kf) {
		float v;
		buffer >> v;
		float t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_double(sdb::index::_key_field* kf) {
		double v;
		buffer >> v;
		double t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare(_key_field * kf) {
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

	inline int compare_int(_key_field *kf) {
		int v;
		buffer >> v;
		int t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline int compare_varchar(_key_field *kf) {
		string v;
		buffer >> v;
		string t;
		kf->buffer >> t;
		return v.compare(t);
	}

	inline int compare_bool(_key_field *kf) {
		bool v;
		buffer >> v;
		bool t;
		kf->buffer >> t;
		return v > t ? 1 : v == t ? 0 : -1;
	}

	inline void write_bool(bool v) {
		buffer << v;
	}

	inline void write_short(short v) {
		buffer << v;
	}

	inline void write_ushort(ushort v) {
		buffer << v;
	}

	inline void write_int(int v) {
		buffer << v;
	}

	inline void write_uint(uint v) {
		buffer << v;
	}

	inline void write_long(long v) {
		buffer << v;
	}
	inline void write_ulong(ulong v) {
		buffer << v;
	}
	inline void write_float(float v) {
		buffer << v;
	}
	inline void write_double(double v) {
		buffer << v;
	}
	inline void write_varchar(char *buff, int len) {
		buffer.push_back(buff, len, true);
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
	/*
	 * total length for key fields, include length head if key field is variant.
	 */
	int len;

	list<_key_field> key_fields;

	inline void ref(char *buff, int len) {
		this->buff = buff;
		this->len = len;
	}

	inline int next_field(_key_field &kf) {
		return next_field(&kf);
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
				tmp.skip(l);
				kv_off += tmp.header();
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
}
}

#endif /* KV_H_ */
