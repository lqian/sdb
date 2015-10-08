/*
 * sdb_table_description.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef SDB_TABLE_DESCRIPTION_H_
#define SDB_TABLE_DESCRIPTION_H_

#include <map>
#include <list>
#include "sdb.h"
#include "../common/char_buffer.h"
#include "field_value.h"

namespace sdb {
namespace enginee {

using namespace std;
using namespace common;

const string default_schema_name = "sdb";
static const int table_desc_magic = 0x890a10e6;
static const int block_head_pos = -1;

const int FIELD_ALREADY_EXISTED = -0x1000;
const int FIELD_NOT_EXISTED = -0x1001;
const int FIELD_EXCEED_MAX = -0x1002;
const int FIELD_NOT_ASSIGNED = -0x1003;
const int FIELD_VALUE_NOT_FOUND = -0x1004;

const int INNER_KEY_INVALID = -0x2000;

class table_desc;

class row_store;

class field_desc {
	friend class table_desc;
	friend class row_store;
private:
	unsigned char inner_key = unassign_inner_key;
	string field_name;
	sdb_table_field_type field_type = unknow_type;
	int size;
	string comment;
	bool deleted;

	bool primary_key = false;
	bool nullable = false;
	bool auto_increment = false;
public:

	field_desc() {
	}

	field_desc(const string & fn, const sdb_table_field_type ft, const bool na =
			false, const bool pk = false, const string & c = "", const bool d =
			false) :
			field_name(fn), field_type(ft), nullable(na), primary_key(pk), comment(
					c), deleted(d) {

		switch (field_type) {
		case bool_type:
			size = BOOL_CHARS;
			break;
		case short_type:
		case unsigned_short_type:
			size = SHORT_CHARS;
			break;
		case int_type:
		case unsigned_int_type:
			size = INT_CHARS;
			break;

		case time_type:
		case long_type:
		case unsigned_long_type:
			size = LONG_CHARS;
			break;

		case float_type:
		case unsigned_float_type:
			size = FLOAT_CHARS;
			break;
		case double_type:
		case unsigned_double_type:
			size = DOUBLE_CHARS;
			break;
		default:
			break;

		}
	}

	field_desc(const string & fn, const sdb_table_field_type ft, const int s,
			const bool na = false, const bool pk = false, const string & c = "",
			const bool d = false) :
			field_name(fn), field_type(ft), size(s), nullable(na), primary_key(
					pk), comment(c), deleted(d) {
	}

	void write_to(common::char_buffer &buff) {
		int ft = field_type;
		buff << inner_key << field_name << ft << size << comment << deleted
				<< nullable << primary_key;
	}

	void load_from(common::char_buffer & buff) {
		int ft;
		buff >> inner_key >> field_name >> ft >> size >> comment >> deleted
				>> nullable >> primary_key;
		field_type = (sdb_table_field_type) ft;
	}

	field_desc(const field_desc & other) {
		inner_key = other.inner_key;
		field_name = other.field_name;
		field_type = other.field_type;
		size = other.size;
		comment = other.comment;
		deleted = other.deleted;
	}

	/**
	 * determine the field description object is empty
	 */
	bool is_assigned() const {
		return inner_key > 0;
	}

	void operator=(const field_desc & other) {
		inner_key = other.inner_key;
		field_name = other.field_name;
		field_type = other.field_type;
		size = other.size;
		comment = other.comment;
		deleted = other.deleted;
	}

	friend bool operator==(const field_desc & a, const field_desc & b) {
		return a.inner_key == b.inner_key && a.field_name == b.field_name
				&& a.field_type == b.field_type && a.deleted == b.deleted
				&& a.comment == b.comment;
	}

	const string& get_comment() const {
		return comment;
	}

	void set_comment(const string& comment) {
		this->comment = comment;
	}

	bool is_deleted() const {
		return deleted;
	}

	void set_deleted(bool deleted) {
		this->deleted = deleted;
	}

	const string& get_field_name() const {
		return field_name;
	}

	void set_field_name(const string& fieldname) {
		field_name = fieldname;
	}

	const enginee::sdb_table_field_type get_field_type() const {
		return field_type;
	}

	void set_field_type(enginee::sdb_table_field_type fieldtype = unknow_type) {
		field_type = fieldtype;
	}

	short get_inner_key() const {
		return inner_key;
	}

	void set_inner_key(short innerkey) {
		inner_key = innerkey;
	}

	bool is_variant() const {
		return (field_type == char_type || field_type == varchar_type)
				|| field_type > varchar_type;
	}

	bool is_nullable() const {
		return nullable;
	}

	void set_nullable(bool nullable = true) {
		this->nullable = nullable;
	}

	bool is_primary_key() const {
		return primary_key;
	}

	void set_primary_key(bool primaryKey = false) {
		primary_key = primaryKey;
		if (primary_key) {
			nullable = false;
		}
	}

	bool is_auto_increment() const {
		return auto_increment;
	}

	void set_auto_increment(bool autoIncrement = false) {
		auto_increment = autoIncrement;
	}

	int get_size() const {
		return size;
	}
};

class table_desc {
	friend class field_desc;
	friend class row_store;
private:
	string schema_name;
	string table_name;
	string comment;

	//field name, field_desc  map, excludes deleted field_desc
	map<string, field_desc> fn_fd_map;

	//field inner key, field_desc map, excludes delete field_desc.
	// fn_fd_map and ik_fd_map must has the same content in literal.
	map<unsigned char, field_desc> ik_fd_map;

	// all field description, include deleted field description
	list<field_desc> fd_list;

	bool deleted = false;

public:

	table_desc() {
	}

	table_desc(string tname, string sname = default_schema_name,
			string c = "") {
		schema_name = sname;
		table_name = tname;
		comment = c;
	}

	/*
	 * load table description and its field description from a char buffer
	 */
	void load_from(common::char_buffer & buffer);

	/*
	 * write table description and its field description to a char buffer
	 */
	void write_to(common::char_buffer & buffer);

	int delete_field(const string & field_name);

	int delete_field(const unsigned char & ik);

	field_desc get_field_desc(const string& s);

	field_desc get_field_desc(const unsigned char & ik);

	int add_field_desc(field_desc & fd);

	bool operator==(const table_desc & other) {
		return schema_name == other.schema_name
				&& table_name == other.table_name && deleted == other.deleted
				&& fn_fd_map == other.fn_fd_map && fd_list == other.fd_list
				&& ik_fd_map == other.ik_fd_map;

	}

	bool exists_field(const string& field_name) {
		return fn_fd_map.size() > 0
				&& (fn_fd_map.find(field_name) != fn_fd_map.end());
	}

	bool exists_field(const field_desc & fd) {
		return exists_field(fd.field_name);
	}

	bool exists_field(const unsigned char & ik) {
		return ik_fd_map.size() > 0 && ik_fd_map.find(ik) != ik_fd_map.end();
	}

	const int field_count() const {
		return fn_fd_map.size();
	}

	const int store_field_count() const {
		return fd_list.size();
	}

	const string& get_comment() const {
		return comment;
	}

	void set_comment(const string& comment) {
		this->comment = comment;
	}

	bool is_deleted() const {
		return deleted;
	}

	void set_deleted(bool deleted = false) {
		this->deleted = deleted;
	}

	const string& get_table_name() const {
		return table_name;
	}
	const list<field_desc>& get_field_desc_list() const {
		return fd_list;
	}

	void operator=(const table_desc & other) {
		this->table_name = other.table_name;
		this->comment = other.comment;
		this->deleted = other.deleted;
		this->fd_list = other.fd_list;
		this->fn_fd_map = other.fn_fd_map;
		this->ik_fd_map = other.ik_fd_map;
		this->deleted = other.deleted;
	}
};

/*
 * a row store is a class that store a row data include row data header and all field values
 *
 * currently, the row_store DOES NOT validate type of value. for example, it a field_desc has
 * int_type, but field_value has a char array of '\0x31\0x32\0x33\0x43'.
 *
 * TODO, the feature should be implemented in field_value class self in future
 *
 * serialize a row_store into char_buffer or char array. the format as
 * |store_field_count| row_bit_map | {field value} .... |
 *
 * the sotre_field_count has one byte which value denotes the bytes of row_bit_map, for
 * example, the store_field_count is 9, the row_bit_map has two bytes.
 *
 */
class row_store {
private:
	/* total store field count when store a row via table_desc object,
	 after the row store persist on solid storage and the its
	 referenced table_desc object has changed, add/remove field_desc,*/
	unsigned char store_field_count;

	/* a few chars store the field_desc index in table_desc.fd_list,
	 * the bit map include deleted and active field_desc.
	 * LOW_ENDIAN, the first char denote 1-8, the second char denote 9-16 and etc */
	char * row_bitmap;

	int rbm_len;

	table_desc * tdesc = nullptr;

	/*
	 * a map store field_values
	 */
	map<unsigned char, field_value> fv_map;

	void set_bit(const unsigned char i, const bool v = true);
	bool test_bit(const unsigned char i);
	void init_row_bitmap();

public:
	int set_field_value(const unsigned char & ik, const char * val,
			const int & len, const bool verify = false);
	int set_field_value(const field_desc & fd, const field_value & fv);
	int set_field_value(const unsigned char & ik, const field_value & fv,
			bool check_exist = false);
	int set_field_value(const std::string name, const field_value & fv);
	int get_field_value(const unsigned char &ik, field_value & fv,
			bool check_exist = false);
	int get_field_value(const field_desc & fd, field_value & fv);

	/*
	 * delete field value from the row store, it's values is null after delete
	 */
	void delete_field(const unsigned char & ik);
	void delete_field(const string & field_name);

	void write_to(char_buffer & buff);
	void load_from(char_buffer & buff);

	row_store() {
	}

	row_store(unsigned char sfc) :
			store_field_count(sfc) {
		init_row_bitmap();
	}

	row_store(table_desc * tdesc) {
		this->tdesc = tdesc;
		store_field_count = tdesc->store_field_count();
		init_row_bitmap();
	}

	table_desc * get_table_desc() {
		return tdesc;
	}

	void set_table_desc(table_desc * tdesc) {
		this->tdesc = tdesc;
		store_field_count = tdesc->store_field_count();
		init_row_bitmap();
	}

	bool value_equals(const row_store & other) {
		if (store_field_count == other.store_field_count
				&& fv_map.size() == other.fv_map.size()) {
			auto sit = fv_map.begin();
			auto oit = other.fv_map.begin();
			bool equals = true;
			for (; equals && sit != fv_map.end() && oit != other.fv_map.end();
					++sit, ++oit) {
				equals = sit->first == oit->first && sit->second == oit->second;
			}
			return equals;
		}

		return false;

	}
};
}
}

#endif /* SDB_TABLE_DESCRIPTION_H_ */
