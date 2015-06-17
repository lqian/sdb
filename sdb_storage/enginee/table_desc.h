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
public:

	field_desc() {
	}

	field_desc(const string & _field_name,
			const sdb_table_field_type _field_type, const string & _comment,
			const bool _deleted = false) :
			inner_key(unassign_inner_key), field_name(_field_name), field_type(
					_field_type), comment(_comment), deleted(_deleted) {
	}

	field_desc(const string & _fn, const sdb_table_field_type _ft,
			const int & _s, const string & _c, const bool _d = false) :
			inner_key(unassign_inner_key), field_name(_fn), field_type(_ft), size(
					_s), comment(_c), deleted(_d) {
	}

	void write_to(common::char_buffer &buff) {
		int ft = field_type;
		buff << inner_key << field_name << ft << size << comment << deleted;
	}

	void load_from(common::char_buffer & buff) {
		int ft;
		buff >> inner_key >> field_name >> ft >> size >> comment >> deleted;
		field_type = (sdb_table_field_type) ft;
	}

	field_desc(const field_desc & other) {
		this->inner_key = other.inner_key;
		this->field_name = other.field_name;
		this->field_type = other.field_type;
		this->size = other.size;
		this->comment = other.comment;
		this->deleted = other.deleted;
	}

	/**
	 * determine the field description object is empty
	 */
	const bool is_assigned() {
		return inner_key > 0;
	}

	void operator=(const field_desc & other) {
		this->inner_key = other.inner_key;
		this->field_name = other.field_name;
		this->field_type = other.field_type;
		this->comment = other.comment;
		this->deleted = other.deleted;
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
};

class table_desc {
	friend class field_desc;
	friend class row_store;
private:
	string schema_name;
	string table_name;
	string comment;

	//description map, excludes deleted field_desc
	map<string, field_desc> fd_map;

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

	field_desc get_field_desc(const string& s);

	int add_field_desc(field_desc & fd);

	bool operator==(const table_desc & other) {
		return schema_name == other.schema_name
				&& table_name == other.table_name && deleted == other.deleted
				&& fd_map == other.fd_map && fd_list == other.fd_list;

	}

	bool exists_field(const string& field_name) {
		return fd_map.size() > 0 && (fd_map.find(field_name) != fd_map.end());
	}
	bool exists_field(const field_desc & fd) {
		return fd_map.size() > 0
				&& (fd_map.find(fd.get_field_name()) != fd_map.end());
	}

	const int field_count() const {
		return fd_map.size();
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
		this->fd_map = other.fd_map;
	}
};

/*
 * a row store is a class that store a row data include row data header and all field values
 */
class row_store {
private:
	/* total store field count when store a row via table_desc object,
	 after the row store persist on solid storage and the its
	 referenced table_desc object has changed, add/remove field_desc,*/
	unsigned char store_field_count;

	/* a few chars store the field_desc index in table_desc.fd_list,
	 * the bit map include deleted and active field_desc. */
	char * row_bit_map;

	table_desc * tdesc;


public:

	void init_header();

	int set_field_value(const unsigned char & ik, const char * val, const int & len ) ;
	int set_field_value(const field_desc & fd, const field_value & fv) ;
	int get_field_value(const unsigned char &ik, field_value & fv);
	int get_field_value(const field_desc & fd, field_value & fv);

};
}
}

#endif /* SDB_TABLE_DESCRIPTION_H_ */
