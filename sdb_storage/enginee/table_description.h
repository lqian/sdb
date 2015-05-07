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
#include "field_description.h"

namespace enginee {

static const int magic = 0x890a10e6;

static const int block_head_pos = -1;

using namespace std;

class TableDescription {

private:
	sdb m_sdb;
	std::string table_name;
	std::string comment;

	//not deleted field description map
	map<std::string, FieldDescription> field_desc_map;

	// all field description, include deleted field description
	list<FieldDescription> field_desc_list;

	// file stored the table description with binary format
	std::string file_name;

	bool deleted;

public:

	TableDescription() {

	}
	explicit TableDescription(const sdb & _sdb, const char * _table_name) :
			m_sdb(_sdb), table_name(_table_name), deleted(false) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit TableDescription(const sdb & _sdb, const std::string & _table_name) :
			m_sdb(_sdb), table_name(_table_name), deleted(false) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit TableDescription(const sdb & _sdb, const std::string & _table_name,
			const std::string & _comment) :
			m_sdb(_sdb), table_name(_table_name), comment(_comment), deleted(
					false) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit TableDescription(const TableDescription & other) :
			m_sdb(other.m_sdb) {
		table_name = other.table_name;
		comment = other.comment;
		deleted = other.deleted;
		field_desc_map = other.field_desc_map;
		field_desc_list = other.field_desc_list;
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	~TableDescription() {

	}

	bool load_from_file();
	bool write_to_file(bool refresh);
	bool update_exist_file();
	bool exists();
	void delete_field(const std::string & field_name);

	bool operator==(const TableDescription & other);

	bool exists_field(const std::string& field_name) {
		return field_desc_map.size() > 0
				&& (field_desc_map.find(field_name) != field_desc_map.end());
	}
	bool exists_field(const FieldDescription & _field_description) {
		return field_desc_map.size() > 0
				&& (field_desc_map.find(_field_description.get_field_name())
						!= field_desc_map.end());
	}

	const FieldDescription & get_field_description(const std::string& s) {
		std::map<std::string, FieldDescription>::iterator its =
				field_desc_map.find(s);
		if (its != field_desc_map.end()) {
			return its->second;
		} else {
			return FieldDescription();
		}
	}

	void add_field_description(FieldDescription & fd) ;

	void load_indexes();

	const int field_count() const {
		return field_desc_map.size();
	}

	const std::string& get_comment() const {
		return comment;
	}

	void set_comment(const std::string& comment) {
		this->comment = comment;
	}

	bool is_deleted() const {
		return deleted;
	}

	const std::map<std::string, FieldDescription>& get_field_description_map() const {
		return field_desc_map;
	}

	const sdb& get_sdb() const {
		return m_sdb;
	}

	void set_sdb(const sdb& sdb) {
		m_sdb = sdb;
	}

	void set_deleted(bool deleted = false) {
		this->deleted = deleted;
	}

	const std::string& get_file_name() const {
		return file_name;
	}

	void set_file_name(const std::string& tableFileName) {
		file_name = tableFileName;
	}

	const std::string& get_table_name() const {
		return table_name;
	}

	void set_table_name(const std::string& tableName) {
		table_name = tableName;
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	const std::list<FieldDescription>& get_field_desc_list() const {
		return field_desc_list;
	}

	void operator=(const TableDescription & other) {
		this->m_sdb = other.m_sdb;
		this->table_name = other.table_name;
		this->comment = other.comment;
		this->deleted = other.deleted;
		this->file_name = other.file_name;
		this->field_desc_list = other.field_desc_list;
		this->field_desc_map = other.field_desc_map;
	}
};
}

#endif /* SDB_TABLE_DESCRIPTION_H_ */
