/*
 * sdb_table_description.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef SDB_TABLE_DESCRIPTION_H_
#define SDB_TABLE_DESCRIPTION_H_

#include "sdb.h"
#include "field_description.h"
#include <map>

namespace enginee {

using namespace std;
class table_description {

	const static int magic = 0x890a10e6;

	const static int block_head_pos = -1;

private:
	sdb m_sdb;
	std::string table_name;
	std::string comment;
	std::map<std::string, field_description> field_description_map;

	// file stored the table description with binary format
	std::string file_name;

	bool deleted;
	sdb_table_status status;

public:

	explicit table_description(const sdb & _sdb, const char * _table_name) :
			m_sdb(_sdb), table_name(_table_name), deleted(false), status(sdb_table_ready) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit table_description(const sdb & _sdb, const std::string & _table_name) :
			m_sdb(_sdb), table_name(_table_name), deleted(false), status(sdb_table_ready) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit table_description(const sdb & _sdb, const std::string & _table_name, const std::string & _comment) :
			m_sdb(_sdb), table_name(_table_name), comment(_comment), deleted(false), status(sdb_table_ready) {
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	explicit table_description(const table_description & other) :
			m_sdb(other.m_sdb) {
		table_name = other.table_name;
		comment = other.comment;
		status = other.status;
		deleted = other.deleted;
		field_description_map = other.field_description_map;
		file_name = m_sdb.get_full_path() + "/" + table_name.c_str() + ".td";
	}

	~table_description() {

	}

	bool load();
	bool write();
	bool update();
	bool exists();

	bool exists_field(std::string& field_name) {
		return field_description_map.find(field_name) != field_description_map.end();
	}
	bool exists_field(field_description & _field_description) {
		return field_description_map.find(_field_description.getFieldName()) != field_description_map.end();
	}

//	const field_description & get_field_description(std::string & field_name) {
//		auto its = field_description_map.find(field_name);
//		if (its != field_description_map.end()) {
//			return its->second;
//		} else {
//			return field_description(this);
//		}
//	}

	bool is_delete() const {
		return deleted;
	}

	void set_deleted(bool deleted = false) {
		this->deleted = deleted;
	}

	sdb_table_status getStatus() const {
		return status;
	}

	void setStatus(sdb_table_status status = sdb_table_ready) {
		this->status = status;
	}

	const std::string& getTableFileName() const {
		return file_name;
	}

	void setTableFileName(const std::string& tableFileName) {
		file_name = tableFileName;
	}

	const std::string& getTableName() const {
		return table_name;
	}

	void setTableName(const std::string& tableName) {
		table_name = tableName;
	}

	void add_field_description(field_description & _field_description) {
		if (exists_field(_field_description)) {
//			field_description_map[_field_description.getFieldName()] = field_description(_field_description);
			std::string k = _field_description.getFieldName();
			field_description_map.insert(std::pair<std::string, field_description>(k, _field_description));
		}
	}

	void load_indexes();

};
}

#endif /* SDB_TABLE_DESCRIPTION_H_ */
