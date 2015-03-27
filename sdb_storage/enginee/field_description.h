/*
 * sdb_field_description.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef SDB_FIELD_DESCRIPTION_H_
#define SDB_FIELD_DESCRIPTION_H_

#include <stdlib.h>
#include <string>

namespace enginee {

class FieldDescription {

protected:
	short inner_key;
	std::string field_name;
	sdb_table_field_type field_type = unknow_type;
	int size;
	std::string comment;
	bool deleted;
public:

	const int unsign_inner_key = -1;

	FieldDescription() :
			inner_key(unsign_inner_key), deleted(false) {
	}

	explicit FieldDescription(const std::string & _field_name,
			const sdb_table_field_type _field_type,
			const std::string & _comment, const bool _deleted) :
			inner_key(unsign_inner_key), field_name(_field_name), field_type(
					_field_type), comment(_comment), deleted(_deleted) {
	}

	explicit FieldDescription(const std::string & _field_name,
			const sdb_table_field_type _field_type, const int & _size,
			const std::string & _comment, const bool _deleted) :
			inner_key(unsign_inner_key), field_name(_field_name), field_type(
					_field_type), size(_size), comment(_comment), deleted(
					_deleted) {
	}

	explicit FieldDescription(const FieldDescription & other) {
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
	const bool empty() {
		return field_type == unknow_type && field_name.size() == 0;
	}

	void operator=(const FieldDescription & other) {
		this->inner_key = other.inner_key;
		this->field_name = other.field_name;
		this->field_type = other.field_type;
		this->comment = other.comment;
		this->deleted = other.deleted;
	}

	friend bool operator==(const FieldDescription & a,
			const FieldDescription & b) {
		return a.field_name == b.field_name && a.field_type == b.field_type
				&& a.deleted == b.deleted && a.comment == b.comment;
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

	void set_deleted(bool deleted) {
		this->deleted = deleted;
	}

	const std::string& get_field_name() const {
		return field_name;
	}

	void set_field_name(const std::string& fieldname) {
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

}

#endif /* SDB_FIELD_DESCRIPTION_H_ */
