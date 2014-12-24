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
#include "table_description.h"

namespace enginee {

class field_description {
protected:
	std::string field_name;
	enginee::sdb_table_field_type field_type;
	std::string comment;
	bool deleted;
public:

	explicit field_description(const std::string & _field_name,
			const sdb_table_field_type _field_type,
			const std::string & _comment, const bool _deleted) :
			field_name(_field_name), field_type(_field_type), comment(
					_comment),deleted(_deleted) {
	}

	explicit field_description(const field_description & other) {
		this->field_name = other.field_name;
		this->field_type = other.field_type;
		this->comment = other.comment;
		this->deleted = other.deleted;
	}

	/**
	 * determine the field description object is empty
	 */
	const bool empty() {
		return field_type == unknow_type && field_name.size() == 0;
	}

	bool isDeleted() const {
		return deleted;
	}

	void setDeleted(bool deleted = false) {
		this->deleted = deleted;
	}

	const std::string& getComment() const {
		return comment;
	}

	void setComment(const std::string& comment) {
		this->comment = comment;
	}

	const std::string& getFieldName() const {
		return field_name;
	}

	void setFieldName(const std::string& fieldName) {
		field_name = fieldName;
	}

	sdb_table_field_type getFieldType() const {
		return field_type;
	}

	void setFieldType(sdb_table_field_type fieldType = unknow_type) {
		field_type = fieldType;
	}

	void operator=(const field_description & _fd) {

	}
};

}

#endif /* SDB_FIELD_DESCRIPTION_H_ */
