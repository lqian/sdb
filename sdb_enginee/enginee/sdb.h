/*
 * db.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef DB_H_
#define DB_H_

#include <stdlib.h>
#include <string>

namespace enginee {

const std::string sdb_meta_file(".sdb");
const std::string sdb_lock_file(".lock");

const int MAX_DB_NAME_SIZE(64);

enum sdb_status {
	sdb_unknown,
	sdb_opening = 1000,
	sdb_opened,
	sdb_ready,
	sdb_in_using,
	sdb_readonly,
	sdb_exited,
	sdb_failured = 2000
};

enum sdb_table_status {
	sdb_table_ready,
	sdb_table_opening,
	sdb_table_opend,
	sdb_table_in_using,
	sdb_table_open_failure = 2000,
	sdb_table_checksum_failure,
	sdb_table_other_failure
};

enum sdb_table_field_type {
	unknow_type =0x09,
	bool_type = 0x10,
	char_type,
	short_type,
	int_type,
	long_type,
	float_type,
	double_type,

	unsigned_char_type = 0x40,
	unsigned_short_type,
	unsigned_int_type,
	unsigned_long_type,
	unsigned_float_type,
	unsigned_double_type
};

class sdb {

private:
	// sdb files store in directory
	std::string dir;

	// sdb name, a directory only has one sdb instance
	std::string name;

	std::string full_path;

	std::string charset; // not support current 2014-12-12

	sdb_status status;

public:
	explicit sdb(const char *dir, const char * name);
	explicit sdb(const std::string & dir, const std::string &name);
	explicit sdb(const sdb& other) :
			dir(other.dir), name(other.name), charset(other.charset), status(
					other.status) {
		full_path = dir + "/" + name;
	}
	virtual ~ sdb();
	bool exists();
	bool init();
	bool open(const bool & read_only);
	bool close();
	const sdb_status& get_status();
	void change_status(sdb_status & status);

	bool exist_table(std::string &table_name);

	bool exist_table(const char * table_name);

	const std::string get_name() {
		return name;
	}

	const std::string get_dir() {
		return dir;
	}

	const std::string get_full_path() {
		return full_path;
	}

	bool equals(const sdb & other) {
		return (other.dir == dir) && (name == other.name);
	}

	 bool operator==(const sdb & b) {
		return equals(b);
	}
};

}

#endif /* DB_H_ */
