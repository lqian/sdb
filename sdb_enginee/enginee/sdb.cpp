/*
 * sdb.cpp
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <errno.h>

#include "table_description.h"
#include "sdb.h"

using namespace std;
using namespace enginee;

sdb::sdb(const std::string &dir, const std::string &name) {
	this->name = name;
	this->dir = dir;
	this->full_path = dir + "/" + name;
	this->status = sdb_ready;
}

sdb::sdb(const char *dir, const char* name) {
	this->name = std::string(name);
	this->dir = std::string(dir);
	this->full_path = this->dir + "/" + this->name;
	this->status = sdb_ready;
}

sdb::~sdb() {

}

bool sdb::exists() {
	DIR *p_dir = opendir(full_path.c_str());
	if (p_dir != NULL) {
		closedir(p_dir);

		ifstream in_stream;
		in_stream.open(full_path + "/" + sdb_meta_file, ios_base::in);
		char * db_name = new char[MAX_DB_NAME_SIZE];
		in_stream.getline(db_name, MAX_DB_NAME_SIZE);

		return strcmp(db_name, this->name.c_str()) == 0;
	}
	return false;
}

bool sdb::open(const bool & read_only) {
	bool success = true;
	ifstream in_stream;

	in_stream.open(full_path + "/" + sdb_meta_file, ios_base::in);
	char * db_name = new char[MAX_DB_NAME_SIZE];
	in_stream.getline(db_name, MAX_DB_NAME_SIZE);

	if (strlen(db_name) != 0) {
		this->name = std::string(db_name);
		this->status = sdb_opening;
	} else {
		success = false;
	}
	in_stream.close();

	// change the db status to sdb_opened
	this->status = sdb_opened;
	return success;
}

bool sdb::init() {
	bool success = true;
	int r = mkdir(full_path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
	if (r == -1) {
		perror(("sdb error: [" + dir + "]").c_str());
		success = false;
	} else {

		ofstream out;
		out.open(full_path + "/" + sdb_meta_file);

		if (!out.is_open()) {
			success = false;
		} else {
			out << name << endl;
			out.flush();
		}
		out.close();
	}
	return success;
}



bool sdb::exist_table(const char * table_name) {
	table_description tbl_desc(*this, table_name);
	return tbl_desc.exists();
}

bool sdb::exist_table(std::string & table_name) {
	return exist_table(table_name.c_str());
}

