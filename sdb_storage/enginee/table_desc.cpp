/*
 * table_desc.cpp
 *
 *  Created on: Jun 17, 2015
 *      Author: lqian
 */

#include "table_desc.h"
#include "../storage/datafile.h"

namespace sdb {
namespace enginee {

using namespace std;

void table_desc::load_from(common::char_buffer & buffer) {
	int magic, sfc;
	fd_map.clear();
	fd_list.clear();

	buffer >> magic >> schema_name >> table_name >> comment >> deleted >> sfc;
	char_buffer fd_buff(buffer.remain());
	buffer >> fd_buff;
	if (magic == table_desc_magic) {
		for (int i = 0; i < sfc; i++) {
			field_desc fd;
			fd.load_from(fd_buff);
			fd_list.push_back(fd);
			if (!fd.deleted) {
				fd_map.insert(pair<string, field_desc>(fd.field_name, fd));
			}
		}
	}
}

void table_desc::write_to(common::char_buffer & buffer) {
	char_buffer fd_buff(1024);
	for (auto it = fd_list.begin(); it != fd_list.end(); it++) {
		it->write_to(fd_buff);
	}
	buffer << table_desc_magic << schema_name << table_name << comment
			<< deleted << store_field_count() << fd_buff;
}

int table_desc::delete_field(const std::string & field_name) {
	if (exists_field(field_name)) {
		fd_map.erase(field_name);
		for (auto it = fd_list.begin(); it != fd_list.end(); it++) {
			if (it->field_name == field_name) {
				it->deleted = true;
			}
		}
		return sdb::SUCCESS;
	} else {
		return FIELD_NOT_EXISTED;
	}
}

field_desc table_desc::get_field_desc(const std::string& s) {
	std::map<std::string, field_desc>::iterator its = fd_map.find(s);
	if (its != fd_map.end()) {
		return its->second;
	} else {
		field_desc empty;
		return empty;
	}
}

int table_desc::add_field_desc(field_desc & fd) {
	if (exists_field(fd)) {
		return FIELD_ALREADY_EXISTED;
	} else {
		if (store_field_count() >= MAX_FIELD_COUNT) {
			return FIELD_EXCEED_MAX;
		} else {
			fd.inner_key = fd_list.size() + 1;
			fd_map.insert(pair<std::string, field_desc>(fd.field_name, fd));
			fd_list.push_back(fd);

			return sdb::SUCCESS;
		}
	}
}
}
}
