/*
 * table_description.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#include <fstream>
#include "../common/encoding.h"
#include "../common/sio.h"
#include "table_description.h"
#include "field_description_store.h"

namespace enginee {

using namespace std;

bool TableDescription::exists() {
	return sdb_io::exist_file(file_name);
}

bool TableDescription::load_from_file() {
	bool loaded = false;
	if (exists()) {
		ifstream in(file_name.c_str(), ios_base::in | ios_base::binary);
		if (in.is_open()) {

			//read all stream to char_buffer;
			in.seekg(0, ifstream::end);
			int len = in.tellg();
			in.seekg(0, ifstream::beg);
			char *p = new char[len];
			in.read(p, len);
			in.close();

			common::char_buffer buffer(p, len);

			int t_magic;
			buffer >> t_magic;
			if (t_magic == magic) {
				field_desc_map.clear();
				field_desc_list.clear();
			} else {
				throw "invalid table description file";
			}

			// ignore the table name
			int field_size;
			std::string t_table_name;
			buffer >> t_table_name >> deleted >> field_size >> comment;

			for (int i = 0; i < field_size; i++) {
				common::char_buffer field_buffer(512);
				if (buffer.pop_char_buffer(&field_buffer)) {
					FieldDescriptionStore fds;
					fds.set_store(field_buffer);
					fds.read_store();

					// push back to the list in spit of its status
					field_desc_list.push_back(fds);

					//add valid field to map
					if (!exists_field(fds) && !fds.is_deleted()) {
						std::string k = fds.get_field_name();
						field_desc_map.insert(std::pair<std::string, FieldDescription>(k, fds));
					}

				} else {
					throw "read field content error";
				}
			}

			in.close();

			loaded = true;
		}
	}
	return loaded;
}

bool TableDescription::write_to_file(bool refresh) {
	bool writed = false;
	if (refresh || !exists()) {

		std::ofstream out(file_name.c_str(), ios_base::out | ios_base::binary);

		if (out.is_open()) {
			common::char_buffer table_desc_buffer(512);

			int fs = field_desc_list.size();
			table_desc_buffer << magic << table_name << deleted << fs << comment;

			int pos = table_desc_buffer.size();
			int pre_pos;
			int next_pos;
			int i = 0;

			for (std::list<FieldDescription>::iterator it = field_desc_list.begin(); it != field_desc_list.end(); it++) {

				FieldDescriptionStore fds = FieldDescriptionStore(*it);

				int bs = fds.evaluate_block_size();
				if (i == 0) {
					pre_pos = block_head_pos;
					next_pos = pos + bs;
				}

				fds.set_pre_store_block_pos(pre_pos);
				fds.set_next_store_block_pos(next_pos);
				fds.fill_store();

				// push back field block store table description buffer
				table_desc_buffer << fds.char_buffer();

				pre_pos = pos;
				pos = next_pos;
			}

			out.write(table_desc_buffer.data(), table_desc_buffer.size());
			out.close();
			writed = true;
		}

	}
	return writed;
}

void TableDescription::delete_field(const string & field_name) {
	// delete from map
	field_desc_map.erase(field_name);

	for (std::list<FieldDescription>::iterator it = field_desc_list.begin(); it != field_desc_list.end(); it++) {
		if (it->get_field_name() == field_name && !it->is_deleted()) {
			it->set_deleted(true);
		}
	}
}
bool TableDescription::update_exist_file() {
	bool updated = false;
	return updated;
}

bool TableDescription::operator ==(const TableDescription & b) {
	return this->m_sdb == b.m_sdb && this->table_name == b.table_name && this->deleted == b.deleted
			&& this->field_desc_list == b.field_desc_list;

}

}

