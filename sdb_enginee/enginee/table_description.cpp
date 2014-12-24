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

bool table_description::exists() {
	return sdb_io::exist_file(file_name);
}

bool table_description::load() {
	bool loaded = false;
	if (exists()) {

	}
	return loaded;
}

bool table_description::write() {
	bool writed = false;
	if (!exists()) {

		std::ofstream out(file_name, ios_base::out | ios_base::binary);
		if (out.is_open()) {
			out << magic << table_name.size() << table_name << deleted;
			out << field_description_map.size();

			int pos = INT_CHARS * 3 + table_name.size() + BOOL_CHARS;
			int pre_pos;
			int next_pos;
			int i = 0;
			for (std::pair<std::string, field_description_store>  element : field_description_map) {
				field_description_store fds = field_description_store(
						element.second);

				int bs = fds.evaluate_block_size();
				if (i == 0) {
					pre_pos = block_head_pos;
					next_pos = pos + bs;
				}

				fds.set_pre_store_block_pos(pre_pos);
				fds.set_next_store_block_pos(next_pos);
				out.write(fds.char_buffer().data(), (long)fds.block_size());

				pre_pos = pos;
				pos = next_pos;
			}

			out.close();
			writed = true;
		}

	}
	return writed;
}
bool table_description::update() {
	bool updated = false;
	return updated;
}
}

