/*
 * table_desc.cpp
 *
 *  Created on: Jun 17, 2015
 *      Author: lqian
 */

#include "table_desc.h"
#include "../sdb_def.h"

namespace sdb {
namespace enginee {

using namespace std;

void table_desc::load_from(common::char_buffer & buffer) {
	int magic, sfc;
	fn_fd_map.clear();
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
				fn_fd_map.insert(pair<string, field_desc>(fd.field_name, fd));
				ik_fd_map.insert(
						pair<unsigned char, field_desc>(fd.inner_key, fd));
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
	auto it = fn_fd_map.find(field_name);
	if (it != fn_fd_map.end()) {

		unsigned char ik = it->second.inner_key;
		ik_fd_map.erase(it->second.inner_key);
		fn_fd_map.erase(it);

		for (auto itl = fd_list.begin(); itl != fd_list.end(); itl++) {
			if (itl->inner_key == ik) {
				itl->deleted = true;
			}
		}
		return sdb::SUCCESS;
	} else {
		return FIELD_NOT_EXISTED;
	}
}

int table_desc::delete_field(const unsigned char & ik) {
	auto it = ik_fd_map.find(ik);
	if (it != ik_fd_map.end()) {
		fn_fd_map.erase(it->second.field_name);
		ik_fd_map.erase(it);

		for (auto itl = fd_list.begin(); itl != fd_list.end(); itl++) {
			if (itl->inner_key == ik) {
				itl->deleted = true;
			}
		}
		return sdb::SUCCESS;
	} else {
		return FIELD_NOT_EXISTED;
	}
}

field_desc table_desc::get_field_desc(const std::string& s) {
	std::map<std::string, field_desc>::iterator its = fn_fd_map.find(s);
	if (its != fn_fd_map.end()) {
		return its->second;
	} else {
		field_desc empty;
		return empty;
	}
}

field_desc table_desc::get_field_desc(const unsigned char & ik) {
	auto it = ik_fd_map.find(ik);
	if (it != ik_fd_map.end()) {
		return it->second;
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
			fn_fd_map.insert(pair<std::string, field_desc>(fd.field_name, fd));
			ik_fd_map.insert(pair<unsigned char, field_desc>(fd.inner_key, fd));
			fd_list.push_back(fd);

			return sdb::SUCCESS;
		}
	}
}

int row_store::set_field_value(const unsigned char & ik, const char * val,
		const int & len, const bool verify) {
	if (verify) {
		field_desc fd = tdesc->get_field_desc(ik);
		if (fd.is_assigned()) {
			field_value fv(fd.field_type, val, len);
			fv_map.erase(fd.inner_key);
			fv_map.insert(pair<unsigned char, field_value>(ik, fv));
			set_bit(ik, true);
			return sdb::SUCCESS;
		} else {
			return INNER_KEY_INVALID;
		}
	}
}

int row_store::set_field_value(const field_desc & fd, const field_value & fv) {
	if (fd.is_assigned()) {
		if (tdesc->exists_field(fd)) {
			fv_map.erase(fd.inner_key);
			fv_map.insert(pair<unsigned char, field_value>(fd.inner_key, fv));
			set_bit(fd.inner_key, true);
			return sdb::SUCCESS;
		} else {
			return FIELD_NOT_EXISTED;
		}
	} else {
		return FIELD_NOT_ASSIGNED;
	}
}

int row_store::set_field_value(const unsigned char & ik, const field_value & fv,
		bool check) {
	if (check && !tdesc->exists_field(ik)) {
		return FIELD_NOT_EXISTED;
	}

	fv_map.erase(ik);
	fv_map.insert(pair<unsigned char, field_value>(ik, fv));
	set_bit(ik, true);
	return sdb::SUCCESS;

}

int row_store::set_field_value(const std::string name, const field_value & fv) {
	field_desc fd = tdesc->get_field_desc(name);
	if (fd.is_assigned()) {
		return set_field_value(fd.inner_key, fv);
	} else {
		return FIELD_NOT_EXISTED;
	}
}

int row_store::get_field_value(const unsigned char &ik, field_value & fv,
		bool check_exist) {
	if (check_exist && !tdesc->exists_field(ik)) {
		return FIELD_NOT_EXISTED;
	}

	auto it = fv_map.find(ik);
	if (it != fv_map.end()) {
		fv = it->second;
		return sdb::SUCCESS;
	} else {
		return FIELD_VALUE_NOT_FOUND;
	}

}

int row_store::get_field_value(const field_desc & fd, field_value & fv) {
	return fd.is_assigned() ?
			get_field_value(fd.inner_key, fv) : FIELD_NOT_ASSIGNED;
}

void row_store::delete_field(const unsigned char & ik) {
	if (tdesc->exists_field(ik)) {
		fv_map.erase(ik);
		set_bit(ik, false);
	}
}

void row_store::delete_field(const string & fn) {
	field_desc fd = tdesc->get_field_desc(fn);
	if (fd.is_assigned()) {
		delete_field(fd.inner_key);
	}
}

void row_store::set_bit(const unsigned char i, const bool v) {
	short p = i - 1;
	int d = p / 8;
	int r = p % 8;

	char n = 1 << r;
	if (v) {
		row_bitmap[d] |= n;
	} else {
		n = ~n;
		row_bitmap[d] &= n;
	}
}

bool row_store::test_bit(const unsigned char i) {
	int d = i / 8;
	int r = i % 8;
	char n = 1 << r;
	return (row_bitmap[d] & n) == n;
}

void row_store::init_row_bitmap() {
// initialize store field count and row bit map
	rbm_len = store_field_count / 8;
	if (store_field_count % 8 > 0) {
		rbm_len++;
	}
	row_bitmap = new char[rbm_len];
	memset(row_bitmap, 0, rbm_len);
}
/*
 * write logic header and field values in a row to char_buffer.
 * row_bitmap contains which field values need to be written to char_buffer
 * in order that make sure low inner_key field write first.
 */
void row_store::write_to(char_buffer & buff) {
	buff << store_field_count;
	buff.push_back(row_bitmap, rbm_len, false);
	for (auto it = fv_map.begin(); it != fv_map.end(); it++) {
		buff.push_back(it->second.value, it->second.len,
				it->second.is_variant());
	}
}

void row_store::load_from(char_buffer & buff) {
	buff >> store_field_count;
	init_row_bitmap();
	buff.pop_cstr(row_bitmap, rbm_len, false);

	field_desc fd;

	int i = 0;
	auto it = tdesc->fd_list.begin();
	for (; it != tdesc->fd_list.end() && i < store_field_count; it++, i++) {
		if (test_bit(i)) {
			fd = (*it);
			if (tdesc->exists_field(fd.inner_key)) {
				if (fd.is_variant()) {
					int len = buff.pop_int();
					field_value fv(fd.field_type, len);
					buff.pop_cstr(fv.value, fv.len, false);
					fv_map.insert(
							pair<unsigned char, field_value>(fd.inner_key, fv));

				} else {
					field_value fv(fd.field_type, fd.size);
					buff.pop_cstr(fv.value, fv.len, false);
					fv_map.insert(
							pair<unsigned char, field_value>(fd.inner_key, fv));
				}

			} else {  // skip deleted field
				if (fd.is_variant()) {
					int len = buff.pop_int();
					buff.skip(len);
				} else {
					buff.skip(fd.size); // fixed size field
				}
			}
		}
	}
}

} /* end namespace enginee */
} /* end namespace sdb */
