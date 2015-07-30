/*
 * sys_seg.cpp
 *
 *  Created on: Jun 19, 2015
 *      Author: lqian
 */

#include "sys_seg.h"
#include "../common/sio.h"
#include "../common/char_buffer.h"

namespace sdb {
namespace enginee {

int sys_seg::add_row(const object_type & ot, row_store & rs) {
	common::char_buffer buff(1024);
	rs.write_to(buff);
	char * p = buff.data();
	int rl = buff.size();

	mem_data_block lblk, nblk; /* last block, new block */
	sys_seg seg;
	int r = seek_last_blk(ot, seg, lblk);
	if (r) {
		if (lblk.add_row_data(p, rl) == UNKNOWN_OFFSET) {
			//TODO maybe the segment is full, the issue also find in update_row
			if (seg == *this) {
				if (!assign_block(nblk)) {
					return IO_READ_BLOCK_ERROR;
				}
			} else {
				if (!seg.assign_block(nblk)) {
					return IO_READ_BLOCK_ERROR;
				}
			}
			lblk.set_next_blk(&nblk);
			nblk.set_pre_block(&lblk);
			if (nblk.add_row_data(p, rl) == UNKNOWN_OFFSET) {
				return ROW_DATA_SIZE_TOO_LARGE;
			} else {
				return sdb::SUCCESS;
			}
		} else {
			return sdb::SUCCESS;
		}
	} else {
		return r;
	}
}

int sys_seg::find_row(const object_type & ot, const string & obj_name,
		row_store & rs) {
	table_desc td;
	if (find_table_desc(ot, td)) {
		rs.set_table_desc(&td);
		field_desc fd = td.get_field_desc(obj_name);
		field_value fv;
		if (!fd.is_assigned()) {
			return NOT_FIND_FIELD_NAME;
		}

		mem_data_block blk;
		sys_seg seg;
		blk.offset = block_size * (int) ot;
		if (!read_block(blk)) {
			return IO_READ_BLOCK_ERROR;
		}

		while (find_row(blk, obj_name, fd, fv, rs) == ROW_NOT_IN_BLOCK) {
			if (blk.test_flag(NEXT_BLK_BIT)) {
				blk.offset = blk.header->next_blk_off;
				if (blk.test_flag(NEXT_BLK_SPAN_SEG_BIT)) {
					next_seg(seg);
					if (!seg.read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				} else {
					if (!read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				}
			} else {
				return ROW_NOT_FOUND;
			}
		}
	} else {
		return NO_SYS_TABLE_DESC;
	}
}

int sys_seg::update_row(const object_type & ot, const string & obj_name,
		row_store & rs) {
	row_store ors;
	table_desc td;
	sys_seg seg;
	if (find_table_desc(ot, td)) {
		ors.set_table_desc(&td);
		field_desc fd = td.get_field_desc(obj_name);
		field_value fv;
		if (!fd.is_assigned()) {
			return NOT_FIND_FIELD_NAME;
		}
		mem_data_block blk;
		blk.offset = block_size * (int) ot;
		int idx = ROW_NOT_IN_BLOCK;
		while ((idx = find_row(blk, obj_name, fd, fv, ors)) == ROW_NOT_IN_BLOCK) {
			if (blk.test_flag(NEXT_BLK_BIT)) {
				blk.offset = blk.header->next_blk_off;
				if (blk.test_flag(NEXT_BLK_SPAN_SEG_BIT)) {
					next_seg(seg);
					if (!seg.read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				} else {
					if (!read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				}
			} else {
				return ROW_NOT_FOUND;
			}
		}

		// update row
		common::char_buffer buff(1024);
		rs.write_to(buff);
		char *p = buff.data();
		int rl = buff.size();
		int r = blk.update_row_data_by_index(idx, p, rl);
		if (r == DELETE_OFFSET) {   // row move between block
			mem_data_block nblk;
			if (seg == *this) {
				if (!assign_block(nblk)) {
					return IO_READ_BLOCK_ERROR;
				}
			} else {
				if (!seg.assign_block(nblk)) {
					return IO_READ_BLOCK_ERROR;
				}
			}
			blk.set_next_blk(&nblk);
			nblk.set_pre_block(&blk);
			r = nblk.add_row_data(p, rl);
			if (r == UNKNOWN_OFFSET) {
				return ROW_DATA_SIZE_TOO_LARGE;
			}
			return r;
		} else {
			return r;
		}
	} else {
		return NO_SYS_TABLE_DESC;
	}
}

int sys_seg::delete_row(const object_type & ot, const string & obj_name) {
	row_store ors;
	table_desc  td;
	sys_seg seg;
	if (find_table_desc(ot, td)) {
		ors.set_table_desc(&td);
		field_desc fd = td.get_field_desc(obj_name);
		field_value fv;
		if (!fd.is_assigned()) {
			return NOT_FIND_FIELD_NAME;
		}
		mem_data_block blk;
		blk.offset = block_size * (int) ot;
		int idx = ROW_NOT_IN_BLOCK;
		while ((idx = find_row(blk, obj_name, fd, fv, ors)) == ROW_NOT_IN_BLOCK) {
			if (blk.test_flag(NEXT_BLK_BIT)) {
				blk.offset = blk.header->next_blk_off;
				if (blk.test_flag(NEXT_BLK_SPAN_SEG_BIT)) {
					next_seg(seg);
					if (!seg.read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				} else {
					if (!read_block(blk)) {
						return IO_READ_BLOCK_ERROR;
					}
				}
			} else {
				return ROW_NOT_FOUND;
			}
		}

		// delete row
		blk.delete_row_by_idx(idx);
		return idx;
	} else {
		return NO_SYS_TABLE_DESC;
	}
}

int sys_seg::find_row(mem_data_block &blk, const string & obj_name,
		const field_desc &fd, field_value & fv, row_store & rs) {
	blk.parse_off_tbl();
	common::char_buffer buff(block_size);
	for (int i = 0; i < blk.off_tbl.size(); i++) {
		if (blk.get_row(i, buff)) {
			rs.load_from(buff);
			buff.rewind();
			if (rs.get_field_value(fd, fv)) {
				if (memcmp(obj_name.c_str(), fv.cstr_val(), obj_name.size())
						== 0) {
					return i;
				}
			}
		}
	}
	return ROW_NOT_IN_BLOCK;
}

int sys_seg::find_table_desc(const object_type &ot, table_desc & td) {
	auto it = type_map.find(ot);
	if (it != type_map.end()) {
		td =  it->second;
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int sys_seg::seek_last_blk(const object_type &ot, sys_seg& seg,
		mem_data_block & blk) {
	blk.offset = block_size * (int) ot;
	if (read_block(blk)) {
		// last next block
		while (blk.test_flag(NEXT_BLK_BIT)) {
			blk.offset = blk.header->next_blk_off;
			if (blk.test_flag(NEXT_BLK_SPAN_SEG_BIT)) {
				next_seg(seg);
				if (!seg.read_block(blk)) {
					return IO_READ_BLOCK_ERROR;
				}
			} else {
				if (!read_block(blk)) {
					return IO_READ_BLOCK_ERROR;
				}
			}
		}
		return sdb::SUCCESS;
	} else {
		return IO_READ_BLOCK_ERROR;
	}
}

int sys_seg::initialize() {

	if (sio::exist_file(path_name)) {

		sys_df.open();

		//assign the first page for system table desc
		sys_df.assgin_segment(*this);

		mem_data_block fst_blk;
		assign_block(fst_blk);
		common::char_buffer buff(1024);
		append_schemas_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		buff.rewind();
		append_tables_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		buff.rewind();
		append_columns_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		buff.rewind();
		append_sequences_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		buff.rewind();
		append_indexes_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		buff.rewind();
		append_index_attrs_desc(buff);
		fst_blk.add_row_data(buff.data(), buff.size());

		//assign a empty block for each table desc
		mem_data_block empty_blk;
		for (int i = 0; i < page_map.size(); i++) {
			assign_block(empty_blk);
		}

		return flush();
	}
	return sdb::FAILURE;
}

int sys_seg::is_initialized() {
	std::string dfn = path_name;
	dfn.append("/");
	dfn.append(system_datafile_name);
	return sio::exist_file(dfn);
}

void sys_seg::get_table_desc(const string & name, table_desc & ptd) {
	auto it = name_map.find(name);
	if (it != name_map.end()) {
		ptd = it->second;
	}
}

int sys_seg::open() {
	std::string dfn = path_name;
	dfn.append("/");
	dfn.append(system_datafile_name);
	data_file sys_df(0L, dfn);
	sys_df.open();

	return sdb::FAILURE;
}

int sys_seg::clean() {
	std::string dfn = path_name;
	dfn.append("/");
	dfn.append(system_datafile_name);
	return sio::remove_file(dfn);
}

void sys_seg::append_schemas_desc(char_buffer & buff) {
	table_desc schema(SCHEMAS);
	field_desc fd0("schema_id", unsigned_int_type);
	field_desc fd1("schema_name", varchar_type, 64);
	field_desc fd2("version", unsigned_int_type);
	field_desc fd3("schema_comment", varchar_type, 2048, true);
	field_desc fd4("create_time", time_type);
	field_desc fd5("update_time", time_type, true);

	fd0.set_primary_key(true);
	fd0.set_auto_increment(true);

	schema.add_field_desc(fd0);
	schema.add_field_desc(fd1);
	schema.add_field_desc(fd2);
	schema.add_field_desc(fd3);
	schema.add_field_desc(fd4);
	schema.add_field_desc(fd5);

	schema.write_to(buff);
	name_map.insert(pair<string, table_desc>(SCHEMAS, schema));
	type_map.insert(pair<int, table_desc>(SCHEMA_OBJ, schema));
}

/*
 * * 2) tables: {table_id uint primary key auto increment, schema_id uint, table_name varchar(64), start_seg_id ulong ,
 * start_page_off uint, end_seg_id ulong, end_page_off uint, table_comment varchar(1024)}
 */
void sys_seg::append_tables_desc(char_buffer & buff) {
	table_desc tables(TABLES);

	field_desc fd1("table_id", unsigned_int_type);
	field_desc fd2("schema_id", unsigned_int_type);
	field_desc fd3("table_name", varchar_type, 64);
	field_desc fd4("start_seg_id", unsigned_long_type);
	field_desc fd5("start_page_off", unsigned_int_type);
	field_desc fd6("end_seg_id", unsigned_long_type);
	field_desc fd7("end_page_off", unsigned_int_type);
	field_desc fd8("table_comment", varchar_type, 1024, true);
	field_desc fd9("create_time", time_type);
	field_desc fda("update_time", time_type, true);

	fd1.set_primary_key(true);
	fd1.set_auto_increment(true);
	tables.add_field_desc(fd1);
	tables.add_field_desc(fd2);
	tables.add_field_desc(fd3);
	tables.add_field_desc(fd4);
	tables.add_field_desc(fd5);
	tables.add_field_desc(fd6);
	tables.add_field_desc(fd7);
	tables.add_field_desc(fd8);
	tables.add_field_desc(fd9);
	tables.add_field_desc(fda);

	tables.write_to(buff);
	name_map.insert(pair<string, table_desc>(TABLES, tables));
	type_map.insert(pair<int, table_desc>(TABLE_OBJ, tables));
}

/*
 * 4) columns {col_id ulong primary key auto increment, column_name varchar(32),
 * column_type short, inner_key ushort, null_able bool_type,  col_comment varchar(1024)}
 */
void sys_seg::append_columns_desc(char_buffer & buff) {
	table_desc columns(COLUMNS);

	field_desc fd1("col_id", unsigned_long_type, false, true);
	field_desc fd2("col_name", varchar_type, 32);
	field_desc fd3("col_type", short_type);
	field_desc fd4("inner_key", unsigned_short_type);
	field_desc fd5("null_able", bool_type);
	field_desc fd6("col_comment", varchar_type, 1024, true);
	field_desc fd7("create_time", time_type);
	field_desc fd8("update_time", time_type, true);

	fd1.set_auto_increment(true);
	columns.add_field_desc(fd1);
	columns.add_field_desc(fd2);
	columns.add_field_desc(fd3);
	columns.add_field_desc(fd4);
	columns.add_field_desc(fd5);
	columns.add_field_desc(fd6);
	columns.add_field_desc(fd7);
	columns.add_field_desc(fd8);

	columns.write_to(buff);
	name_map.insert(pair<string, table_desc>(COLUMNS, columns));
	type_map.insert(pair<int, table_desc>(COLUMN_OBJ, columns));
}

/*
 * 6) sequences: {sequence_name varchar(128), bind_tab_id  uint nullable,
 *     value_length short, curr_value char[8], next_value char(8), step int, create_time time_t)
 */
void sys_seg::append_sequences_desc(char_buffer & buff) {
	table_desc sequences(SEQUENCES);
	field_desc fd1("sequence_name", varchar_type, 128, false, true);
	field_desc fd2("bind_tab_id", unsigned_int_type, true);
	field_desc fd3("value_length", short_type);
	field_desc fd4("curr_value", char_type, 8);
	field_desc fd5("next_value", char_type, 8, true);
	field_desc fd6("step", int_type);
	field_desc fd7("create_time", time_type);
	field_desc fd8("update_time", time_type, true);
	field_desc fd9("seq_comment", varchar_type, 1024, true);

	sequences.add_field_desc(fd1);
	sequences.add_field_desc(fd2);
	sequences.add_field_desc(fd3);
	sequences.add_field_desc(fd4);
	sequences.add_field_desc(fd5);
	sequences.add_field_desc(fd6);
	sequences.add_field_desc(fd7);
	sequences.add_field_desc(fd8);
	sequences.add_field_desc(fd9);

	sequences.write_to(buff);
	name_map.insert(pair<string, table_desc>(SEQUENCES, sequences));
	type_map.insert(pair<int, table_desc>(SEQUENCE_OBJ, sequences));
}

/*
 * 3) indexes {index_id uint primary key auto increment, index_name varchar(64), on_table_id uint,
 *  index_type varchar(32), sort_order varchar(4), start_seg_id ulong, start_page_off uint,
 *  end_seg_id ulong, end_page_off uint, index_comment varchar(2048) }
 */
void sys_seg::append_indexes_desc(char_buffer & buff) {
	table_desc indexes(INDEXES);
	field_desc fd1("index_id", unsigned_int_type, false, true);
	field_desc fd2("index_name", varchar_type, 64);
	field_desc fd3("on_table_id", unsigned_int_type);
	field_desc fd4("index_type", varchar_type, 32);
	field_desc fd5("sort_order", varchar_type, 4);
	field_desc fd6("start_seg_id", unsigned_long_type);
	field_desc fd7("start_page_off", unsigned_int_type);
	field_desc fd8("end_seg_id", unsigned_long_type);
	field_desc fd9("end_page_off", unsigned_int_type);
	field_desc fda("index_comment", varchar_type, 2048, true);
	field_desc fdb("create_time", time_type);
	field_desc fdc("update_time", time_type, true);

	fd1.set_auto_increment(true);

	indexes.add_field_desc(fd1);
	indexes.add_field_desc(fd2);
	indexes.add_field_desc(fd3);
	indexes.add_field_desc(fd4);
	indexes.add_field_desc(fd5);
	indexes.add_field_desc(fd6);
	indexes.add_field_desc(fd7);
	indexes.add_field_desc(fd8);
	indexes.add_field_desc(fd9);
	indexes.add_field_desc(fda);
	indexes.add_field_desc(fdb);
	indexes.add_field_desc(fdc);

	indexes.write_to(buff);
	name_map.insert(pair<string, table_desc>(INDEXES, indexes));
	type_map.insert(pair<int, table_desc>(INDEX_OBJ, indexes));

}

/*
 * 5) index_attrs (index_attr_id ulong primary key auto increment, idx_col_name varchar(32), index_id uint ,
 * idx_col_order uint, create_time time, update_time time)
 */
void sys_seg::append_index_attrs_desc(char_buffer & buff) {
	table_desc index_attrs(INDEX_ATTRS);
	field_desc fd1("idx_attr_id", unsigned_long_type, false, true);
	field_desc fd2("idx_id", unsigned_int_type);
	field_desc fd3("idx_col_name", varchar_type, 32);
	field_desc fd4("idx_col_order", unsigned_int_type);
	field_desc fd5("create_time", time_type);
	field_desc fd6("update_time", time_type, true);

	index_attrs.add_field_desc(fd1);
	index_attrs.add_field_desc(fd2);
	index_attrs.add_field_desc(fd3);
	index_attrs.add_field_desc(fd4);
	index_attrs.add_field_desc(fd5);
	index_attrs.add_field_desc(fd6);

	index_attrs.write_to(buff);
	name_map.insert(pair<string, table_desc>(INDEX_ATTRS, index_attrs));
	type_map.insert(pair<int, table_desc>(INDEX_ATTR_OBJ, index_attrs));
}

sys_seg::sys_seg(const std::string pathname) {
	this->path_name = pathname;
	id = 0L;
	seg_type = sdb::storage::system_segment_type;
	length = M_64;
	block_size = K_8 * kilo_byte;

	std::string dfn = std::string(path_name);
	dfn.append("/");
	dfn.append(system_datafile_name);

	sys_df.set_id(1L);
	sys_df.set_path(dfn);

	page_map.insert(std::pair<string, int>(SCHEMAS, SCHEMA_OBJ));
	page_map.insert(std::pair<string, int>(TABLES, TABLE_OBJ));
	page_map.insert(std::pair<string, int>(COLUMNS, COLUMN_OBJ));
	page_map.insert(std::pair<string, int>(SEQUENCES, SEQUENCE_OBJ));
	page_map.insert(std::pair<string, int>(INDEXES, INDEX_OBJ));
	page_map.insert(std::pair<string, int>(INDEX_ATTRS, INDEX_ATTR_OBJ));
}

sys_seg::sys_seg() {
	id = 0L;
	seg_type = sdb::storage::system_segment_type;
	length = M_64;
	block_size = K_8 * kilo_byte;
}
sys_seg::~sys_seg() {
	if (this->d_file != nullptr) {
		d_file->close();
	}
}

} /* namespace enginee */
} /* namespace sdb */
