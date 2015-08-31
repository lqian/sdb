/*
 * sys_seg.h
 *
 *  Created on: Jun 19, 2015
 *      Author: lqian
 */

#ifndef SYS_SEG_H_
#define SYS_SEG_H_

#include <map>
#include "table_desc.h"
#include "../storage/datafile.h"

namespace sdb {
namespace enginee {

enum object_type {
	SCHEMA_OBJ = 1,
	TABLE_OBJ,
	COLUMN_OBJ,
	SEQUENCE_OBJ,
	INDEX_OBJ,
	INDEX_ATTR_OBJ
};

using namespace std;
using namespace sdb::storage;

const std::string system_datafile_name = "sys.df";

/*
 * system segment is a special type of segment, it stores both system table desc and
 * their row data. system table desc list with persuod sql
 *
 * 1) schemas: {schema_id uint primary key auto increment, schema_name varchar(64) primary key, version uint, schema_comment varchar(2048), create_time time }
 *
 * 2) tables: {table_id uint primary key auto increment, schema_id uint, table_name varchar(64), start_seg_id ulong ,
 * start_page_off uint, end_seg_id ulong, end_page_off uint, table_comment varchar(1024)}
 *
 * 3) indexes {index_id uint primary key auto increment, index_name varchar(64), on_table_id uint,
 *  index_type varchar(32), sort_order varchar(4), start_seg_id ulong, start_page_off uint,
 *  end_seg_id ulong, end_page_off uint, index_comment varchar(2048) }
 *
 * 4) columns {col_id ulong primary key auto increment, column_name varchar(32),
 * column_type short, inner_key ushort, null_able char(1),  col_comment varchar(1024)}
 *
 * 5) index_attrs (index_attr_id ulong primary key auto increment, idx_col_name varchar(32),
 * idx_col_order uint, create_time time, update_time time)
 *
 * 6) sequences: {sequence_name varchar(128), bind_tab_name varchar(64) nullable,
 *     value_length short, curr_value char[8], next_value char(8), step int, create_time time_t)
 *
 *
 * system segment can contains two types of page, system table desc meta page id is defined as 0 variant size page.
 * system table row data start the end of it. each system table's data start in a new page.  data page of a system table
 * is chain after assign it.
 *
 * system segment only persist on master node.
 *
 */
class sys_seg: public segment {

	const int NO_SYS_TABLE_DESC = -0x400;
	const int NOT_FIND_FIELD_NAME = 0x401;

	const std::string SCHEMAS = "schemas";
	const std::string TABLES = "tables";
	const std::string COLUMNS = "columns";
	const std::string SEQUENCES = "sequences";
	const std::string INDEXES = "indexes";
	const std::string INDEX_ATTRS = "index_attrs";

public:

	/*
	 * initialize system table desc in the data file named sys.df
	 */
	int initialize();
	int is_initialized();
	int open();
	int clean();

	int add_row(const object_type & ot, row_store & rs);

	/*
	 * find an object type of table in sys_seg with a column name and its value equals a field value,
	 * if found the the system object, return the object value to row_store parameter specified
	 */
	int find_row(const object_type & ot, const string & col_name, const field_value fv,
			row_store & rs);
	int update_row(const object_type & ot,const string & col_name, const field_value & ffv,
			row_store & rs);
	int delete_row(const object_type & ot, const string & col_name, const field_value & fv);

	void get_table_desc(const string & name, table_desc &ptd);

	sys_seg();
	sys_seg(const std::string pathname);
	sys_seg(unsigned long _id) = delete;
	sys_seg(unsigned long _id, data_file *_f, unsigned int _off = 0) = delete;
	//sys_seg(const sys_seg & another) = delete;
	virtual ~sys_seg();

private:
	int version = 0x10;
	std::string path_name;
	map<string, int> page_map;
	map<string, table_desc> name_map;
	map<int, table_desc> type_map;
	data_file sys_df;

	int seek_last_blk(const object_type &ot, sys_seg& seg,
			mem_data_block & blk);
	int find_table_desc(const object_type &ot, table_desc & td);

	void append_schemas_desc(char_buffer & buff);
	void append_tables_desc(char_buffer & buff);
	void append_columns_desc(char_buffer & buff);
	void append_indexes_desc(char_buffer & buff);
	void append_index_attrs_desc(char_buffer & buff);
	void append_sequences_desc(char_buffer & buff);

	int find_row(mem_data_block &blk,
			const field_desc &fd, const field_value & tfv, row_store & rs);

	int find_row(mem_data_block &blk, const string & obj_name);

};

} /* namespace enginee */
} /* namespace sdb */

#endif /* SYS_SEG_H_ */
