#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"
#include <cstring>
#include <vector>
#include <iostream>
#include "storage/datafile.h"
#include <algorithm>
#include "tree/avl.h"
#include "enginee/table_desc.h"
#include "enginee/field_value.h"
#include "enginee/sys_seg.h"
#include "enginee/seg_mgr.h"
#include "enginee/table.h"
#include "common/char_buffer.h"

using namespace std;
using namespace sdb::common;
using namespace sdb::tree;
using namespace sdb::storage;
using namespace sdb::enginee;

void data_file_basic_test() {

	sdb::storage::data_file d_file(1L, "/tmp/sdb_data_file_1.db");
	ASSERT(d_file.create() == sdb::SUCCESS);
	ASSERT(d_file.open() == sdb::SUCCESS);
	ASSERT(d_file.close() == sdb::SUCCESS);
	d_file.remove();

	ASSERT(d_file.exist() == false);
}

void segment_basic_test() {
	using namespace sdb;
	using namespace sdb::storage;

	sdb::storage::segment seg1(1L), seg2(2L), seg(seg1);

	ASSERT(seg1 == seg);
	std::string path = "/tmp/sdb_data_file_tmp_2.db";
	sdb::storage::data_file df1(2L, path);
	ASSERT(df1.open() == SUCCESS);
	seg1.set_length(sdb::storage::M_64);
	ASSERT(df1.assign_segment(seg1) == SUCCESS);
	df1.close();

	data_file df2(2L, path);
	ASSERT(df2.open() == SUCCESS);

	ASSERT(df2.has_next_segment(seg));
	ASSERT(seg.get_create_time() > 0 && seg.get_assign_time() > 0);
	ASSERT(df2.close() == SUCCESS);

	df2.remove();
}

void test_delete_segment() {
	using namespace sdb;
	using namespace sdb::storage;
	sdb::storage::data_file df(3L, "/tmp/sdb_data_file_tmp_3.db");
	if (df.exist())
		df.remove();
	df.open();
	segment seg(4L);
	seg.set_offset(1024);
	seg.set_length(M_64);
	ASSERT(df.assign_segment(seg) == SUCCESS);

	ASSERT(seg.get_offset() == 0);
	ASSERT((*seg.get_file()) == df);

	df.remove();
}

void test_update_segment() {
	using namespace sdb;
	using namespace sdb::storage;
	std::string path("/tmp/sdb_data_file_tmp_update.db");
	sdb::storage::data_file df(3L, path);
	if (df.exist())
		df.remove();
	df.open();

	segment seg_w(4L);
	seg_w.set_offset(1024);
	seg_w.set_length(M_64);
	ASSERT(df.assign_segment(seg_w) == SUCCESS);
	ASSERT(seg_w.get_offset() == 0);
	ASSERT((*seg_w.get_file()) == df);

	segment seg_r(seg_w);
	seg_w.assign_content_buffer();

	const char * pc = path.c_str();
	seg_w.update(0, pc, 0, path.length());
	df.flush_segment(seg_w);
	df.close();

	//re-open the data file and read  the segment content
	sdb::storage::data_file df1(3L, path);
	ASSERT(df1.open() == SUCCESS);
	ASSERT(seg_r.get_offset() == 0);
	ASSERT(df1.fetch_segment(seg_r) == SUCCESS);
	char * buff = new char[path.length()];
	seg_r.read(buff, 0, path.length());
	df1.remove();

	std::cout << buff << std::endl;
	ASSERT(memcmp(path.c_str(), buff, path.length()) == 0);
}

void mem_block_test() {

	using namespace sdb::storage;

	sdb::storage::mem_data_block mdb;
	mdb.length = 4096;
	mdb.free_perct = 100;
	mdb.buffer = new char[mdb.length];
	mdb.ref_flag = true;

	// a row to be add
	int ral = 26;
	char * ra = "abcdefghijklmnopqrstuvwxyz";

	unsigned short t_off = mdb.length - ral;  // target offset of ra
	int idx = mdb.add_row_data(ra, ral);
	ASSERT(mdb.entry_count == 1 && mdb.p_dir_off[idx] == t_off);
	ASSERT(memcmp(mdb.buffer + t_off, ra, ral) == 0);

	int rbl = 102;
	char * rb = new char[rbl + 10];
	for (int i = 0; i < rbl + 10; i++) {
		rb[i] = (i + 1);
	}

	t_off -= rbl; 			// target offset of rb
	idx = mdb.add_row_data(rb, rbl);
	ASSERT(mdb.entry_count == 2 && mdb.p_dir_off[idx] == t_off);
	// update the rb as shrink manner in the end
	idx = mdb.update_row_by_index(idx, rb, rbl - 20);
	ASSERT(idx >= 0 && t_off - 20 == mdb.p_dir_off[idx]);

	// update the rb as extend manner in the end
	idx = mdb.update_row_data(idx, rb, rbl + 10);
	ASSERT(idx >= 0 && t_off + 10 == mdb.p_dir_off[idx]);
	delete[] rb;

	// update the ral as shrink manner
	ral = 13;
	idx = 0;
	t_off = mdb.p_dir_off[idx];
	idx = mdb.update_row_by_index(idx, ra, ral);
	ASSERT(mdb.p_dir_off[idx] == t_off);
	delete[] ra;

	//update the ral as re-create manner
	ral += 20;
	idx = 0;
	ra = new char[ral];
	t_off = mdb.p_dir_off[idx] - ral;
	int n_off = mdb.update_row_by_index(idx, ra, ral);
	ASSERT(mdb.entry_count == 3);
	int flag = mdb.p_dir_off[0] >> 15;
	ASSERT(flag == 1); // check the delete flag
	ASSERT(t_off = n_off);

	// test write offset tbl
	char_buffer head_buff(mdb.buffer, 3 * sdb::storage::offset_bytes, true);

	unsigned short off0, off1, off2;
	head_buff >> off0 >> off1 >> off2;
	ASSERT(off0 == mdb.p_dir_off[0]);
	ASSERT(off1 == mdb.p_dir_off[1]);
	ASSERT(off2 == mdb.p_dir_off[2]);
}

void test_block_write() {
	mem_data_block b1, b2;

	std::string path("/tmp/test_block_write.db");
	sdb::storage::data_file df(3L, path);
	if (df.exist())
		df.remove();
	df.open();

	segment seg(1L);
	seg.set_length(M_64);
	// assign a new data block
	seg.set_block_size(sdb::storage::K_4);
	segment seg_r(seg);

	df.assign_segment(seg);

	seg.assign_content_buffer();
	seg.assign_block(b1);

	ASSERT(b1.ref_flag);
	ASSERT(b1.length == K_4 * kilo_byte - sdb::storage::block_head_size);

	int row_len = path.length();
	int w_off = b1.add_row_data(path.c_str(), row_len);
	ASSERT(w_off == b1.length - row_len);

	ASSERT(b1.entry_count == 1);
	ASSERT(b1.p_dir_off[0] == b1.length - row_len);
	char * row_data = b1.buffer + b1.p_dir_off[0];
	ASSERT(memcmp(row_data, path.c_str(), row_len) == 0);
	ASSERT(df.write_block(seg, b1, true) == sdb::SUCCESS);

	//re-open the data file for read block
	df.close();
	df.open();

	b2.offset = 0;
	seg_r.set_offset(0);
	seg_r.set_block_size(sdb::storage::K_4);
	ASSERT(df.fetch_segment(seg_r) == sdb::SUCCESS);
	seg_r.read_block(b2);

	ASSERT(b1.header->blk_magic == b2.header->blk_magic);
	ASSERT(b1.header->create_time == b2.header->create_time);

	ASSERT(b2.length == K_4 * kilo_byte - sdb::storage::block_head_size);
	ASSERT(b2.entry_count == 1 && b2.p_dir_off[0] == b2.length - path.length());
	ASSERT(
			memcmp(b2.buffer + b2.p_dir_off[0], path.c_str(), path.length())
					== 0);

}

void table_desc_test() {
	table_desc td1("tab1", "db1", "comment for tab1");
	field_desc fd1("fd1", int_type, 4, "comment for field desc 1", false), fd2(
			"fd2", varchar_type, 64, "comment for field desc 2", false);

	//TODO
	td1.add_field_desc(fd1);
	td1.add_field_desc(fd2);
	ASSERT(fd1.is_assigned() && fd2.is_assigned());

	char_buffer fd_buff(100);
	fd1.write_to(fd_buff);
	fd_buff.rewind();
	field_desc fd1_clone;
	fd1_clone.load_from(fd_buff);
	ASSERT(fd1 == fd1_clone);

	ASSERT(td1.delete_field("fd2"));
	ASSERT(td1.store_field_count() == 2);

	// test for table description load and write to buffer
	char_buffer td_buff(512);
	td1.write_to(td_buff);
	td_buff.rewind();
	table_desc td1_clone;
	td1_clone.load_from(td_buff);

	ASSERT(td1_clone == td1);

}

void row_store_test() {
	table_desc td1("tab1", "db1", "comment for tab1");
	field_desc fd1("col1", int_type, "comment for field desc 1", false), fd2(
			"col2", varchar_type, 64, "comment for field desc 2", false), fd3(
			"col3", long_type, "comment for field desc 3", false);

	td1.add_field_desc(fd1);
	td1.add_field_desc(fd2);
	td1.add_field_desc(fd3);

	field_value fv1, fv2, fv3;
	fv1.set_int(20150618);
	fv2.set_string(fd2.get_comment());
	fv3.set_long(1651L);

	row_store rs(&td1);
	rs.set_field_value(fd1, fv1);
	rs.set_field_value(fd2, fv2);
	rs.set_field_value(fd3, fv3);

	char_buffer row_buff(128);
	rs.write_to(row_buff);

	row_store rs_clone;
	rs_clone.set_table_desc(&td1);
	row_buff.rewind();
	rs_clone.load_from(row_buff);

	ASSERT(rs_clone.value_equals(rs));
}

void test_sys_seg() {
	sys_seg seg("/tmp");

	if (seg.is_initialized()) {
		seg.clean();
	}
	seg.initialize();

	ASSERT(seg.is_initialized());

	table_desc tdsm;
	seg.get_table_desc("schemas", tdsm);
	unsigned char ik = 1;
	ASSERT(tdsm.get_field_desc("schema_id").get_field_name() == "schema_id");
	ASSERT(tdsm.get_field_desc(1).get_field_name() == "schema_id");

	// add a row for create a new schema in database
	ik = 1;
	row_store rs(&tdsm);
//	rs.set_table_desc(&tdsm);
	field_value fv0, fv1, fv2, fv3, fv4, fv5;
	fv0.set_uint(123L);
	fv1.set_string("db1");
	fv2.set_uint(0x10);
	fv3.set_string("a test schema");
	fv4.set_time(123991281829L);
	fv5.set_time(123991281829L);

	rs.set_field_value(ik++, fv0);
	rs.set_field_value(ik++, fv1);
	rs.set_field_value(ik++, fv2);
	rs.set_field_value(ik++, fv3);
	rs.set_field_value(ik++, fv4);
	rs.set_field_value(ik++, fv5);

	seg.add_row(SCHEMA_OBJ, rs);

	// find that row store
	row_store trs;
	field_value tfv;
	tfv.set_string("db1");
	ASSERT(seg.find_row(SCHEMA_OBJ, "schema_name", tfv, trs) == sdb::SUCCESS);
	// the trs row store object's tdesc doesn't point a invalid table_desc object because that it was free

	ASSERT(trs.value_equals(rs));
	fv1.set_string("db1_1");
	ik = 2;
	trs.set_field_value(ik, fv1);
	ASSERT(seg.update_row(SCHEMA_OBJ, "schema_name", fv1, trs) >= 0);

	ASSERT(seg.delete_row(SCHEMA_OBJ, "schema_name", fv1) == 0);
	seg.flush();
}

void test_seg_mgr() {
	using namespace sdb::enginee;
	using namespace std;

	LOCAL_SEG_MGR.add_datafile_paths("/tmp/dir1,/tmp/dir2");
	LOCAL_SEG_MGR.add_datafile_paths("/tmp/dir3,/tmp/dir4");
	LOCAL_SEG_MGR.remove_files();
	LOCAL_SEG_MGR.set_seg_default_length(M_1);
	LOCAL_SEG_MGR.load();

	data_file *pdf = nullptr;
//	ASSERT(LOCAL_SEG_MGR.assign_data_file(pdf) == sdb::SUCCESS);

//	data_file *df1 = new data_file;
//	data_file *df2 = new data_file;
//	data_file *df3 = new data_file;
//	ASSERT(LOCAL_SEG_MGR.assign_data_file(df1) == sdb::SUCCESS);
//	ASSERT(LOCAL_SEG_MGR.assign_data_file(df2) == sdb::SUCCESS);
//	ASSERT(LOCAL_SEG_MGR.assign_data_file(df3) == sdb::SUCCESS);

	segment * seg1 = new segment;
	segment * seg2 = new segment;
	segment * seg3 = new segment;
	segment * seg4 = new segment;
	ASSERT(LOCAL_SEG_MGR.assign_segment(seg1) == sdb::SUCCESS);
	ASSERT(LOCAL_SEG_MGR.assign_segment(seg2) == sdb::SUCCESS);
	ASSERT(LOCAL_SEG_MGR.assign_segment(seg3) == sdb::SUCCESS);
	ASSERT(LOCAL_SEG_MGR.assign_segment(seg4) == sdb::SUCCESS);

	data_file *f = LOCAL_SEG_MGR.find_data_file(1L);
	ASSERT(f != nullptr && f->get_id() == 1L);

	LOCAL_SEG_MGR.close();
	LOCAL_SEG_MGR.clean();
}

void test_table() {
	using namespace sdb::enginee;
	using namespace std;

	LOCAL_SEG_MGR.add_datafile_paths("/tmp/dir1,/tmp/dir2");
	LOCAL_SEG_MGR.add_datafile_paths("/tmp/dir3,/tmp/dir4");
	LOCAL_SEG_MGR.set_seg_default_length(M_1);
	LOCAL_SEG_MGR.load();

	table_desc td("demo_table", "db1", "comment for tab1");
	field_desc fd1("col1", int_type, "comment for field desc 1", false), fd2(
			"col2", varchar_type, 64, "comment for field desc 2", false), fd3(
			"col3", long_type, "comment for field desc 3", false);

	td.add_field_desc(fd1);
	td.add_field_desc(fd2);
	td.add_field_desc(fd3);

	field_value fv1, fv2, fv3;
	fv1.set_int(20150618);
	fv2.set_string(fd2.get_comment());
	fv3.set_long(1651L);

	row_store rs(&td);
	rs.set_field_value(fd1, fv1);
	rs.set_field_value(fd2, fv2);
	rs.set_field_value(fd3, fv3);

	char_buffer row_buff(128);
	rs.write_to(row_buff);
	segment *seg = LOCAL_SEG_MGR.find_segment(1L);
	table demo_table(seg, seg, &td);
	demo_table.set_seg_mgr(&LOCAL_SEG_MGR);
	demo_table.open();

	int total_rows = 100000;
	for (int i = 0; i < total_rows; i++) {
		demo_table.add_row(rs);
	}
	demo_table.flush();

	demo_table.head();
	int j = 0;
	while (demo_table.has_next_row()) {
		row_buff.reset();
		demo_table.next_row(row_buff);
		j++;
	}
	ASSERT(j == total_rows);

	demo_table.tail();
	while (demo_table.has_pre_row()) {
		row_buff.reset();
		demo_table.pre_row(row_buff);
		j--;
	}
	ASSERT(j == 0);

	LOCAL_SEG_MGR.close();

}

void runSuite() {
	cute::suite s;

	//memory block test
	s.push_back(CUTE(mem_block_test));
	s.push_back(CUTE(test_block_write));

	// data file and segment test
	s.push_back(CUTE(data_file_basic_test));
	s.push_back(CUTE(segment_basic_test));
	s.push_back(CUTE(test_delete_segment));
	s.push_back(CUTE(test_update_segment));
	s.push_back(CUTE(table_desc_test));
	s.push_back(CUTE(row_store_test));
	s.push_back(CUTE(test_sys_seg));
	s.push_back(CUTE(test_seg_mgr));
	s.push_back(CUTE(test_table));

	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "storage test suite");
}

int main() {
	runSuite();
	return 0;
}

