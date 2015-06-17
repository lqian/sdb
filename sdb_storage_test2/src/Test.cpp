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

using namespace std;
using namespace common;
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
	ASSERT(df1.assgin_segment(seg1) == SUCCESS);
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
	ASSERT(df.assgin_segment(seg) == SUCCESS);

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
	ASSERT(df.assgin_segment(seg_w) == SUCCESS);
	ASSERT(seg_w.get_offset() == 0);
	ASSERT((*seg_w.get_file()) == df);

	segment seg_r(seg_w);
	seg_w.assign_content_buffer();

	const char * pc = path.c_str();
	seg_w.update(0, pc, 0, path.length());
	df.update_segment(seg_w);
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
	int ral = 57;
	char * ra = new char[ral];
	for (int i = 0; i < ral; i++) {
		ra[i] = (i + 1);
	}

	unsigned short t_off = mdb.length - ral;  // target offset of ra
	unsigned short ra_off = mdb.add_row_data(ra, ral);
	ASSERT(ra_off == t_off);
	ASSERT(mdb.off_tbl.size() == 1 && mdb.off_tbl[0] == t_off);
	ASSERT(memcmp(mdb.buffer + t_off, ra, ral) == 0);

	int rbl = 102;
	char * rb = new char[rbl + 10];
	for (int i = 0; i < rbl + 10; i++) {
		rb[i] = (i + 1);
	}

	t_off -= rbl; 			// target offset of rb
	unsigned short rb_off = mdb.add_row_data(rb, rbl);
	ASSERT(rb_off == t_off);
	ASSERT(mdb.off_tbl.size() == 2 && mdb.off_tbl[1] == t_off);
	// update the rb as shrink manner in the end
	unsigned short sb_off = mdb.update_row_data(rb_off, rb, rbl - 20);
	ASSERT(sb_off - 20 == rb_off);

	// update the rb as extend manner in the end
	unsigned short eb_off = mdb.update_row_data(sb_off, rb, rbl + 10);
	ASSERT(eb_off + 10 == rb_off);
	delete[] rb;

	// update the ral as shrink manner
	ral = 54;
	t_off = mdb.off_tbl[0];

	ASSERT(mdb.index(t_off) == 0);

	int u_off = mdb.update_row_data(mdb.off_tbl[0], ra, ral);
	ASSERT(u_off == t_off);
	delete[] ra;

	//update the ral as re-create manner
	ral += 20;
	ra = new char[ral];
	t_off = mdb.off_tbl[1] - ral;
	int n_off = mdb.update_row_data(mdb.off_tbl[0], ra, ral);
	ASSERT(mdb.off_tbl.size() == 3);
	int flag = mdb.off_tbl[0] >> 15;
	ASSERT(flag == 1); // check the delete flag
	ASSERT(t_off = n_off);

	// test write offset tbl
	mdb.write_off_tbl();
	common::char_buffer head_buff(mdb.buffer, 3 * sdb::storage::offset_bytes);

	unsigned short off0, off1, off2;
	head_buff >> off0 >> off1 >> off2;
	ASSERT(off0 == mdb.off_tbl[0]);
	ASSERT(off1 == mdb.off_tbl[1]);
	ASSERT(off2 == mdb.off_tbl[2]);
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

	df.assgin_segment(seg);

	seg.assign_content_buffer();
	seg.assign_block(b1);

	ASSERT(b1.ref_flag);
	ASSERT(b1.length == K_4 * kilo_byte - sdb::storage::block_header_size);

	int row_len = path.length();
	int w_off = b1.add_row_data(path.c_str(), row_len);
	ASSERT(w_off == b1.length - row_len);

	ASSERT(b1.off_tbl.size() == 1);
	ASSERT(b1.off_tbl[0] == b1.length - row_len);
	char * row_data = b1.buffer + b1.off_tbl[0];
	ASSERT(memcmp(row_data, path.c_str(), row_len) == 0);
	b1.write_off_tbl();
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

	ASSERT(b2.length == K_4 * kilo_byte - sdb::storage::block_header_size);
	b2.parse_off_tbl();
	ASSERT(
			b2.off_tbl.size() == 1
					&& b2.off_tbl[0] == b2.length - path.length());
	ASSERT(memcmp(b2.buffer + b2.off_tbl[0], path.c_str(), path.length()) == 0);

}

void table_desc_test() {
	table_desc td1("tab1", "db1", "comment for tab1");
	field_desc fd1("fd1", int_type, 4, "comment for field desc 1", false), fd2(
			"fd2", varchar_type, 40, "comment for field desc 2", false);

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

	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "storage test suite");
}

int main() {
	runSuite();
	return 0;
}

