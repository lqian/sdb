#include "cute.h"
#include "ide_listener.h"
#include "xml_listener.h"
#include "cute_runner.h"

#include "common/char_buffer.h"
#include "common/encoding.h"
#include "enginee/sdb.h"
#include "enginee/table_description.h"
#include "enginee/field_description_store.h"
#include "enginee/table_store.h"
#include "enginee/block_store.h"

#include <list>
#include <ctime>



using namespace std;
using namespace enginee;

void time_test() {
	time_t curr;
	long seconds = time(&curr);

	cout << "invoke once:" << seconds << endl;

	seconds = time(&curr);

	cout << "invoke twice: " << seconds << endl;

	time_t myt = 1420440500;
	cout << "another time:" << ctime(&myt) << endl;
}

void sdb_table_store_test() {
	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::TableDescription tbl_desc(db, tab1);

	if (!db.exists()) {
		db.init();
	}

	enginee::TableStore tab1_store(tbl_desc);

	if (!tab1_store.is_initialized()) {
		tab1_store.init_store(false);
	}

	ASSERT(tab1_store.is_initialized() == true);

	ASSERT(tab1_store.open() == true);

//	ASSERT(tab1_store.get_block_store_off() == enginee::table_head_length);
//
//	ASSERT(tab1_store.get_head_block_store_pos() ==  enginee::table_head_length);

	if (!tab1_store.has_block_store()) {
		BlockStore bs;
		ASSERT(tab1_store.assign_block_store(bs));
		ASSERT(bs.get_status() == enginee::DISK_SPACE_ASSIGNED);
		tab1_store.sync_head();
	}

	BlockStore r;
	r.set_start_pos(enginee::TABLE_HEAD_LENGTH); // set as the first block
	tab1_store.read_block_store(r, false);
	ASSERT(r.get_block_size() == enginee::BLOCK_SIZE_64M);
	ASSERT(r.get_usage_percent() == 0);
	ASSERT(r.get_status() == enginee::DISK_SPACE_ASSIGNED);

	ASSERT(tab1_store.close());
}

void test_block_store_append() {

	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::TableDescription tbl_desc(db, tab1);

	if (!db.exists()) {
		db.init();
	}

	tbl_desc.load_from_file();
	enginee::TableStore tab1_store(tbl_desc);

	if (!tab1_store.is_initialized()) {
		tab1_store.init_store(false);
	}

	BlockStore head_block;
	ASSERT(tab1_store.open());
	ASSERT(tab1_store.head_block(&head_block));

	ASSERT(head_block.get_start_pos() == (long )enginee::TABLE_HEAD_LENGTH);

	char_buffer block_buff(ROW_STORE_SIZE_128K);
	head_block.set_block_buff(&block_buff);

	RowStore r1;

	FieldDescription fd1(tbl_desc.get_field_description("field1"));
	FieldDescription fd2(tbl_desc.get_field_description("field2"));
	FieldDescription fd3(tbl_desc.get_field_description("field3"));

	FieldValue v1(fd1.get_field_type());
	FieldValue v2(fd2.get_field_type());
	FieldValue v3(fd3.get_field_type());

	v1.set_bool(true);
	v2.set_int(1024);
	v3.set_long(65536L);

	r1.add_field_value_store(fd1, v1);
	r1.add_field_value_store(fd2, v2);
	r1.add_field_value_store(fd3, v3);

	head_block.append_to_buff(&r1);
	tab1_store.sync_buffer(head_block,
			enginee::SYNC_BLOCK_HEAD | enginee::SYNC_BLOCK_DATA);

	ASSERT(tab1_store.close());

}

void test_delete_row_store() {
	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::TableDescription tbl_desc(db, tab1);

	if (!db.exists()) {
		db.init();
	}

	tbl_desc.load_from_file();
	enginee::TableStore tab1_store(tbl_desc);

	if (!tab1_store.is_initialized()) {
		tab1_store.init_store(false);
	}

	BlockStore head_block;
	ASSERT(tab1_store.open());
	ASSERT(tab1_store.head_block(&head_block));

	RowStore rs;
	rs.set_start_pos(enginee::BLOCK_STORE_HEAD_SIZE); // the first row store in the block
	tab1_store.mark_deleted_status(head_block, rs);

	tab1_store.close();
}

void test_read_row_store() {
	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::TableDescription tbl_desc(db, tab1);

	if (!db.exists()) {
		db.init();
	}

	tbl_desc.load_from_file();
	enginee::TableStore tab1_store(tbl_desc);

	if (!tab1_store.is_initialized()) {
		tab1_store.init_store(false);
	}

	BlockStore head_block;
	ASSERT(tab1_store.open());
	ASSERT(tab1_store.head_block(&head_block));

	RowStore first;
	RowStore sec;
	first.set_start_pos(enginee::BLOCK_STORE_HEAD_SIZE); // the first row store in the block
	tab1_store.read_row_store(head_block, &first);

	ASSERT(first.get_status() == enginee::ROW_STORE_DELETED);

	sec.set_start_pos(
			enginee::BLOCK_STORE_HEAD_SIZE + head_block.get_row_store_size());
	bool r = tab1_store.read_row_store(head_block, &sec);
	tab1_store.close();
	ASSERT(r);

	if (r) {
		short k1 = 1, k2 = 2, k3 = 3;
		ASSERT(sec.get_field_value_map().size() == 3);
		ASSERT(sec.get_status() == enginee::ROW_STORE_IN_USAGE);
		ASSERT(sec.get_field_value(k2).int_val() == 1024);
		cout << "find key 2" << endl;
		ASSERT(sec.get_field_value(k3).long_val() == 65536L);
		cout << "find key 3" << endl;

		FieldDescription fd;
		fd = tbl_desc.get_field_description("field1");
		ASSERT(sec.get_field_value(fd.get_inner_key()).bool_val());
	}
}

void test_row_store() {
	RowStore r1;

	FieldDescription fd1, fd2, fd3;
	string fn1 = "field1";
	string fn2 = "field2";
	string fn3 = "field3";

	fd1.set_field_name(fn1);
	fd2.set_field_name(fn2);
	fd3.set_field_name(fn3);

	fd1.set_inner_key(1);
	fd2.set_inner_key(2);
	fd3.set_inner_key(3);

	fd1.set_field_type(bool_type);
	fd2.set_field_type(int_type);
	fd3.set_field_type(long_type);

	string comment1 = "field1 comment";
	string comment2 = "field2 comment";
	string comment3 = "field3 comment";

	fd1.set_comment(comment1);
	fd2.set_comment(comment2);
	fd3.set_comment(comment3);

	FieldValue v1(fd1.get_field_type());
	FieldValue v2(fd2.get_field_type());
	FieldValue v3(fd3.get_field_type());

	v1.set_bool(true);
	v2.set_int(1024);
	v3.set_long(65536L);

	r1.add_field_value_store(fd1, v1);
	r1.add_field_value_store(fd2, v2);
	r1.add_field_value_store(fd3, v3);

	FieldValue gv(r1.get_field_value(1));
	ASSERT(gv.bool_val());

	char_buffer row_buff(ROW_STORE_SIZE_4K);
	r1.fill_char_buffer(&row_buff);
	ASSERT(
			r1.get_content_size()
					== (4 + (3 * 2 + INT_CHARS)
							+ (4 * 3 + 1 + 4 + 8 + INT_CHARS)));

	ASSERT(
			row_buff.size()
					== (12 + (4 + 3 * 2 + INT_CHARS)
							+ (4 + 1 + 4 + 4 + 4 + 8 + INT_CHARS)));

	RowStore r2;
	r2.read_char_buffer(&row_buff);
	ASSERT(
			r2.get_content_size()
					== (4 + (3 * 2 + INT_CHARS)
							+ (4 * 3 + 1 + 4 + 8 + INT_CHARS)));

	ASSERT(r2.get_field_value(fd1.get_inner_key()).bool_val());
	ASSERT(r2.get_field_value(fd2.get_inner_key()).int_val() == 1024);
	ASSERT(r2.get_field_value(fd3.get_inner_key()).long_val() == 65536L);
}

void char_pointer_test() {
	char *p = new char[10];

	char f[] = { 0 };

	char t[] = { 1 };

	int pos = 0;

	p[pos++] = f[0];
	p[pos++] = t[0];

	ASSERT(p[0] == 0);
	ASSERT(p[1] == 1);

	delete[] p;
}

void list_test() {
	ofstream out("test", ios_base::out | ofstream::binary);
	out << 1 << 2 << "----------";
	out.close();

	ifstream in("test", ios_base::in | ios_base::binary);

	int a, b;
	in >> a >> b;
	cout << a << endl << b << endl;

	std::list<int> my_list;
	my_list.push_back(20);
	my_list.push_back(30);

	for (std::list<int>::iterator it = my_list.begin(); it != my_list.end();
			it++) {
		cout << *it << endl;
	}

	ASSERT((*my_list.begin()) == 20);

	my_list.insert(my_list.begin(), 40);

	cout << "after my_list.insert()..." << endl;
	std::list<int>::iterator it = my_list.begin();
	for (; it != my_list.end(); it++) {
		cout << *it << endl;
	}
	ASSERT((*my_list.begin()) == 40);
}

void sdb_common_encoding_test() {
	char t[1] = { 1 };
	ASSERT(to_bool(t));

	char f[1] = { 0 };
	ASSERT(to_bool(f) == false);
}

void sdb_common_char_buff_test() {

	common::char_buffer buffer1(512);

	buffer1 << 128L << true;
	ASSERT(buffer1.size() == 9);

	long l;
	bool b;
	buffer1 >> l >> b;
	ASSERT(b);

	common::char_buffer buffer(512);
	buffer.push_back(128);

	buffer.push_back(2014L);

	ASSERT(buffer.size() == 12);

	ASSERT(buffer.pop_int() == 128);

	ASSERT(buffer.pop_long() == 2014);

	ASSERT(buffer.size() == 12);

	ASSERT(buffer.remain() == 0);

	ASSERT(buffer.capacity() == 512);

	buffer.shrink();

	ASSERT(buffer.capacity() == 12);

	char * s = "--make the buffer to expand with push a string";
	buffer.push_back(s, 46);
	char *ps = buffer.pop_cstr();

	ASSERT(strlen(ps) == 46);

	buffer.push_back(true);
	bool t = buffer.pop_bool();
	ASSERT(t);

}

void sdb_common_char_buff_operator_test() {
	common::char_buffer buffer(512);
	int i = 128;
	long l = 2014L;
	string str("this is a string");

	buffer << i;
	buffer << l;
	buffer << str;
//	buffer << i << l << str;
	ASSERT(buffer.size() == 4 + 8 + 4 + 16);

}

void sdb_table_description_test() {
	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::TableDescription tbl_desc(db, tab1);
	if (!db.exists()) {
		db.init();
	}
	ASSERT(db.exists() == true);

	ASSERTM(tbl_desc.get_file_name(),
			tbl_desc.get_file_name() == std::string("/tmp/db1/tab1.td"));

	tbl_desc.set_comment("this is the table comment");

	FieldDescription field1, field2, field3;

	string fn1 = "field1";
	string fn2 = "field2";
	string fn3 = "field3";

	field1.set_field_name(fn1);
	field2.set_field_name(fn2);
	field3.set_field_name(fn3);

	field1.set_field_type(bool_type);
	field2.set_field_type(int_type);
	field3.set_field_type(long_type);

	string comment1 = "field1 comment";
	string comment2 = "field2 comment";
	string comment3 = "field3 comment";

	field1.set_comment(comment1);
	field2.set_comment(comment2);
	field3.set_comment(comment3);

	tbl_desc.add_field_description(field1);
	tbl_desc.add_field_description(field2);
	tbl_desc.add_field_description(field3);

	ASSERT(field1.get_inner_key() == 1);

	tbl_desc.write_to_file(true);

	ASSERT(db.exist_table(tab1) == true);

	TableDescription other(db, tab1);
	bool loaded = other.load_from_file();
	ASSERT(loaded == true);

	ASSERT(other.get_table_name() == tbl_desc.get_table_name());
	ASSERT(other.get_comment() == tbl_desc.get_comment());
	ASSERT(other.field_count() == tbl_desc.field_count());

	ASSERT(other.exists_field(fn1) == true);
	FieldDescription ofd1(other.get_field_description(fn1));
	ASSERT(ofd1.get_field_name() == fn1);
	ASSERT(ofd1.get_comment() == comment1);
	ASSERT(ofd1.get_field_type() == field1.get_field_type());

	other.set_table_name("copy_tab1");
	other.delete_field(fn1);
	other.write_to_file(true);

	//reload the table description
	loaded = other.load_from_file();

	ASSERT(loaded == true);

//	for (auto it = other.get_field_description_map().begin();
//			it != other.get_field_description_map().end(); it++) {
//		cout << "map key:" << it->first << "map value:"
//				<< it->second.get_inner_key() << " status: "
//				<< it->second.is_deleted() << endl;
//	}

	for (std::map<std::string, FieldDescription>::const_iterator it =
			other.get_field_description_map().begin();
			it != other.get_field_description_map().end(); it++) {
		cout << "map key:" << it->first << "map value:"
				<< it->second.get_inner_key() << " status: "
				<< it->second.is_deleted() << endl;
	}

	ASSERT(other.exists_field(fn1) == false);

	ASSERT(other.field_count() == 2);

	FieldDescription ofd2(other.get_field_description(fn2));
	ASSERT(ofd2.get_field_name() == fn2);
	ASSERT(ofd2.get_comment() == comment2);
	ASSERT(ofd2.get_field_type() == field2.get_field_type());
}

void sdb_test_sdb() {
	enginee::sdb dba("/tmp", "db1");
	enginee::sdb dbb("/tmp", "db1");
	ASSERT(dba == dbb);
}

void sdb_test_field_description_store() {
	std::string field1 = "f";
	std::string comment = "c";
	FieldDescriptionStore fds(field1, bool_type, comment, true);

	fds.set_pre_store_block_pos(-1);
	fds.set_next_store_block_pos(128);
	fds.fill_store();

	ASSERT(fds.block_size() == fds.evaluate_block_size());

	FieldDescriptionStore copy;

	copy.set_store(fds.char_buffer());

	copy.read_store();

	ASSERT(copy.get_comment() == comment);
	ASSERT(copy.get_field_name() == field1);
	ASSERT(copy.is_deleted());
}

void sdb_common_buffer_str_test() {
	common::char_buffer buffer(512);

	// add string to
	char chars[] = "make the buffer to expand with push a string";
	buffer.push_back(std::string(chars));
	ASSERT(buffer.tail() == 48);
	ASSERT(buffer.pop_string() == std::string(chars));

	char * s = "--make the buffer to expand with push a string";
	buffer.push_back(s, 46);
	cout << "pop c_str:" << buffer.pop_string() << "\n";

	// is a bug?
	std::string str(s);
	buffer.push_back(str);
	cout << "size:" << str.size() << ":" << str.c_str() << endl;
	cout << "pop string:" << buffer.pop_cstr() << "\n";
}

void runAllTests(int argc, char const *argv[]) {
	cute::suite s;
	// add your test here
	s.push_back(CUTE(char_pointer_test));

	s.push_back(CUTE(sdb_common_char_buff_operator_test));
	s.push_back(CUTE(sdb_common_encoding_test));
	s.push_back(CUTE(sdb_common_char_buff_test));
	s.push_back(CUTE(sdb_common_buffer_str_test));
	s.push_back(CUTE(sdb_test_sdb));
	s.push_back(CUTE(sdb_test_field_description_store));

	s.push_back(CUTE(sdb_table_description_test));
	s.push_back(CUTE(time_test));
	s.push_back(CUTE(list_test));
	s.push_back(CUTE(test_row_store));
	s.push_back(CUTE(sdb_table_store_test));
	s.push_back(CUTE(test_block_store_append));
	s.push_back(CUTE(test_delete_row_store));
	s.push_back(CUTE(test_read_row_store));

	// CUTE engineer start
	cute::xml_file_opener xmlfile(argc, argv);
	cute::xml_listener<cute::ide_listener<> > lis(xmlfile.out);
	cute::makeRunner(lis, argc, argv)(s, "AllTests");
}

int main(int argc, char const *argv[]) {
	runAllTests(argc, argv);
	return 0;
}
