#include "cute.h"
#include "ide_listener.h"
#include "xml_listener.h"
#include "cute_runner.h"
#include "common/char_buffer.h"
#include "common/encoding.h"
#include "enginee/sdb.h"
#include "enginee/table_description.h"
#include "enginee/field_description_store.h"
#include <list>

using namespace std;
using namespace enginee;

void char_pointer_test(){
	char *p = new char[10];

	char f[] = {0};

	char t[] = {1};

	int pos = 0;

	p[pos++] = f[0];
	p[pos++] = t[0];

	ASSERT(p[0]==0);
	ASSERT(p[1]==1);

	delete[] p;
}



void newTestFunction() {
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
	for (std::list<int>::iterator it = my_list.begin(); it != my_list.end();
			it++) {
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
	ASSERT(buffer1.size()==9);

	long l ;
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

	buffer << i << l << str;
	ASSERT(buffer.size() == 4 + 8 + 4 + 16);

}

void sdb_table_description_test() {
	string tab1 = "tab1";
	enginee::sdb db("/tmp", "db1");
	enginee::table_description tbl_desc(db, tab1);
	if (!db.exists()) {
		db.init();
	}
	ASSERT(db.exists() == true);

	ASSERTM(tbl_desc.get_file_name(),
			tbl_desc.get_file_name() == std::string("/tmp/db1/tab1.td"));

	tbl_desc.set_comment("this is the table comment");

	field_description field1, field2, field3;

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

	tbl_desc.write_to_file(true);

	ASSERT(db.exist_table(tab1) == true);

	table_description other(db, tab1);
	bool loaded = other.load_from_file();
	ASSERT(loaded == true);

	ASSERT(other.get_table_name() == tbl_desc.get_table_name());
	ASSERT(other.get_comment() == tbl_desc.get_comment());
	ASSERT(other.field_count() == tbl_desc.field_count());

	ASSERT(other.exists_field(fn1) == true);
	field_description ofd1(other.get_field_description(fn1));
	ASSERT(ofd1.get_field_name() == fn1);
	ASSERT(ofd1.get_comment() == comment1);
	ASSERT(ofd1.get_field_type() == field1.get_field_type());

	tbl_desc.delete_field(fn1);

	ASSERT(tbl_desc.field_count() == 2);

	ASSERT(tbl_desc.exists_field(fn1) == false);

	for (auto it = tbl_desc.get_field_desc_list().begin();
			it != tbl_desc.get_field_desc_list().end(); it++) {
		cout << " field name: " << it->get_field_name() << " is deleted: "
				<< it->is_deleted() << endl;
	}

	tbl_desc.write_to_file(true);

	//reload the table description
	loaded = other.load_from_file();

	ASSERT(loaded == true);

	for (auto it = other.get_field_description_map().begin();
			it != other.get_field_description_map().end(); it++) {
		cout << "map key:" << it->first << "map value:"
				<< it->second.get_inner_key() << " status: "
				<< it->second.is_deleted() << endl;
	}

	ASSERT(other.exists_field(fn1) == false);

	ASSERT(other.field_count() == 2);

	field_description ofd2(other.get_field_description(fn2));
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
	field_description_store fds(field1, bool_type, comment, true);

	fds.set_pre_store_block_pos(-1);
	fds.set_next_store_block_pos(128);
	fds.fill_store();

	ASSERT(fds.block_size() == fds.evaluate_block_size());

	field_description_store copy;

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
	s.push_back(CUTE(newTestFunction));
	s.push_back(CUTE(sdb_common_char_buff_operator_test));
	s.push_back(CUTE(sdb_common_encoding_test));
	s.push_back(CUTE(sdb_common_char_buff_test));
	s.push_back(CUTE(sdb_common_buffer_str_test));
	s.push_back(CUTE(sdb_test_sdb));
	s.push_back(CUTE(sdb_test_field_description_store));

	s.push_back(CUTE(sdb_table_description_test));


	// CUTE engineer start
	cute::xml_file_opener xmlfile(argc, argv);
	cute::xml_listener<cute::ide_listener<> > lis(xmlfile.out);
	cute::makeRunner(lis, argc, argv)(s, "AllTests");
}

int main(int argc, char const *argv[]) {
	runAllTests(argc, argv);
	return 0;
}
