#include "cute.h"
#include "ide_listener.h"
#include "xml_listener.h"
#include "cute_runner.h"
#include "common/store_vector.h"
#include "enginee/sdb.h"
#include "enginee/table_description.h"

using namespace std;
using namespace enginee;

void sdb_common_char_buff_test() {
	common::char_buffer buffer(512);
	buffer.push_back(1224);

	buffer.push_back(2014L);
	ASSERT(buffer.size() == 12);

	ASSERT(buffer.pop_int() == 1224);

	ASSERT(buffer.pop_long() == 2014);

	ASSERT(buffer.size() == 12);

	ASSERT(buffer.remain() == 0);

	ASSERT(buffer.capacity() == 512);

	buffer.shrink();

	ASSERT(buffer.capacity() == 12);

	char * s = "--make the buffer to expand with push a string";
	buffer.push_back(s, 46);
	ASSERT(buffer.pop_string() == std::string(s));
}

void sdb_table_description_test() {
	enginee::sdb db("/tmp", "db1");
	enginee::table_description desc(db, "tab1");
	if (!db.exists()) {
		db.init();
	}
	ASSERT(db.exists() == true);
	ASSERT(db.exist_table("tab1") == false);
}
void sdb_test_sdb(){
	enginee::sdb dba("/tmp", "db1");
	enginee::sdb dbb("/tmp", "db1");
	ASSERT(dba == dbb);
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
	s.push_back(CUTE(sdb_common_char_buff_test));
	s.push_back(CUTE(sdb_common_buffer_str_test));
	s.push_back(CUTE(sdb_table_description_test));
	s.push_back(CUTE(sdb_test_sdb));

	// CUTE engineer start
	cute::xml_file_opener xmlfile(argc, argv);
	cute::xml_listener<cute::ide_listener<> > lis(xmlfile.out);
	cute::makeRunner(lis, argc, argv)(s, "AllTests");
}

int main(int argc, char const *argv[]) {
	runAllTests(argc, argv);
	return 0;
}

