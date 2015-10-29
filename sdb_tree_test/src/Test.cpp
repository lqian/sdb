#include <index/bpt2.h>
#include <index/field.h>

#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

using namespace sdb::index;
using namespace sdb::common;

void key_field_test() {
	int_key_field k1, k2;
	char_buffer buff(8);
	buff << 20 << 30;
	k1.ref(buff.data(), 4);
	k2.ref(buff.data() + 4, 4);

	ASSERT(k1.compare(k2) == -1);
}

void key_test() {

	_key_field kf1;
	kf1.type = int_type;
	kf1.field_len = 4;
	_key_field kf2;
	kf2.type = varchar_type;
	_key k1, k2;
	k1.add_key_field(kf1);
	k1.add_key_field(kf2);

	k2.add_key_field(kf1);
	k2.add_key_field(kf2);

	char_buffer buff1(20), buff2(20);
	string str1 = "abc", str2 = "abcd";
	buff1 << 1 << str1;
	buff2 << 1 << str2;
	k1.ref(buff1.data(), buff1.size());
	k2.ref(buff2.data(), buff2.size());

	ASSERT(k1.compare(k2) == -1);

}

void index_component_test() {
	segment seg(1L);
	seg.set_length(M_64);
	seg.assign_content_buffer();
	fs_ipage fip;
	fs_lpage flp;
	seg.assign_block(&fip);
	seg.assign_block(&flp);
	int key_len = 4;
	int val_len = 14;
	fip.header->node_size = key_len + sizeof(fs_inode::head);
	flp.header->node_size = key_len + val_len + sizeof(fs_lnode::head);

	fs_lnode ln;
	fs_inode in;
	fip.assign_node(&in);
	flp.assign_node(&ln);

	ASSERT(in.len == key_len && ln.len == key_len + val_len);
}

void runSuite() {
	cute::suite s;
	//TODO add your test here
	s.push_back(CUTE(key_field_test));
	s.push_back(CUTE(key_test));
	s.push_back(CUTE(index_component_test));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The bpt2 index test Suite");
}

int main() {
	runSuite();
	return 0;
}

