#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

#include "tree/bpt2.h"

using namespace sdb::tree;
using namespace sdb::common;

void key_field_test() {
	int_key_field k1, k2;
	char_buffer buff(8);
	buff << 20 << 30;
	k1.ref(buff.data(), 4);
	k2.ref(buff.data() + 4, 4);

	ASSERT(k1.compare(k2) == -1);
}

void runSuite() {
	cute::suite s;
	//TODO add your test here
	s.push_back(CUTE(key_field_test));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The Suite");
}

int main() {
	runSuite();
	return 0;
}

