#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

#include "enginee/log_mgr.h"

using namespace sdb::enginee;
using namespace sdb::common;

void log_block_test() {
	log_block lb;
	timestamp ts = 100L;
	action a;
	a.wl = 35;
	a.wd = new char[35];
	a.seq = 1;

	action b = a;
	b.seq = 2;
	ASSERT(lb.add_action(ts, a) == DIRCTORY_ENTRY_LENGTH * 0);
	ASSERT(lb.add_action(ts, b) == DIRCTORY_ENTRY_LENGTH * 1);

	ASSERT(lb.count_entry() == 2);

	char_buffer buff(1024);
	while (lb.has_next()) {
		lb.next_entry(buff);
	}
	ASSERT(buff.size() == 70);

	lb.tail();
	while (lb.has_pre()) {
		lb.pre_entry(buff);
	}
	ASSERT(buff.size() == 140);

}

void runSuite() {
	cute::suite s;
	s.push_back(CUTE(log_block_test));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The Log Mgr Test Suite");
}

int main() {
	runSuite();
	return 0;
}

