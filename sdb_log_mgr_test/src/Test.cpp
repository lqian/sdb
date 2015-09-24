#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

#include "enginee/log_mgr.h"

using namespace sdb::enginee;
using namespace sdb::common;

void log_block_test() {
	log_block lb;
	lb.assign_block_buffer();
	timestamp ts = 100L;
	action a;
	a.wl = 35;
	a.wd = new char[35];
	a.seq = 1;

	action b = a;
	b.seq = 2;
	ASSERT(lb.add_action(ts, a) == DIRCTORY_ENTRY_LENGTH * 0);
	ASSERT(lb.add_action(ts, b) == DIRCTORY_ENTRY_LENGTH * 1);
	ASSERT(lb.add_commit(ts) == DIRCTORY_ENTRY_LENGTH * 2);

	timestamp rts=1002L;
	ASSERT(lb.add_rollback(rts) == DIRCTORY_ENTRY_LENGTH * 3);

	ASSERT(lb.count_entry() == 4);

	log_block::dir_entry e;
	int size;
	while (lb.has_next()) {
		lb.next_entry(e);
		if (e.get_type() == dir_entry_type::data_item) {
			size += e.length;
		}
	}
	ASSERT(size == 70);

	lb.tail();
	while (lb.has_pre()) {
		lb.pre_entry(e);
		if (e.get_type() == dir_entry_type::data_item) {
			size += e.length;
		}
	}
	ASSERT(size == 140);

	log_block::dir_entry e2 = lb.get_entry(2);
	ASSERT(e2.get_type() == dir_entry_type::commit_item);
	log_block::dir_entry e3 = lb.get_entry(3);
	ASSERT(e3.get_type()== dir_entry_type::rollback_item);
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

