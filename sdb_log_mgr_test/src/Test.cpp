#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

#include <forward_list>
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
	int idx = 0;
	ASSERT(lb.add_start(ts) == DIRCTORY_ENTRY_LENGTH * idx++);
	ASSERT(lb.add_action(ts, a) == DIRCTORY_ENTRY_LENGTH * idx++);
	ASSERT(lb.add_action(ts, b) == DIRCTORY_ENTRY_LENGTH * idx++);
	ASSERT(lb.add_commit(ts) == DIRCTORY_ENTRY_LENGTH * idx++);

	timestamp rts = 1002L;
	ASSERT(lb.add_start(rts) == DIRCTORY_ENTRY_LENGTH * idx++);
	ASSERT(lb.add_abort(rts) == DIRCTORY_ENTRY_LENGTH * idx++);

	ASSERT(lb.count_entry() == idx);

	log_block::dir_entry e;
	int size = 0;
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

	log_block::dir_entry e2 = lb.get_entry(3);
	ASSERT(e2.get_type() == dir_entry_type::commit_item);
	log_block::dir_entry e3 = lb.get_entry(5);
	ASSERT(e3.get_type() == dir_entry_type::abort_item);
}

void log_file_test() {

	string fn = "/tmp/000001.log";
	sio::remove_file(fn);
	log_mgr lmgr;
	log_file lf(fn);
	lf.set_log_mgr(&lmgr);
	ASSERT(lf.open() == sdb::SUCCESS);

	timestamp ts = 100L;
	action a;
	a.wl = 100;
	a.wd = new char[100];
	a.wd[0] = 0xFF;
	a.wd[99] = 0xEE;
	a.seq = 1;
	lf.append(ts, a);

	for (int i = 0; i < 60; i++) {
		a.seq = 2 + i;
		lf.append(ts, a);
	}

	log_block lb;
	// scan forward
	ASSERT(lf.head());
	int bs = lf.get_block_size();
	char *buff = new char[bs];
	while (lf.next_block(buff) == sdb::SUCCESS) {
		lb.ref_buffer(buff, bs);
		ASSERT(lb.count_entry() >= 0);
		cout << "scan forward log entry count:" << lb.count_entry() << endl;
	}

	// scan backward
	ASSERT(lf.tail() == sdb::SUCCESS);
	while (lf.pre_block(buff) == sdb::SUCCESS) {
		lb.ref_buffer(buff, bs);
		ASSERT(lb.count_entry() >= 0);
		cout << "scan backward log entry count:" << lb.count_entry() << endl;
	}


	lf.inactive();
	lf.close();

	log_file other;
	other.open(fn);
	ASSERT(other.is_active() == false);
}

void log_mgr_test() {
	log_mgr lmg;
	ASSERT(lmg.load("/tmp/logs") == sdb::SUCCESS);

	timestamp ts = 100L;
	action a;
	a.wl = 35;
	a.wd = new char[35];
	a.seq = 1;

	action b = a;
	b.seq = 2;
	timestamp rts = 1002L;
	lmg.log_action(ts, a);
	lmg.log_commit(ts);

	lmg.log_action(rts, b);
	lmg.log_rollback(rts);
}

void forward_list_test() {
	std::forward_list<int> mylist = { 10, 20, 30, 40, 30, 20, 10 };
	mylist.remove(20);
	std::cout << "mylist contains:";
	for (int& x : mylist)
		std::cout << ' ' << x;
	std::cout << '\n';

}

void runSuite() {
	cute::suite s;

	s.push_back(CUTE(log_block_test));
	s.push_back(CUTE(log_file_test));
	s.push_back(CUTE(log_mgr_test));
	s.push_back(CUTE(forward_list_test));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "The Log Mgr Test Suite");
}

int main() {
	runSuite();
	return 0;
}

