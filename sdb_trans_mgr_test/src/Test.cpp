#include "cute.h"
#include "ide_listener.h"
#include "cute_runner.h"

#include "enginee/trans_mgr.h"
#include <thread>
#include <chrono>
#include <iostream>

using namespace std;
using namespace sdb::enginee;
using namespace sdb::common;

int rl = 10;

void before() {
	log_mgr * lm = &sdb::enginee::LOCAL_LOG_MGR;
	string path = "/tmp/logs";
	lm->set_path(path);
	lm->clean();
	lm->load();

	seg_mgr * sm = &sdb::enginee::LOCAL_SEG_MGR;
	sm->add_datafile_paths("/tmp/dir1,/tmp/dir2");
	sm->remove_files();
	sm->load();

	trans_mgr *tm = &sdb::enginee::LOCAL_TRANS_MGR;
	tm->open();
}

void after() {
	log_mgr * lm = &sdb::enginee::LOCAL_LOG_MGR;
	lm->close();
	seg_mgr * sm = &sdb::enginee::LOCAL_SEG_MGR;
	sm->flush_all_segment();

	trans_mgr *tm = &sdb::enginee::LOCAL_TRANS_MGR;
	tm->close();
}
void trans_basic_test() {
	trans_mgr *tm = &sdb::enginee::LOCAL_TRANS_MGR;
	ASSERT(tm->is_open());

	transaction t1;
	timestamp ts = tm->get_curr_ts();
	tm->submit(&t1);
	ASSERT(tm->get_curr_ts() - ts == 1);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	ASSERT(t1.status() == trans_status::COMMITTED);
}

/*
 * test transaction management and transaction without multi-thread
 */
void trans_basic_single() {
	segment *seg = new segment;
	seg_mgr * sm = &sdb::enginee::LOCAL_SEG_MGR;
	sm->assign_segment(seg);
	data_item_ref dif;
	mem_data_block blk;
	int rl = 10;
	char *n_buff = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff[i] = 'A' + i;
	}
	dif.seg_id = seg->get_id();
	seg->assign_block(blk);
	dif.row_idx = blk.assign_row(rl);
	dif.blk_off = blk.offset;
	blk.write_off_tbl();

	transaction t1;
	action a;
	a.dif = &dif;
	a.seq = 0;
	a.op = action_op::WRITE;
	a.create(n_buff, rl);
	t1.add_action(a);

	trans_mgr *tm = &sdb::enginee::LOCAL_TRANS_MGR;
	tm->assign_trans(&t1);
	t1.execute();
	ASSERT(t1.status() == trans_status::COMMITTED);

	transaction t2;
	action b;
	b.dif = &dif;
	b.seq = 0;
	b.op = action_op::READ;
	t2.add_action(b);
	tm->assign_trans(&t2);
	t2.execute();
	ASSERT(t2.status() == trans_status::COMMITTED);
	auto it = t2.get_actions().begin();
	ASSERT(it->rl == rl);

}

void trans_double() {
	int rl = 10;
	seg_mgr * sm = &sdb::enginee::LOCAL_SEG_MGR;
	segment *seg = sm->find_segment(1L);
	data_item_ref dif;   // it references an existed data item.
	mem_data_block blk;
	blk.offset = 0;
	seg->read_block(blk);
	blk.parse_off_tbl();
	dif.seg_id = seg->get_id();
	dif.blk_off = blk.offset;
	dif.row_idx = blk.assign_row(rl);
	blk.write_off_tbl();

	char *n_buff1 = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff1[i] = 'A' + i;
	}

	transaction t1, t2;
	action a;
	a.dif = &dif;
	a.seq = 0;
	a.op = action_op::WRITE;
	a.create(n_buff1, rl);
	t1.add_action(a);

	char *n_buff2 = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff2[i] = 'a' + i;
	}

	action b = a;
	b.update(n_buff2, rl, n_buff1, rl);
	t2.add_action(b);

	trans_mgr *tm = &sdb::enginee::LOCAL_TRANS_MGR;
	tm->assign_trans(&t1);
	tm->assign_trans(&t2);
	t1.execute();
	t2.execute();

	// wait for the transaction completed
	std::this_thread::sleep_for(std::chrono::seconds(1));
	ASSERT(t1.status() == trans_status::COMMITTED);
}

void runSuite() {
	cute::suite s;
	s.push_back(CUTE(before));

	s.push_back(CUTE(trans_basic_test));
	s.push_back(CUTE(trans_basic_single));
	s.push_back(CUTE(trans_double));
	s.push_back(CUTE(after));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "Transaction Management Test");
}

int main() {
	runSuite();
	return 0;
}

