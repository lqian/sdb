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

log_mgr * lmgr;
seg_mgr * smgr;
trans_mgr * tmgr;

// share variants
char *n_buff;
int rl = 10;
segment *seg;
data_item_ref dif;
mem_data_block blk;

void before() {
	lmgr = &sdb::enginee::LOCAL_LOG_MGR;
	string path = "/tmp/logs";
	lmgr->set_path(path);
	lmgr->clean();
	lmgr->load();

	smgr = &sdb::enginee::LOCAL_SEG_MGR;
	smgr->add_datafile_paths("/tmp/dir1,/tmp/dir2");
	smgr->remove_files();
	smgr->load();


	tmgr = &sdb::enginee::LOCAL_TRANS_MGR;
	tmgr->open();

	// prepare data_item_ref
	smgr->assign_segment(seg);
	dif.seg_id = seg->get_id();
	seg->assign_block(blk);
	dif.row_idx = blk.assign_row(rl);
	dif.blk_off = blk.offset;
	n_buff = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff[i] = 'A' + i;
	}
}

void after() {
	lmgr->close();
	smgr->flush_all_segment();
	tmgr->close();
}
void trans_empty() {
	ASSERT(lmgr->is_open());
	transaction t1;
	timestamp ts = tmgr->get_curr_ts();
	tmgr->submit(&t1);
	ASSERT(tmgr->get_curr_ts() - ts == 1);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	ASSERT(t1.status() == trans_status::COMMITTED);
}

/*
 * test transaction management and transaction without multiple thread.
 * a transaction write a row represented with data_item_ref, then another
 * transaction read the row.
 */
void trans_write_read() {
	transaction t1;  // write transaction
	action a;
	a.dif = &dif;
	a.seq = 0;
	a.op = action_op::WRITE;
	a.ref(n_buff, rl);
	t1.add_action(a);
	t1.execute();
	ASSERT(t1.status() == trans_status::COMMITTED);

	transaction t2; // read transaction
	action b;
	b.dif = &dif;
	b.seq = 0;
	b.op = action_op::READ;
	t2.add_action(b);
	t2.execute();
	ASSERT(t2.status() == trans_status::COMMITTED);
	auto it = t2.get_actions().begin();
	ASSERT(it->len == rl && memcmp(it->buff, n_buff, rl) == 0);
}

void trans_double() {
	dif.row_idx = blk.assign_row(rl);  // assign a new row

	char *n_buff1 = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff1[i] = 'A' + i;
	}

	transaction t1, t2;
	action a;
	a.dif = &dif;
	a.seq = 0;
	a.op = action_op::WRITE;
	a.ref(n_buff1, rl);
	t1.add_action(a);

	char *n_buff2 = new char[rl];
	for (int i = 0; i < rl; i++) {
		n_buff2[i] = 'a' + i;
	}

	action b = a;
	b.op = action_op::UPDATE;
	b.ref(n_buff2, rl);
	t2.add_action(b);
	t1.execute();
	t2.execute();

	// wait for the transaction completed
	std::this_thread::sleep_for(std::chrono::seconds(1));
	ASSERT(t1.status() == trans_status::COMMITTED);
	ASSERT(t2.status() == trans_status::COMMITTED);
}

/*
 * manually abort a transaction and observe its new/old value
 */
void trans_abort() {
	int o_len = 10;
	char * o_buff = new char[o_len];
	for (int i = 0; i < o_len; i++) {
		o_buff[i] = 'a' + i;
	}
	int n_len = rl * 2;
	char * n_buff = new char[n_len];
	for (int i = 0; i < n_len; i++) {
		n_buff[i] = 'A' + i;
	}

	action a;
	a.dif = &dif;
	a.op = action_op::UPDATE;
	a.ref(n_buff, n_len);

	transaction t(false);
	t.add_action(a);
//	t.execute();
	tmgr->submit(&t);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	ASSERT(t.status() == trans_status::PARTIALLY_COMMITTED);
	char_buffer buff;
	// observe the new value
	int r = smgr->get_row(dif, buff);
	ASSERT(r == sdb::SUCCESS && buff.size() == n_len);
	ASSERT(memcmp(buff.data(), n_buff, n_len) == 0);

	t.abort();
	// observe the old value
	ASSERT(t.status() == trans_status::ABORTED);
	buff.reset();
	r = smgr->get_row(dif, buff);
	ASSERT(buff.size() == o_len && r == sdb::SUCCESS);
	ASSERT(memcmp(buff.data(), o_buff, rl) == 0);
}

/*
 * a transaction is waiting the transaction ahead to commit
 */
void trans_wait() {
	n_buff = "0123456789";
	action a;
	a.dif = &dif;
	a.op = action_op::WRITE;
	a.ref(n_buff, rl);

	transaction t(false), w;
	t.add_action(a);
	w.add_action(a);
	t.execute();
	tmgr->submit(&w);
	this_thread::sleep_for(chrono::seconds(1));
	ASSERT(w.status() == trans_status::ABORTED);
	t.commit();
	ASSERT(t.status() == trans_status::COMMITTED);
}

/*
 * a earlier transaction skip write action
 */

void trans_skip() {
	char * n_buff = "abcdefghij";
	char * n_buff2 = "qrstuvwxyz";

	action a;
	a.dif = &dif;
	a.op = action_op::WRITE;
	action b(a);
	a.ref(n_buff, rl);
	b.ref(n_buff2, rl);

	transaction t1, t2;
	tmgr->assign_trans(&t1);
	tmgr->assign_trans(&t2);
	t1.add_action(a);
	t2.add_action(b);
	t2.execute();
	ASSERT(t2.status() == COMMITTED);
	tmgr->submit(&t1, false);  // this transaction action should be skip
//	t1.execute();
	this_thread::sleep_for(chrono::seconds(1));
	ASSERT(t1.status() == COMMITTED);

	char_buffer buff;
	ASSERT(smgr->get_row(a.dif, buff) >= 0 );
	ASSERT(memcmp(buff.data(), n_buff2, rl) == 0);
}
void runSuite() {
	cute::suite s;
	s.push_back(CUTE(before));

	s.push_back(CUTE(trans_empty));
	s.push_back(CUTE(trans_write_read));
	s.push_back(CUTE(trans_double));
	s.push_back(CUTE(trans_abort));
	s.push_back(CUTE(trans_wait));
	s.push_back(CUTE(trans_skip));

	s.push_back(CUTE(after));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "Transaction Management Test");
}

int main() {
	runSuite();
	return 0;
}

