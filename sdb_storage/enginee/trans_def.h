/*
 * trans_def.h
 *
 *  Created on: Oct 16, 2015
 *      Author: lqian
 */

#ifndef TRANS_DEF_H
#define TRANS_DEF_H

#include "../common/char_buffer.h"
#include <mutex>

const short NEW_VALUE_BIT = 7;
const short OLD_VALUE_BIT = 6;

const int ACTION_HEADER_LENGTH = 15;

typedef unsigned long timestamp;

namespace sdb {
namespace enginee {

using namespace std;
using namespace sdb::common;

enum action_op {
	WRITE, READ
};

enum  commit_flag {
	trans_leave, trans_on
};
struct data_item_ref {
	ulong seg_id;
	uint blk_off;
	ushort row_idx;
	timestamp wts = 0; // write timestamp
	timestamp rts = 0; // read timestamp
	char cmt_flag = 0;

	mutex mtx;
	bool operator==(const data_item_ref & an);
	void lock();
	void un_lock();
};

struct action {
	unsigned short seq; // maybe
	action_op op;
	data_item_ref * dif = nullptr;  // data item ref
	bool assign_dif = false;

	char flag = 0;
	char * wd = nullptr; // action data write buffer, includes data_item_ref  and flag
	int wl = 0; // action data write length, include data_item_ref and flag

	char * rd = nullptr;
	int rl = 0;

	/*
	 * op is write, but the action doesn't have old data, the new data specified
	 * with two parameters, the method assume that data_item_ref has been assigned
	 */
	void create(char * n_buff, int n_len);

	void update(char * n_buff, int n_len, char * o_buff, int o_len);

	void remove(char * o_buff, int o_len);

	action& operator=(const action & an);

	void read_from(char_buffer & buff);
	void write_to(char_buffer & buff);
	int copy_nitem(char * buff);
	int copy_oitem(char * buff);
	int ref_oitem(char *buff);
	int ref_nitem(char * buff);

	action() {
	}
	action(const action & an);
	~action();
};

}
}
#endif /* TRANS_DEF_H */

