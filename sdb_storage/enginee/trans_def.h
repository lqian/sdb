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
	READ, WRITE, UPDATE,  DELETE
};

enum commit_flag {
	trans_leave, trans_on
};

struct data_item {
	ulong seg_id;
	uint blk_off;
	ushort row_idx;
};

struct data_item_ref : data_item {
	timestamp wts = 0; // write timestamp
	timestamp rts = 0; // read timestamp
	char cmt_flag = 0;

	mutex mtx;
	bool operator==(const data_item_ref & an);
};

struct action {
	ushort seq; // maybe
	action_op op;
	data_item_ref * dif = nullptr;  // data item ref

	char * buff = nullptr;
	int len;
	bool ref_flag = false;

	inline void ref(char * buff, int len) {
		this->buff = buff;
		this->len = len;
		this->ref_flag = true;
	}

	action& operator=(const action & an);

	action() {
	}
	action(const action & an);
	~action();
};

}
}
#endif /* TRANS_DEF_H */

