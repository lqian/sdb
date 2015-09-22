/*
 * log_mgr.cpp
 *
 *  Created on: Sep 11, 2015
 *      Author: lqian
 */

#include "log_mgr.h"

namespace sdb {
namespace enginee {
log_block::log_block(int block_size) {
	this->length = block_size - BLOCK_HEADER_LENGTH;
	this->buffer = new char[length];
	this->_header.remain_space = length;
}

log_block::log_block(char *buff, int block_size) {
	this->length = block_size - BLOCK_HEADER_LENGTH;
	this->buffer = buff;
	this->ref_flag = true;
	this->_header.remain_space = length;
}

log_block::~log_block() {
	if (!ref_flag) {
		delete[] buffer;
	}
}

int log_block::add_action(timestamp ts, action & a) {
	if (_header.remain_space >= a.wl + DIRCTORY_ENTRY_LENGTH) {
		int weo = _header.writing_entry_off;
		dir_entry e;
		e.ts = ts;
		e.seq = a.seq;
		e.length = a.wl;
		if (weo == 0) {
			e.offset = length - a.wl;
		} else {
			dir_entry *le = (dir_entry *) (buffer + weo - DIRCTORY_ENTRY_LENGTH );
			e.offset = le->offset - a.wl;
		}

		memcpy(buffer + weo, &e, DIRCTORY_ENTRY_LENGTH);
		char * write_buffer = buffer + e.offset;
		memcpy(a.wd, write_buffer, a.wl);

		_header.writing_entry_off += DIRCTORY_ENTRY_LENGTH;
		_header.remain_space -= (a.wl + DIRCTORY_ENTRY_LENGTH);
		return weo;
	} else {
		return LOG_BLK_SPACE_NOT_ENOUGH;
	}
}

int log_block::count_entry() {
	return _header.writing_entry_off / DIRCTORY_ENTRY_LENGTH;
}

log_mgr::log_mgr() {

}

log_mgr::~log_mgr() {

}

} /* namespace enginee */
} /* namespace sdb */
