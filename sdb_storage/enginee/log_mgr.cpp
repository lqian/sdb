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

log_block::log_block(char *buff, int length) {
	this->length = length;
	this->buffer = buff;
	this->ref_flag = true;
	this->_header.remain_space = length;
}

log_block::log_block(const log_block & another) {
	this->length = another.length;
	if (ref_flag) {
		this->buffer = another.buffer;
	} else {
		this->buffer = new char[length];
		memcpy(buffer, another.buffer, length);
	}
	this->_header = another._header;
	this->read_entry_off = another.read_entry_off;
}

log_block & log_block::operator =(const log_block & another) {
	if (this != &another) {
		this->length = another.length;
		if (ref_flag) {
			this->buffer = another.buffer;
		} else {
			this->buffer = new char[length];
			memcpy(buffer, another.buffer, length);
		}
		this->_header = another._header;
		this->read_entry_off = another.read_entry_off;
	}
	return *this;
}

void log_block::head() {
	read_entry_off = 0;
}

void log_block::tail() {
	read_entry_off = _header.writing_entry_off;
}

bool log_block::has_next() {
	return read_entry_off < _header.writing_entry_off;
}

bool log_block::has_pre() {
	return read_entry_off > 0;
}

void log_block::next_entry(char_buffer & buff) {
	char_buffer dir(buffer + read_entry_off, DIRCTORY_ENTRY_LENGTH, true);
	dir_entry e;
	e.read_from(dir);
	buff.push_back(buffer + e.offset, e.length, false);
	read_entry_off += DIRCTORY_ENTRY_LENGTH;
}

void log_block::pre_entry(char_buffer & buff) {
	read_entry_off -= DIRCTORY_ENTRY_LENGTH;
	char_buffer dir(buffer + read_entry_off, DIRCTORY_ENTRY_LENGTH, true);
	dir_entry e;
	e.read_from(dir);
	buff.push_back(buffer + e.offset, e.length, false);
}

int log_block::get_entry(int idx, char_buffer & buff) {
	int ent_off = idx * DIRCTORY_ENTRY_LENGTH;
	if (ent_off >= _header.writing_entry_off) {
		return OUTOF_ENTRY_INDEX;
	}
	dir_entry * e = (dir_entry *) (buffer + ent_off);
	buff.push_back(buffer + e->offset, e->length, false);
	return sdb::SUCCESS;;
}

log_block::~log_block() {
	if (!ref_flag) {
		delete[] buffer;
	}
}

void log_block::dir_entry::write_to(char_buffer & buff) {
	buff << ts << seq << offset << length;
}

void log_block::dir_entry::read_from(char_buffer & buff) {
	buff >> ts >> seq >> offset >> length;
}

int log_block::add_action(timestamp ts, action & a) {
	if (_header.remain_space >= a.wl + DIRCTORY_ENTRY_LENGTH) {
		char_buffer buff(DIRCTORY_ENTRY_LENGTH);
		int weo = _header.writing_entry_off;
		dir_entry e;
		e.ts = ts;
		e.seq = a.seq;
		e.length = a.wl;
		if (weo == 0) {
			e.offset = length - a.wl;
		} else {
			int leo = weo - DIRCTORY_ENTRY_LENGTH;
			buff.push_back(buffer + leo, DIRCTORY_ENTRY_LENGTH, false);
			dir_entry le;
			buff.rewind();
			le.read_from(buff);
			e.offset = le.offset - a.wl;
		}
		buff.reset();
		e.write_to(buff);

		memcpy(buffer + weo, buff.data(), DIRCTORY_ENTRY_LENGTH);
		char * write_buffer = buffer + e.offset;
		memcpy(write_buffer, a.wd, a.wl);

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
