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
	this->length = block_size - LOG_BLOCK_HEADER_LENGTH;
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

void log_block::header::write_to(char_buffer & buff) {
	buff << magic << offset << pre_blk_off << next_blk_off << writing_entry_off
			<< remain_space;
}

void log_block::header::read_from(char_buffer & buff) {
	buff >> magic >> offset >> pre_blk_off >> next_blk_off >> writing_entry_off
			>> remain_space;
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

void log_block::assign_block_buffer() {
	buffer = new char[length];
}

void log_block::set_block_size(int bs) {
	this->length = bs - LOG_BLOCK_HEADER_LENGTH;
}

void log_block::ref_buffer(char *buff, int len) {
	ref_flag = true;
	char_buffer head(buff, LOG_BLOCK_HEADER_LENGTH, true);
	_header.read_from(head);
	buffer = buff + LOG_BLOCK_HEADER_LENGTH;
	length = len - LOG_BLOCK_HEADER_LENGTH;
}

log_file::log_file(const string & fn) :
		pathname(fn), active(false), initialized(false) {
	block_buffer = new char[_header.block_size];
}
log_file::~log_file() {
	delete[] block_buffer;
}

int log_file::open() {
	int r = sdb::FAILURE;
	bool existed = sio::exist_file(pathname);
	log_stream.open(pathname.c_str(),
			ios_base::in | ios_base::out | ios_base::binary);
	if (log_stream.is_open() && log_stream.good()) {
		if (!existed) {
			r = init_file_header();
			_header.active = true;
			last_blk.ref_buffer(block_buffer, _header.block_size);
			last_blk._header.magic = LOG_BLOCK_MAGIC;
		} else {
			// file existed, must check it has the correct magic,
			log_stream.seekg(ios_base::beg);
			char_buffer buff(LOG_FILE_HEADER_LENGTH);
			log_stream.read(buff.data(), LOG_FILE_HEADER_LENGTH);
			buff.rewind();
			_header.read_from(buff);

			//read the last block from file if the stream is good and log file is active
			if (_header.is_valid() && _header.active) {
				log_stream.seekg(ios_base::end);
				log_stream.seekg(0 - _header.block_size, ios_base::cur);
				log_stream.read(block_buffer, _header.block_size);

				if (log_stream.good()) {
					last_blk.ref_buffer(block_buffer, _header.block_size);
					initialized = true;
				}
			}
		}
		if (log_stream.good()) {
			active = true;
			r = sdb::SUCCESS;
		}
	}
	return r;
}

// do not support span blocks
int log_file::append(timestamp ts, action &a) {
	int r = last_blk.add_action(ts, a);
	if (r != LOG_BLK_SPACE_NOT_ENOUGH) {
		if (_log_mgr->sync == log_sync::immeidate) {
			flush_last_block();
		}
	} else if (_header.block_size - DIRCTORY_ENTRY_LENGTH
			- LOG_BLOCK_HEADER_LENGTH > a.wl) {
		// the data content less than empty block buffer size
		flush_last_block();
		renewal_last_block();
		return last_blk.add_action(ts, a);
	}
}

int log_file::append(const char* buff, int len) {
	flush_last_block();
	log_stream.write(buff, len);
	if (_log_mgr->sync == log_sync::immeidate) {
		log_stream.flush();
	}
	if (log_stream.good()) {
		renewal_last_block();
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}

}

int log_file::renewal_last_block() {
	int r = sdb::FAILURE;
	return r;
}

int log_file::init_file_header() {
	char_buffer buff(LOG_FILE_HEADER_LENGTH);
	this->_header.write_to(buff);
	buff.rewind();
	log_stream.write(buff.data(), LOG_FILE_HEADER_LENGTH);
	log_stream.flush();
	return log_stream.good() ? sdb::SUCCESS : sdb::FAILURE;
}

bool log_file::header::is_valid() {
	return magic == LOG_FILE_MAGIC;
}
void log_file::header::write_to(char_buffer & buff) {
	buff << magic << block_size << active;
}
void log_file::header::read_from(char_buffer & buff) {
	buff >> magic >> block_size >> active;
}

log_mgr::log_mgr() {

}

log_mgr::~log_mgr() {

}

} /* namespace enginee */
} /* namespace sdb */
