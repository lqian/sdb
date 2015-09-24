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
	this->_header.writing_data_off = length;
}

log_block::log_block(char *buff, int length) {
	this->length = length;
	this->buffer = buff;
	this->ref_flag = true;
	this->_header.writing_data_off = length;
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

void log_block::next_entry(dir_entry & e) {
	char_buffer dir(buffer + read_entry_off, DIRCTORY_ENTRY_LENGTH, true);
	e.read_from(dir);
	read_entry_off += DIRCTORY_ENTRY_LENGTH;
}

log_block::dir_entry log_block::get_entry(int idx) {
	dir_entry e;
	int off = idx * DIRCTORY_ENTRY_LENGTH;
	char_buffer buff(buffer + off, DIRCTORY_ENTRY_LENGTH, true);
	e.read_from(buff);
	return e;
}

void log_block::pre_entry(dir_entry & e) {
	read_entry_off -= DIRCTORY_ENTRY_LENGTH;
	char_buffer dir(buffer + read_entry_off, DIRCTORY_ENTRY_LENGTH, true);
	e.read_from(dir);
}

int log_block::copy_data(int idx, char_buffer & buff) {
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
			<< writing_data_off;
}

void log_block::header::read_from(char_buffer & buff) {
	buff >> magic >> offset >> pre_blk_off >> next_blk_off >> writing_entry_off
			>> writing_data_off;
}

void log_block::dir_entry::write_to(char_buffer & buff) {
	buff << ts << seq << offset << length;
}

void log_block::dir_entry::read_from(char_buffer & buff) {
	buff >> ts >> seq >> offset >> length;
}

dir_entry_type log_block::dir_entry::get_type() {
	if (length == 0) {
		if (offset == COMMIT_DEFINE) {
			return dir_entry_type::commit_item;
		} else if (offset == ROLLBACK_DEFINE) {
			return dir_entry_type::rollback_item;
		} else {
			return dir_entry_type::unknown;
		}
	} else {
		return dir_entry_type::data_item;
	}
}

void log_block::dir_entry::as_commit() {
	length = 0;
	offset = COMMIT_DEFINE;
}

void log_block::dir_entry::as_rollback() {
	length = 0;
	offset = ROLLBACK_DEFINE;
}

int log_block::add_action(timestamp ts, action & a) {
	if (remain() >= a.wl + DIRCTORY_ENTRY_LENGTH) {
		int weo = _header.writing_entry_off;
		dir_entry e;
		e.ts = ts;
		e.seq = a.seq;
		e.length = a.wl;
		e.offset = _header.writing_data_off - a.wl;

		char_buffer buff(DIRCTORY_ENTRY_LENGTH);
		e.write_to(buff);

		memcpy(buffer + weo, buff.data(), DIRCTORY_ENTRY_LENGTH);
		char * write_buffer = buffer + e.offset;
		memcpy(write_buffer, a.wd, a.wl);

		_header.writing_entry_off += DIRCTORY_ENTRY_LENGTH;
		_header.writing_data_off -= a.wl;
		return weo;
	} else {
		return LOG_BLK_SPACE_NOT_ENOUGH;
	}
}

int log_block::add_commit(timestamp ts) {
	if (remain() >= DIRCTORY_ENTRY_LENGTH) {
		dir_entry e;
		e.ts = ts;
		e.as_commit();
		char_buffer buff(DIRCTORY_ENTRY_LENGTH);
		e.write_to(buff);
		int weo = _header.writing_entry_off;
		memcpy(buffer + weo, buff.data(), DIRCTORY_ENTRY_LENGTH);
		_header.writing_entry_off += DIRCTORY_ENTRY_LENGTH;
		return weo;
	} else {
		return LOG_BLK_SPACE_NOT_ENOUGH;
	}
}

int log_block::add_rollback(timestamp ts) {
	if (remain() >= DIRCTORY_ENTRY_LENGTH) {
		dir_entry e;
		e.ts = ts;
		e.as_rollback();
		char_buffer buff(DIRCTORY_ENTRY_LENGTH);
		e.write_to(buff);
		int weo = _header.writing_entry_off;
		memcpy(buffer + weo, buff.data(), DIRCTORY_ENTRY_LENGTH);
		_header.writing_entry_off += DIRCTORY_ENTRY_LENGTH;
		return weo;
	} else {
		return LOG_BLK_SPACE_NOT_ENOUGH;
	}
}
int log_block::count_entry() {
	return _header.writing_entry_off / DIRCTORY_ENTRY_LENGTH;
}

int log_block::remain() {
	return _header.writing_data_off - _header.writing_entry_off;
}

void log_block::reset() {
	_header.magic = LOG_BLOCK_MAGIC;
	_header.writing_entry_off = 0;
	_header.writing_data_off = length;
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
		pathname(fn), initialized(false) {
	block_buffer = new char[_header.block_size];
}
log_file::~log_file() {
	delete[] block_buffer;
	log_stream.close();
}

int log_file::open() {
	int r = sdb::FAILURE;
	bool existed = sio::exist_file(pathname);
	if (!existed && init_log_file()) {
		_header.active = true;
	}

	log_stream.open(pathname.c_str(),
			ios_base::binary | ios_base::in | ios_base::out);
	if (log_stream.is_open()) {
		{
			// file existed, must check it has the correct magic,
			log_stream.seekg(ios_base::beg);
			char_buffer buff(LOG_FILE_HEADER_LENGTH);
			log_stream.read(buff.data(), LOG_FILE_HEADER_LENGTH);
			buff.rewind();
			_header.read_from(buff);

			//read the last block from file if the stream is good and log file is active
			if (_header.is_valid() && _header.active) {
				log_stream.seekg(0, ios_base::end);
				log_stream.seekg(0 - _header.block_size, ios_base::cur);
				log_stream.read(block_buffer, _header.block_size);

				if (log_stream.good()) {
					last_blk.ref_buffer(block_buffer, _header.block_size);
					initialized = true;
				}
			}
		}
		if (log_stream.good()) {
			r = sdb::SUCCESS;
		}
	}
	return r;
}

// do not support span blocks currently
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
		return append(ts, a);
	} else {
		return DATA_LENGTH_TOO_LARGE;
	}
}
int log_file::append(const char* buff, int len) {
	flush_last_block();
	log_stream.write(buff, len);
	if (_log_mgr->sync == log_sync::immeidate) {
		log_stream.flush();
	}
	if (log_stream.good()) {
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int log_file::renewal_last_block() {
	last_blk.ref_buffer(block_buffer, _header.block_size);
	last_blk._header.magic = LOG_BLOCK_MAGIC;
	last_blk._header.writing_entry_off = 0;

	//assign the empty block to the log file
	log_stream.seekp(ios_base::end);
	long p = log_stream.tellp();
	int d = (p - LOG_FILE_HEADER_LENGTH) % _header.block_size;
	if (d != 0) {
		log_stream.seekp(0 - d, ios_base::cur);
	}
	log_stream.write(block_buffer, _header.block_size);
	return log_stream.good() ? sdb::SUCCESS : sdb::FAILURE;
}

int log_file::flush_last_block() {
	log_stream.seekg(0, ios_base::end);
	long p = log_stream.tellg();
	int d = (p - LOG_FILE_HEADER_LENGTH) % _header.block_size;
	log_stream.seekg(0 - d - _header.block_size, ios_base::cur);
	p = log_stream.tellg();
	p = log_stream.tellp();
	char_buffer buff(block_buffer, _header.block_size, true);
	last_blk._header.write_to(buff);
	log_stream.write(block_buffer, _header.block_size);
	return log_stream.good() ? sdb::SUCCESS : sdb::FAILURE;
}

int log_file::init_log_file() {
	fstream out;
	out.open(pathname.c_str(), ios_base::binary | ios_base::app);
	if (out.is_open()) {
		char_buffer buff(LOG_FILE_HEADER_LENGTH);
		this->_header.write_to(buff);
		buff.rewind();

		last_blk.ref_buffer(block_buffer, _header.block_size);
		last_blk.reset();
		char_buffer head_buff(block_buffer, _header.block_size, true);
		last_blk._header.write_to(head_buff);

		out.write(buff.data(), LOG_FILE_HEADER_LENGTH);
		out.write(block_buffer, _header.block_size);
		out.flush();
		return out.good() ? sdb::SUCCESS : sdb::FAILURE;
	} else {
		return sdb::FAILURE;
	}
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

void log_file::set_log_mgr(log_mgr * lm) {
	this->_log_mgr = lm;
}

log_mgr::log_mgr() {

}

log_mgr::~log_mgr() {

}

} /* namespace enginee */
} /* namespace sdb */
