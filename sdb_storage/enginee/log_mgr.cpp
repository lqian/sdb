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

void log_block::seek(int dir_ent_off) {
	read_entry_off = dir_ent_off;
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
	return sdb::SUCCESS;
}

void log_block::copy_data(const dir_entry &e, char_buffer & buff) {
	buff.push_back(buffer + e.offset, e.length, false);
}

void log_block::copy_data(const dir_entry &e, action &a) {
	char_buffer buff(buffer + e.offset, e.length, true);
	a.read_from(buff);
}

ulong log_block::get_seg_id(const dir_entry & e) {
	ulong * p = (ulong *) buffer + e.offset;
	return p[0];
}

log_block::~log_block() {
	if (!ref_flag && ass_flag) {
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
		} else if (offset == ABORT_DEFINE) {
			return dir_entry_type::abort_item;
		} else if (offset == START_DEFINE) {
			return dir_entry_type::start_item;
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

void log_block::dir_entry::as_start() {
	length = 0;
	offset = START_DEFINE;
}

void log_block::dir_entry::as_abort() {
	length = 0;
	offset = ABORT_DEFINE;
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

int log_block::add_start(timestamp ts) {
	if (remain() >= DIRCTORY_ENTRY_LENGTH) {
		dir_entry e;
		e.ts = ts;
		e.as_start();
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

int log_block::add_abort(timestamp ts) {
	if (remain() >= DIRCTORY_ENTRY_LENGTH) {
		dir_entry e;
		e.ts = ts;
		e.as_abort();
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
	ass_flag = true;
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

void log_block::write_to(char *buff) {
	char_buffer tmp(LOG_BLOCK_HEADER_LENGTH);
	_header.write_to(tmp);
	int len = tmp.size();
	memcpy(buff, tmp.data(), len);  // copy header and content
	memcpy(buff + len, buffer, length);

}

log_file::log_file() {
}
log_file::log_file(const string & fn) :
		pathname(fn), initialized(false) {

}
log_file::~log_file() {
	close();

	if (write_buffer) {
		delete[] write_buffer;
	}
	if (cutoff_point) {
		delete cutoff_point;
	}
}

bool log_file::operator ==(const log_file & an) {
	return an.pathname == pathname;
}

bool log_file::operator !=(const log_file & an) {
	return an.pathname != pathname;
}
int log_file::open() {
	return open(pathname);
}

int log_file::open(const string & pathname) {
	int r = sdb::FAILURE;
	this->pathname = pathname;
	bool existed = sio::exist_file(pathname);
	if (!existed) {
		r = init_log_file();
		if (r == sdb::FAILURE) {
			return INIT_LOG_FILE_FAILURE;
		}
	}

	log_stream.open(pathname.c_str(),
			ios_base::binary | ios_base::in | ios_base::out);
	if (log_stream.is_open()) {
		// file existed, must check it has the correct magic,
		log_stream.seekg(ios_base::beg);
		char_buffer buff(LOG_FILE_HEADER_LENGTH);
		log_stream.read(buff.data(), LOG_FILE_HEADER_LENGTH);
		_header.read_from(buff);

		//read the last block from file if the stream is good and log file is active
		write_buffer = new char[_header.block_size];
		if (_header.is_valid()) {
			log_stream.seekg(0, ios_base::end);
			log_stream.seekg(0 - _header.block_size, ios_base::cur);
			log_stream.read(write_buffer, _header.block_size);

			if (log_stream.good()) {
				last_blk.ref_buffer(write_buffer, _header.block_size);
				initialized = true;
			}
		}
		if (log_stream.good()) {
			r = sdb::SUCCESS;
			opened = true;
		} else {
			r = LOG_STREAM_ERROR;
		}

		if (opened && !_header.active) {
			// read the last check_point from check file with the same file name
			string chk_path = pathname;
			chk_path.replace(chk_path.end() - 4, chk_path.end(),
					CHECKPOINT_FILE_SUFFIX);

			if (sio::exist_file(chk_path)) {
				checkpoint_file chk_file;

				chk_file.open(chk_path);
				check_point cp;
				r = chk_file.last_checkpoint(cp);
				chk_file.close();
				if (r) {
					cutoff_point = new check_point;
					cutoff_point->ts = cp.ts;
					cutoff_point->log_blk_off = cp.log_blk_off;
					cutoff_point->dir_ent_off = cp.dir_ent_off;
				}
			}
		}
	}
	return r;
}

int log_file::re_open() {
	log_stream.close();
	log_stream.open(pathname.c_str(),
			ios_base::binary | ios_base::in | ios_base::out);
	return log_stream.is_open() ? sdb::SUCCESS : LOG_STREAM_ERROR;
}

// do not support span blocks currently
int log_file::append(timestamp ts, action &a) {
	int r = last_blk.add_action(ts, a);
	if (r != LOG_BLK_SPACE_NOT_ENOUGH) {
		if (_log_mgr->sync_police == log_sync_police::immeidate) {
			flush_last_block();
		}
	} else if (_header.block_size - DIRCTORY_ENTRY_LENGTH
			- LOG_BLOCK_HEADER_LENGTH > a.wl) {
		// the data content less than empty block buffer size
		flush_last_block();
		r = renewal_last_block();
		if (r >= 0) {
			r = append(ts, a);
		}
		return r;
	} else {
		flush_last_block(); // make sure flush last block
		return DATA_LENGTH_TOO_LARGE;
	}
}

int log_file::append(const char* buff, int len) {
	flush_last_block();
	log_stream.write(buff, len);
	if (_log_mgr->sync_police == log_sync_police::immeidate) {
		log_stream.flush();
	}
	if (log_stream.good()) {
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int log_file::append_commit(timestamp ts) {
	int r = last_blk.add_commit(ts);
	if (r != LOG_BLK_SPACE_NOT_ENOUGH) {
		if (_log_mgr->sync_police == log_sync_police::immeidate) {
			flush_last_block();
		}
	} else {
		flush_last_block();
		r = renewal_last_block();
		if (r >= 0) {
			r = append_commit(ts);
		}
		return r;
	}
}

int log_file::append_abort(timestamp ts) {
	int r = last_blk.add_abort(ts);
	if (r != LOG_BLK_SPACE_NOT_ENOUGH) {
		if (_log_mgr->sync_police == log_sync_police::immeidate) {
			flush_last_block();
		}
	} else {
		flush_last_block();
		r = renewal_last_block();
		if (r >= 0) {
			r = append_abort(ts);
		}
		return r;
	}
}

int log_file::append_start(timestamp ts) {
	int r = last_blk.add_start(ts);
	if (r != LOG_BLK_SPACE_NOT_ENOUGH) {
		if (_log_mgr->sync_police == log_sync_police::immeidate) {
			flush_last_block();
		}
	} else {
		flush_last_block();
		r = renewal_last_block();
		if (r >= 0) {
			r = append_start(ts);
		}
		return r;
	}
}

int log_file::renewal_last_block() {
	last_blk.reset();

	//assign the empty block to the log file
	log_stream.seekp(0, ios_base::end);
	long p = log_stream.tellp();

	if (p >= LOG_FILE_HEADER_LENGTH + _log_mgr->log_file_max_size) {
		return EXCEED_LOGFILE_LIMITATION;
	}

	int bs = _header.block_size;
	int d = (p - LOG_FILE_HEADER_LENGTH) % bs;
	if (d != 0) {
		log_stream.seekp(-d, ios_base::cur);
	}
	last_blk._header.offset = p - d;
	last_blk.write_to(write_buffer);
	log_stream.write(write_buffer, bs);
	return log_stream.good() ? sdb::SUCCESS : sdb::FAILURE;
}

int log_file::flush_last_block() {
	log_stream.seekg(0, ios_base::end);
	long p = log_stream.tellg();
	int d = (p - LOG_FILE_HEADER_LENGTH) % _header.block_size;
	log_stream.seekg(0 - d - _header.block_size, ios_base::cur);
	char_buffer buff(write_buffer, _header.block_size, true);
	last_blk._header.write_to(buff);
	log_stream.write(write_buffer, _header.block_size);
	return log_stream.good() ? sdb::SUCCESS : sdb::FAILURE;
}

int log_file::init_log_file() {
	fstream out;
	out.open(pathname.c_str(), ios_base::binary | ios_base::app);
	if (out.is_open()) {
		opened = true;
		write_buffer = new char[_header.block_size];
		char_buffer buff(LOG_FILE_HEADER_LENGTH);
		_header.active = true;
		_header.write_to(buff);
		out.write(buff.data(), LOG_FILE_HEADER_LENGTH);

		last_blk.ref_buffer(write_buffer, _header.block_size);
		last_blk.reset();
		char_buffer head_buff(write_buffer, _header.block_size, true);
		last_blk._header.write_to(head_buff);

		out.write(write_buffer, _header.block_size);
		out.flush();
		delete[] write_buffer;
		return out.good() ? sdb::SUCCESS : sdb::FAILURE;
	} else {
		return sdb::FAILURE;
	}
}

bool log_file::is_open() {
	return opened;
}

int log_file::close() {
	if (opened) {
		log_stream.close();
	}
	opened = false;
}

bool log_file::is_active() {
	return _header.active;
}

int log_file::next_block(char * buff) {
	int bs = _header.block_size;
	if (read_blk_offset > last_blk._header.offset) {
		return sdb::FAILURE;
	}
	if (read_blk_offset == last_blk._header.offset) {
		last_blk.write_to(buff);
		read_blk_offset += bs;
		return sdb::SUCCESS;
	} else {
		long p = log_stream.tellg();
		log_stream.seekg(read_blk_offset - p, ios_base::cur);
		log_stream.read(buff, bs);
		if (log_stream.good()) {
			read_blk_offset += bs;
			return sdb::SUCCESS;
		} else {
			return LOG_STREAM_ERROR;
		}
	}
}

int log_file::pre_block(char *buff) {
	if (read_blk_offset <= LOG_FILE_HEADER_LENGTH) {
		return sdb::FAILURE;
	}

	int bs = _header.block_size;
	long p = log_stream.tellg();
	if (read_blk_offset == last_blk._header.offset + _header.block_size) {
		last_blk.write_to(buff);
	} else {
		log_stream.seekg(read_blk_offset - bs - p, ios_base::cur);
		log_stream.read(buff, bs);
	}
	if (log_stream.good()) {
		read_blk_offset -= bs;
		return sdb::SUCCESS;
	} else {
		return LOG_STREAM_ERROR;
	}
}

int log_file::head() {
	int r = sdb::FAILURE;
	if (log_stream.eof() || log_stream.bad()) {
		r = re_open();
	}
	if (r) {
		log_stream.seekg(LOG_FILE_HEADER_LENGTH, ios_base::beg);
		read_blk_offset = LOG_FILE_HEADER_LENGTH;
		return log_stream.good() ? sdb::SUCCESS : LOG_STREAM_ERROR;
	}
	return r;
}

int log_file::seek(int blk_off) {
	int r = sdb::FAILURE;
	if (log_stream.eof() || log_stream.bad()) {
		r = re_open();
	}
	if (r) {
		int p = log_stream.tellg();
		log_stream.seekg(blk_off - p, ios_base::cur);
		read_blk_offset = blk_off;
		return log_stream.good() ? sdb::SUCCESS : LOG_STREAM_ERROR;
	}
	return r;
}

int log_file::tail() {
	int r = sdb::SUCCESS;
	if (log_stream.eof() || log_stream.bad()) {
		r = re_open();
		if (r != sdb::SUCCESS)
			return r;
	}

	log_stream.seekg(0, ios_base::end);
	read_blk_offset = (int) log_stream.tellg();
	return log_stream.good() ? sdb::SUCCESS : LOG_STREAM_ERROR;

}

int log_file::inactive() {
	_header.active = false;
	char_buffer buff(LOG_FILE_HEADER_LENGTH);
	_header.write_to(buff);
	log_stream.seekg(0, ios_base::beg);
	log_stream.write(buff.data(), buff.capacity());
	log_stream.flush();
	return log_stream.good() ? sdb::SUCCESS : LOG_STREAM_ERROR;
}

void log_file::check_log_block(log_block & lb) {
	log_block::dir_entry de;
	while (lb.has_next()) {
		lb.next_entry(de);
		dir_entry_type t = de.get_type();
		if (t == dir_entry_type::start_item) {
			check_point cp;
			cp.ts = de.ts;
			cp.log_file_seq = log_file_id;
			cp.log_blk_off = read_blk_offset;
			cp.dir_ent_off = lb.read_entry_off;
			check_list.push_front(cp);
		} else if (t == dir_entry_type::abort_item
				|| t == dir_entry_type::commit_item) {
			check_point cp;
			cp.ts = de.ts;
			cp.log_file_seq = log_file_id;
			cp.log_blk_off = read_blk_offset;
			cp.dir_ent_off = lb.read_entry_off;
			check_list.remove(cp);
		} else if (t == dir_entry_type::data_item) {
			// gather segment to be flush
			check_segs.insert(lb.get_seg_id(de));
		}
	}
}

int log_file::rfind(timestamp ts, list<action> & actions) {
	int r = NOT_FIND_TRANSACTION;
	log_block lb;
	log_block::dir_entry de;
	// if find the <start ts> return
	r = tail();
	if (r) {
		int bs = _header.block_size;
		char * buff = new char[bs];
		while (pre_block(buff)) {
			lb.ref_buffer(buff, bs);
			lb.tail();
			while (lb.has_pre()) {
				lb.pre_entry(de);
				if (de.ts == ts) {
					if (de.get_type() == start_item) {
						return FIND_TRANSACTION_START;
					} else if (de.get_type() == data_item) {
						action a;
						r = CONTINUE_TO_FIND;
						lb.copy_data(de, a);
						actions.push_back(a);
					}
				}
			}
		}
	}
	return r;
}

int log_file::irfind(timestamp ts, list<action> & actions) {
	int r = sdb::FAILURE;
	if (!opened) {
		r = open();
	}
	if (!r) {
		return r;
	}

	if (active) {
		return INVALID_OPS_ON_ACTIVE_LOG_FILE;
	}

	r = tail();
	if (r) {
		int bs = _header.block_size;
		char *buff = new char[bs];
		log_block lb;
		log_block::dir_entry de;
		int lcbo = cutoff_point ? cutoff_point->log_blk_off : 0; // last check block offset
		while (read_blk_offset >= lcbo && pre_block(buff)) {
			lb.ref_buffer(buff, bs);
			lb.tail();

			while (lb.has_pre()) {
				lb.pre_entry(de);
				if (de.ts == ts) {
					if (de.get_type() == start_item) {
						return FIND_TRANSACTION_START;
					} else if (de.get_type() == data_item) {
						r = CONTINUE_TO_FIND;
						action a;
						lb.copy_data(de, a);
						actions.push_back(a);
					}
				}
			}
		}
	}

	return r;

}

int log_file::check(int blk_off, int dir_entry_off) {

	if (_header.active) {
		return LOG_FILE_IS_ACTIVE;
	}

	int r = sdb::FAILURE;
	int check_offset = -1;
	r = seek(blk_off);
	if (r) {
		char * buff = new char[_header.block_size];
		log_block lb;
		if (next_block(buff) == sdb::SUCCESS) { // do check the non-completed log block
			lb.ref_buffer(buff, _header.block_size);
			lb.seek(dir_entry_off);
			check_log_block(lb);
		}

		// others whole block
		while (next_block(buff) == sdb::SUCCESS) {
			lb.ref_buffer(buff, _header.block_size);
			check_log_block(lb);
		}

		delete[] buff;

		// assume all logs has completed
		if (cutoff_point == nullptr) {
			cutoff_point = new check_point;
		}
		cutoff_point->ts = 0;
		cutoff_point->log_blk_off = read_blk_offset;
		cutoff_point->dir_ent_off = lb.read_entry_off;

	}
	return r;
}

int log_file::cut_off() {
	int r = sdb::FAILURE;

	if (!check_list.empty()) {
		auto it = check_list.begin();
		cutoff_point->ts = it->ts;
		cutoff_point->log_blk_off = it->log_blk_off;
		cutoff_point->dir_ent_off = it->dir_ent_off;
	}

	string chk_path = pathname;
	chk_path.replace(chk_path.end() - 4, chk_path.end(),
			CHECKPOINT_FILE_SUFFIX);
	checkpoint_file chk_file;
	r = chk_file.open(chk_path);
	if (r) {
		r = chk_file.write_checkpoint(cutoff_point);
	}
	return r;

}

void log_file::set_block_size(int bs) {
	_header.block_size = bs;
}
int log_file::get_block_size() {
	return _header.block_size;
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

bool check_point::operator ==(const check_point & an) {
	return ts == an.ts ;
}

void log_file::set_log_mgr(log_mgr * lm) {
	this->_log_mgr = lm;
}

void log_mgr::set_log_file_max_size(int size) {
	log_file_max_size = size;
}
void log_mgr::set_sync_police(const enum log_sync_police & sp) {
	sync_police = sp;
}

const log_sync_police & log_mgr::get_sync_police() const {
	return sync_police;
}

int log_mgr::load() {
	return load(path);
}
int log_mgr::load(const string &path) {
	int r = sdb::FAILURE;
	this->path = path;
	if (!sio::exist_file(path)) {
		r = sio::make_dir(path);
	}

	r = in_lock();
	if (r == LOCKED_LOG_MGR_PATH) {
		return r;
	}

	list<string> log_files = sio::list_file(path, LOG_FILE_SUFFIX);
	for (auto & lf : log_files) {
		ulong id = strtoul(lf.data(), nullptr, 16);
		if (id > log_file_seq) {
			log_file_seq = id;
		}
	}

	char *alphas = new char[16];
	ultoa(log_file_seq, alphas);
	string pathname = this->path + "/" + alphas + LOG_FILE_SUFFIX;
	string chkpath = this->path + "/" + alphas + CHECKPOINT_FILE_SUFFIX;

	r = curr_log_file.open(pathname);
	if (r) {
		curr_log_file.set_log_mgr(this);
		//if (sio::exist_file(chkpath)) {
		//open last checkpoint file match the last log file
		//	chk_file_seq = log_file_seq;
		//} else

		{  // scan the last checkpoint file name
			list<string> chk_files = sio::list_file(path,
					CHECKPOINT_FILE_SUFFIX);
			for (auto & cf : chk_files) {
				ulong id = strtoul(cf.data(), nullptr, 16);
				if (id > chk_file_seq) {
					chk_file_seq = id;
				}
			}
		}
	}

	curr_log_file.set_log_mgr(this);
	delete[] alphas;
	opened = true;
	return r;
}

int log_mgr::in_lock() {
	if (sio::exist_file(lock_pathname)) {
		return LOCKED_LOG_MGR_PATH;
	}

	fstream fs;
	fs.open(lock_pathname.c_str(), ios_base::binary | ios_base::app);
	fs.close();
	return fs.good() ? sdb::SUCCESS : LOCK_STREAM_ERROR;
}

int log_mgr::out_lock() {
	int r = sdb::FAILURE;
	if (sio::exist_file(lock_pathname.c_str())) {
		r = sio::remove_file(lock_pathname);
	}
	return r ? sdb::SUCCESS : OUT_LOCK_LOGMGR_FAILURE;
}

void log_mgr::add_check_snap(const log_file & lf) {
	check_segs.insert(lf.check_segs.begin(), lf.check_segs.end());
	check_points.insert_after(check_points.end(), lf.check_list.begin(),
			lf.check_list.end());
}

int log_mgr::renew_log_file() {
	char *alphas = new char[16];
	ulong id = log_file_seq;
	ultoa(++id, alphas);
	string name = alphas + LOG_FILE_SUFFIX;
	string pathname = path + "/" + name;
	curr_log_file.inactive();
	curr_log_file.close();
	int r = curr_log_file.open(pathname);
	if (r) {
		log_file_seq = id;
	}
	delete[] alphas;
	return r;
}

int log_mgr::close() {
	int r = sdb::FAILURE;
	r = out_lock();
	return r;
}

void log_mgr::clean() {
	list<string> names;
	sio::list_file(path, names);
	for (auto & name : names) {
		string pathname = path + "/" + name;
		sio::remove_file(pathname);
	}
}

int log_mgr::flush() {
	int r = sdb::FAILURE;
	if (sync_police == log_sync_police::buffered) {

	} else if (sync_police == log_sync_police::immeidate) {
		r = sdb::SUCCESS;
	} else if (sync_police == log_sync_police::async) {

	}
	return r;
}

bool log_mgr::is_open() {
	return opened;
}

log_mgr::log_mgr() {

}

log_mgr::log_mgr(const string &path) {
	this->path = path;
	lock_pathname = path + "/" + LOG_MGR_LOCK_FILENAME;
}

log_mgr::~log_mgr() {
	if (opened) {
		flush();
		close();
		out_lock();
	}
}

int log_mgr::log_start(timestamp ts) {
	int r = curr_log_file.append_start(ts);
	if (r == EXCEED_LOGFILE_LIMITATION) {
		r = renew_log_file();
		if (r) {
			return curr_log_file.append_start(ts);
		}
	}

	return r;
}

int log_mgr::log_action(timestamp ts, action & a) {
	int r = curr_log_file.append(ts, a);
	if (r == EXCEED_LOGFILE_LIMITATION) {
		r = renew_log_file();
		if (r) {
			return curr_log_file.append(ts, a);
		}
	}
	return r;
}

int log_mgr::log_commit(timestamp ts) {
	int r = curr_log_file.append_commit(ts);
	if (r == EXCEED_LOGFILE_LIMITATION) {
		r = renew_log_file();
		if (r) {
			return curr_log_file.append_commit(ts);
		}
	}
	return r;
}

int log_mgr::log_abort(timestamp ts) {
	int r = curr_log_file.append_abort(ts);
	if (r == EXCEED_LOGFILE_LIMITATION) {
		r = renew_log_file();
		if (r) {
			return curr_log_file.append_abort(ts);
		}
	}
	return r;
}

int log_mgr::rfind(timestamp ts, list<action> & actions) {
	int r = sdb::FAILURE;
	if (sync_police == log_sync_police::immeidate) {
		r = curr_log_file.rfind(ts, actions);

		if (r != FIND_TRANSACTION_START) {
			r = lf.irfind(ts, actions);
		}
	}

	return r;
}

int log_mgr::check() {
	int r = sdb::FAILURE;
	char *alphas = new char[16];
	ultoa(chk_file_seq, alphas);
	string name = alphas + LOG_FILE_SUFFIX;
	string pathname = path + "/" + name;

	list<string> log_files = sio::list_file(path, LOG_FILE_SUFFIX);
	log_file lf;
	if (chk_file_seq > 0) { // last check happen
		r = lf.open(pathname);
		if (r) {
			check_point *lcp = lf.cutoff_point;
			if (lcp) {
				lf.log_file_id = chk_file_seq;
				r = lf.check(lcp->log_blk_off, lcp->dir_ent_off);

			} else {
				return MISSING_LAST_CHECK_POINT;
			}
			add_check_snap(lf);

			// continue to check newer inactive log file
			while (r && ++chk_file_seq < log_file_seq) {
				ultoa(chk_file_seq, alphas);
				name = alphas + LOG_FILE_SUFFIX;
				pathname = path + "/" + name;
				if (sio::exist_file(pathname)) {
					r = lf.open(pathname);
					if (r) {
						lf.log_file_id = chk_file_seq;
						r = lf.check();
					}
					if (r) {
						add_check_snap(lf);
					}
				}
			}

			//TODO flush segment to disk;
			if (r) {
				check_point cp;
				auto it = check_points.begin();
				chk_file_seq = it->log_file_seq; //
				cp.ts = it->ts;
				cp.log_blk_off = it->log_blk_off;
				cp.dir_ent_off = it->dir_ent_off;
				checkpoint_file ckf;
				ultoa(chk_file_seq, alphas);
				name = alphas + LOG_FILE_SUFFIX;
				pathname = path + "/" + name;
				r = ckf.open(pathname);
				if (r) {
					r = ckf.write_checkpoint(cp);
				}
			}

		}
	} else if (log_file_seq > 0) {
		if (log_files.size() > 1) {
			// at lease on inactive log file and active log file
			r = lf.open(pathname);
			lf.log_file_id = chk_file_seq;
			if (r) {
				r = lf.check();
			}

			if (r) {
				r = lf.cut_off();
			}
		}
	} else {
		r = NO_LOG_FILE_CHECK;
	}

	delete[] alphas;
	return r;
}

int checkpoint_file::open(const string & pathname) {
	this->pathname = pathname;
	int r = sdb::FAILURE;
	chk_stream.open(pathname.c_str(),
			ios_base::binary | ios_base::in | ios_base::out | ios_base::app);
	r = chk_stream.is_open();
	return r;
}

int checkpoint_file::write_checkpoint(const check_point * cp) {
	int r = sdb::FAILURE;
	char_buffer buff(CHECK_POINT_LENGTH);
	buff << cp->ts << cp->log_blk_off << cp->dir_ent_off;
	chk_stream.write(buff.data(), CHECK_POINT_LENGTH);
	return r = chk_stream.good();
}

int checkpoint_file::write_checkpoint(const check_point & cp) {
	int r = sdb::FAILURE;
	char_buffer buff(CHECK_POINT_LENGTH);
	buff << cp.ts << cp.log_blk_off << cp.dir_ent_off;
	chk_stream.write(buff.data(), CHECK_POINT_LENGTH);
	return r = chk_stream.good();
}

int checkpoint_file::write_checkpoint(const timestamp &ts, const int & lbo,
		const int & deo) {
	int r = sdb::FAILURE;
	char_buffer buff(CHECK_POINT_LENGTH);
	buff << ts << lbo << deo;
	chk_stream.write(buff.data(), CHECK_POINT_LENGTH);
	return r = chk_stream.good();
}

int checkpoint_file::last_checkpoint(check_point & cp) {
	int r = sdb::FAILURE;
	chk_stream.seekg(0, ios_base::end);
	chk_stream.seekg(-CHECK_POINT_LENGTH, ios_base::cur);
	char *b = new char[CHECK_POINT_LENGTH];
	chk_stream.read(b, CHECK_POINT_LENGTH);
	r = chk_stream.good();
	long p = chk_stream.tellg();
	if (r && chk_stream.tellg() > 0) {
		char_buffer buff(b, CHECK_POINT_LENGTH, true);
		buff >> cp.ts >> cp.log_blk_off >> cp.dir_ent_off;
	}
	return r;
}

int checkpoint_file::last_checkpoint(check_point * cp) {
	int r = sdb::FAILURE;
	chk_stream.seekg(0, ios_base::end);
	chk_stream.seekg(-CHECK_POINT_LENGTH, ios_base::cur);
	char *b = new char[CHECK_POINT_LENGTH];
	chk_stream.read(b, CHECK_POINT_LENGTH);
	r = chk_stream.good();
	if (r) {
		char_buffer buff(b, CHECK_POINT_LENGTH, true);
		buff >> cp->ts >> cp->log_blk_off >> cp->dir_ent_off;
	}
	return r;
}

void checkpoint_file::close() {
	chk_stream.close();
}

} /* namespace enginee */
} /* namespace sdb */
