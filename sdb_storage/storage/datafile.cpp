/*
 * datafile.cpp
 *
 *  Created on: Apr 20, 2015
 *      Author: linkqian
 */

#include <iostream>
#include <string>
#include <cmath>
#include "datafile.h"
#include "../common/sio.h"
#include "../common/char_buffer.h"
#include <fstream>

namespace storage {

data_file::data_file(std::string full_path) {
	this->full_path = full_path;
	this->opened = false;
}

data_file::data_file(unsigned long id, std::string full_path) {
	this->id = id;
	this->full_path = full_path;
	this->free_perct = 99;
	time(&create_time);
	this->version = data_file_version;
	this->opened = false;
}

bool data_file::operator ==(const data_file & another) {
	return magic == data_file_magic && magic == another.magic
			&& id == another.id && full_path == another.full_path;
}

bool data_file::exist() {
	return sio::exist_file(full_path);
}

int data_file::create() {
	if (id == unused_id) {
		perror("invalid data file id");
		return FAILURE;
	}

	time(&attach_time);
	common::char_buffer buff(data_file_head_size);
	fill_head_to_buff(buff);
	ofstream out(full_path, ios_base::app | ios_base::binary);
	out.write(buff.data(), buff.capacity());
	out.flush();
	out.close();
	return out.good() ? SUCCESS : FAILURE;
}

int data_file::open() {
	int r = FAILURE;
	if (!opened) {
		if (!exist() && create() != SUCCESS) {
			return FAILURE;
		}
		fs.open(full_path, ios_base::binary | ios_base::in | ios_base::out);
		if (fs.is_open()) {
			char * data = new char[data_file_head_size];
			fs.read(data, data_file_head_size);
			if (fs.good()) {
				common::char_buffer buffer(data, data_file_head_size, true);
				data_file df(this->full_path);
				df.fetch_head_from_buff(buffer);
				if (df.is_valid() && df == *this) {
					r = SUCCESS;
					opened = true;
				} else {
					perror("invalid data file magic or data file id");
				}
			}
		}
	} else {
		r = SUCCESS;
	}
	return r;
}

bool data_file::is_valid() {
	return magic == data_file_magic && id > unused_id && exist();
}

int data_file::seek_segment(const segment & seg) {
	long pos = fs.tellg();
	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	return fs.good() && !fs.eof() ? SUCCESS : FAILURE;
}

bool data_file::has_next_segment(segment & seg) {
	long pos = fs.tellg();
	if (fs.fail() || fs.eof()) {
		return false;
	} else {
		while (!fs.eof()) {
			common::char_buffer buff(segment_head_size);
			fs.read(buff.data(), segment_head_size);
			seg.fetch_head_from_buff(buff);
			return seg.is_valid();
		}
	}
}

int data_file::assgin_segment(segment & seg) {
	fs.seekg(0, ios_base::end);
	unsigned int g = fs.tellg();
	seg.offset = (g - data_file_head_size);

	if (fs.fail()) {
		return FAILURE;
	}
	time(&seg.assign_time);
	common::char_buffer head_buff(segment_head_size);
	seg.fill_head_to_buff(head_buff);
	fs.write(head_buff.data(), head_buff.capacity());
	char * seg_body = new char[seg.length - segment_head_size];
	fs.write(seg_body, seg.length);
	fs.flush();
	delete[] seg_body;
	if (fs.good()) {
		free_perct = free_perct
				- ceil((seg.length * 100.0) / data_file_max_size);
		time(&update_time);
		seg.d_file = this;
		return SUCCESS;
	} else {
		return FAILURE;
	}
}

int data_file::delete_segment(segment & seg) {

	if (seg.status == storage::segment_deleted) {
		return FAILURE;
	}

	if (seek_segment(seg) == SUCCESS) {
		common::char_buffer buff(segment_head_size);
		fs.read(buff.data(), segment_head_size);
		if (fs.good()) {
			segment disk_seg;
			disk_seg.fetch_head_from_buff(buff);
			if (disk_seg.is_valid() && disk_seg == seg
					&& disk_seg.status == storage::segment_alive) {
				seg.status = storage::segment_deleted;
				buff.rewind();
				seg.fill_head_to_buff(buff);

				fs.seekg(-segment_head_size, ios_base::cur);
				fs.write(buff.data(), buff.capacity());
				fs.flush();
				return fs.good() ? SUCCESS : FAILURE;
			}
		}
	} else {
		return FAILURE;
	}
}

int data_file::update_segment(segment &seg) {
	int r = SUCCESS;

	if (seg.status == storage::segment_deleted || !seg.is_valid()) {
		return FAILURE;
	}

	long pos = fs.tellg();
	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	if (!fs.eof()) {
		common::char_buffer head_buff(segment_head_size);
		seg.fill_head_to_buff(head_buff);
		fs.write(head_buff.data(), segment_head_size);
		if (seg.has_buffer) {
			fs.write(seg.content_buffer, seg.length - segment_head_size);
		}
		fs.flush();

		if (fs.fail()) {
			r = FAILURE;
		}
	}
	return r;
}

int data_file::update_segment(segment &seg, int off, int length) {
	int r = SUCCESS;

	if (seg.status == storage::segment_deleted || !seg.is_valid()) {
		return FAILURE;
	}

	long pos = fs.tellg();
	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	if (!fs.eof()) {
		common::char_buffer head_buff(segment_head_size);
		seg.fill_head_to_buff(head_buff);
		fs.write(head_buff.data(), segment_head_size);
		if (seg.has_buffer) {
			fs.seekg(off, ios_base::cur);
			fs.write(seg.content_buffer + off, length);
		}
		fs.flush();

		if (fs.fail()) {
			r = FAILURE;
		}
	}
	return r;
}

void data_file::skip_segment(segment & seg) {
	if (seg.is_valid()) {
		fs.seekg(seg.length - segment_head_size, ios_base::cur);
	}
}

void data_file::remove() {
	sio::remove_file(full_path);
}

void data_file::fill_head_to_buff(common::char_buffer & buff) {
	// THE ORDER MUST NOT CHANGE
	buff << data_file_magic << id << create_time << attach_time << update_time
			<< version << free_perct;
}

void data_file::fetch_head_from_buff(char_buffer & buff) {
	buff >> magic >> id >> create_time >> attach_time >> update_time >> version
			>> free_perct;
}

int data_file::fetch_segment(segment & seg) {
	int r = FAILURE;
	long pos = fs.tellg();
	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	if (fs.good() && !fs.eof()) {
		char_buffer head_buff(segment_head_size);
		fs.read(head_buff.data(), segment_head_size);
		segment s1;
		s1.fetch_head_from_buff(head_buff);

		if (s1.is_valid() && s1 == seg) {
			seg.assign_content_buffer();
			fs.read(seg.content_buffer, seg.length - segment_head_size);
			if (fs.good() && !fs.eof()) {
				r = SUCCESS;
			}
		}
	}
	return r;
}

int data_file::close() {
	if (!closed) {
		int r = FAILURE;
		if (opened) {
			common::char_buffer head_buff(data_file_head_size);
			fill_head_to_buff(head_buff);
			fs.seekp(ios_base::beg);
			fs.write(head_buff.data(), head_buff.capacity());
			fs.flush();
			closed = true;
			if (fs.good()) {
				r = SUCCESS;
				fs.close();
			}
		}

		return r;
	} else {
		return SUCCESS;
	}
}
data_file::~data_file() {
	if (!closed) {
		close();
	}
}

segment::segment() {
	time(&create_time);
	update_time = create_time;
	block_size = K_4 * kilo_byte;
}
segment::segment(unsigned long int _id) :
		id(_id) {
	time(&create_time);
	update_time = create_time;
	block_size = K_4 * kilo_byte;
}

segment::segment(unsigned long int _id, data_file *_f, unsigned int _off) :
		id(_id), offset(_off) {
	time(&create_time);
	update_time = create_time;
	this->d_file = _f;
	block_size = K_4 * kilo_byte;
}

segment::segment(const segment & another) {
	magic = another.magic;
	id = another.id;
	status = another.status;
	length = another.length;
	offset = another.offset;
	next_seg_offset = another.next_seg_offset;
	pre_seg_offset = another.pre_seg_offset;
	assign_time = another.assign_time;
	update_time = another.update_time;
	create_time = another.create_time;
	free_perct = another.free_perct;
	has_buffer = another.has_buffer;
	block_size = another.block_size;

	if (has_buffer) {
		assign_content_buffer();
		memcpy(content_buffer, another.content_buffer,
				length - segment_head_size);
	}
}

segment::segment(segment && another) {
	magic = another.magic;
	id = another.id;
	status = another.status;
	length = another.length;
	offset = another.offset;
	next_seg_offset = another.next_seg_offset;
	pre_seg_offset = another.pre_seg_offset;
	assign_time = another.assign_time;
	create_time = another.create_time;
	update_time = another.update_time;
	free_perct = another.free_perct;
	has_buffer = another.has_buffer;
	content_buffer = another.content_buffer;
	block_size = another.block_size;

	//move another content buffer to this
	another.content_buffer = NULL;
	another.has_buffer = false;
}

segment::~segment() {
	if (has_buffer) {
		delete[] content_buffer;
	}
}

void segment::fill_to_buff(char_buffer & buff) {
	//TODO fill segment's content
}

void segment::fill_head_to_buff(char_buffer &buff) {
	buff << magic << id << offset << status << length << create_time
			<< assign_time << update_time << pre_seg_offset << next_seg_offset
			<< block_size;
}

bool segment::operator ==(const segment & other) {
	return other.id == id && other.status == status;
}

void segment::fetch_head_from_buff(common::char_buffer & buff) {
	buff >> magic >> id >> offset >> status >> length >> create_time
			>> assign_time >> update_time >> pre_seg_offset >> next_seg_offset
			>> block_size;
}

} /* namespace storage */
