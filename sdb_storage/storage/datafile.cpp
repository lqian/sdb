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
namespace sdb {
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

data_file::data_file(const data_file &another) {
	if (another.opened)
		throw "another data file already opened";

	this->id = another.id;
	this->full_path = another.full_path;
	this->free_perct = another.free_perct;
	this->magic = another.magic;
	this->create_time = another.create_time;
	this->closed = another.closed;
	this->last_seg_offset = another.last_seg_offset;
	this->length = another.length;
	this->update_time = another.update_time;
	this->opened = another.opened;
	this->version = another.version;
}

data_file & data_file::operator=(const data_file & another) {
	if (another.opened) {
		throw "another data file already opened";
	}
	this->id = another.id;
	this->full_path = another.full_path;
	this->free_perct = another.free_perct;
	this->magic = another.magic;
	this->create_time = another.create_time;
	this->closed = another.closed;
	this->last_seg_offset = another.last_seg_offset;
	this->length = another.length;
	this->update_time = another.update_time;
	this->opened = another.opened;
	this->version = another.version;
	return *this;
}

bool data_file::operator ==(const data_file & another) {
	return magic == data_file_magic && magic == another.magic
			&& id == another.id && full_path == another.full_path;
}

bool data_file::exist() {
	return sio::exist_file(full_path);
}

int data_file::create() {
//	if (id == valid_data_file_id) {
//		perror("invalid data file id");
//		return FAILURE;
//	}

	time(&attach_time);
	common::char_buffer buff(data_file_head_size);
	fill_head_to_buff(buff);
	ofstream out(full_path.c_str(), ios_base::app | ios_base::binary);
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
		fs.open(full_path.c_str(),
				ios_base::binary | ios_base::in | ios_base::out);
		if (fs.is_open()) {
			char * data = new char[data_file_head_size];
			fs.read(data, data_file_head_size);
			if (fs.good()) {
				common::char_buffer buffer(data, data_file_head_size, true);
				data_file df(this->full_path);
				df.fetch_head_from_buff(buffer);
				if (df.is_valid() && df == *this) {
					buffer.rewind();
					fetch_head_from_buff(buffer);
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
	return magic == data_file_magic && id >= valid_data_file_id && exist();
}

int data_file::seek_segment(const segment & seg) {
	long pos = fs.tellg();
	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	return !fs.fail() ? SUCCESS : FAILURE;
}

bool data_file::has_next_segment(segment & seg) {
	long pos = fs.tellg();
//	fs.seekg(0, ios_base::end);
//	long end = fs.tellg();
	if (pos < length) {
		common::char_buffer buff(segment_head_size);
		fs.read(buff.data(), segment_head_size);
		seg.fetch_head_from_buff(buff);
		seg.d_file = this;
		return seg.is_valid();
	}
	return false;
}

int data_file::assign_segment(segment & seg) {
	fs.seekg(0, ios_base::end);
	unsigned int g = fs.tellg();
	seg.offset = (g - data_file_head_size);

	if (fs.fail()) {
		return FAILURE;
	}
	time(&seg.assign_time);
	common::char_buffer head_buff(segment_head_size);
	seg.fill_head_to_buff(head_buff);
	fs.write(head_buff.data(), segment_head_size);
	seg.assign_content_buffer();
	fs.write(seg.content_buffer, seg.length - segment_head_size);
	fs.flush();
	if (!fs.fail()) {
		length += seg.length;
		free_perct = free_perct
				- ceil((seg.length * 100.0) / data_file_max_size);
		time(&update_time);
		seg.d_file = this;
		return SUCCESS;
	} else {
		return FAILURE;
	}
}

int data_file::assign_segment(segment * seg) {
	fs.seekg(0, ios_base::end);
	unsigned int g = fs.tellg();
	if (fs.fail()) {
		return FAILURE;
	}
	seg->offset = (g - data_file_head_size);
	time(&seg->assign_time);
	common::char_buffer head_buff(segment_head_size);
	seg->fill_head_to_buff(head_buff);
	fs.write(head_buff.data(), segment_head_size);
	seg->assign_content_buffer();
	fs.write(seg->content_buffer, seg->length - segment_head_size);
	fs.flush();
	if (!fs.fail()) {
		length += seg->length;
		free_perct = free_perct
				- ceil((seg->length * 100.0) / data_file_max_size);
		time(&update_time);
		seg->d_file = this;
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
		} else {
			return FAILURE;
		}
	} else {
		return FAILURE;
	}
}

int data_file::flush_segment(segment &seg) {
	int r = SUCCESS;

	if (seg.status == storage::segment_deleted || !seg.is_valid()) {
		return FAILURE;
	}

	long pos = fs.tellg();
	if (pos == -1) {
		return sdb::FAILURE;
	}

	fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
	if (!fs.eof()) {
		common::char_buffer head_buff(segment_head_size);
		seg.fill_head_to_buff(head_buff);
		fs.write(head_buff.data(), segment_head_size);
		if (seg.has_buffer()) {
			fs.write(seg.content_buffer, seg.length - segment_head_size);
		}
		fs.flush();

		if (fs.fail()) {
			r = FAILURE;
		}
	}
	return r;
}

int data_file::flush_segment(segment *seg) {
	int r = SUCCESS;

	if (seg->status == storage::segment_deleted || !seg->is_valid()) {
		return FAILURE;
	}

	long pos = fs.tellg();
	if (pos == -1) {
		return sdb::FAILURE;
	}

	fs.seekg(seg->offset + data_file_head_size - pos, ios_base::cur);
	pos = fs.tellg();
	if (!fs.eof()) {
		common::char_buffer head_buff(segment_head_size);
		seg->fill_head_to_buff(head_buff);
		fs.write(head_buff.data(), data_file_head_size);
		if (seg->has_buffer()) {
			fs.write(seg->content_buffer, seg->length - segment_head_size);
		}
		fs.flush();

		if (fs.fail()) {
			r = FAILURE;
		}
	}
	return r;
}

int data_file::flush_segment(segment &seg, int off, int length) {
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
		if (seg.has_buffer()) {
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

int data_file::write_block(segment &seg, data_block &blk,
		bool update_seg_head) {
	if (blk.offset + seg.block_size > seg.length - segment_head_size) {
		return BLK_OFF_OVERFLOW;
	} else {
		long pos = fs.tellg();
		fs.seekg(seg.offset + data_file_head_size - pos, ios_base::cur);
		if (!fs.fail()) {
			if (update_seg_head) {
				common::char_buffer head_buff(segment_head_size);
				seg.fill_head_to_buff(head_buff);
				fs.write(head_buff.data(), segment_head_size);
			}
			//okay, the stream moves to the content position
			if (!fs.fail()) {
				fs.seekg(blk.offset + (update_seg_head ? 0 : segment_head_size),
						ios_base::cur);
				fs.write((char *) blk.header, block_header_size);
				fs.write(blk.buffer, blk.length);
				fs.flush();
				if (!fs.fail())
					return SUCCESS;
			}
		}
	}
	return FAILURE;
}

int data_file::read_block(segment &seg, data_block &blk) {
	long pos = fs.tellg();
	long off = seg.offset + data_file_head_size + blk.offset + segment_head_size
			- pos;
	fs.seekg(off - pos, ios_base::cur);
	if (!fs.eof()) {
		fs.read(blk.buffer - block_header_size, blk.length + block_header_size);
		if (fs.good()) {
			return SUCCESS;
		}
	}
	return FAILURE;
}

int data_file::read_block(segment *seg, data_block *blk) {
	long pos = fs.tellg();
	long off = seg->offset + data_file_head_size + blk->offset
			+ segment_head_size - pos;
	fs.seekg(off - pos, ios_base::cur);
	if (!fs.eof()) {
		fs.read(blk->buffer - block_header_size,
				blk->length + block_header_size);
		if (fs.good()) {
			return SUCCESS;
		}
	}
	return FAILURE;
}

void data_file::skip_segment(segment & seg) {
	if (seg.is_valid()) {
		fs.seekg(seg.length - segment_head_size, ios_base::cur);
	}
}

void data_file::remove() {
	sio::remove_file(full_path);
}

void data_file::fill_head_to_buff(char_buffer & buff) {
	// THE ORDER MUST NOT CHANGE
	buff << data_file_magic << id << length << create_time << attach_time
			<< update_time << version << free_perct;
}

void data_file::fetch_head_from_buff(char_buffer & buff) {
	buff >> magic >> id >> length >> create_time >> attach_time >> update_time
			>> version >> free_perct;
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
			if (!fs.fail()) {
				r = SUCCESS;
				seg.d_file = this;
			}
		}
	}
	return r;
}

int data_file::next_segment(segment * seg) {
	int r = FAILURE;
	long p = fs.tellg();
	if (fs.good() && !fs.eof()) {
		if (seg->is_valid()) {
			seg->assign_content_buffer();
			fs.read(seg->content_buffer, seg->length - segment_head_size);
			if (!fs.fail()) {
				r = SUCCESS;
			}
		}
	}
	return r;
}

int data_file::fetch_segment(segment * seg) {
	int r = FAILURE;
	long pos = fs.tellg();
	fs.seekg(seg->offset + data_file_head_size - pos, ios_base::cur);
	if (fs.good() && !fs.eof()) {
		char_buffer head_buff(segment_head_size);
		fs.read(head_buff.data(), segment_head_size);
		seg->fetch_head_from_buff(head_buff);

		if (seg->is_valid()) {
			seg->assign_content_buffer();
			fs.read(seg->content_buffer, seg->length - segment_head_size);
			if (!fs.fail()) {
				r = SUCCESS;
				seg->d_file = this;
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
			fs.write(head_buff.data(), head_buff.size());
			fs.flush();
			closed = true;
			opened = false;
			if (!fs.fail()) {
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
	d_file = another.d_file;
	id = another.id;
	status = another.status;
	length = another.length;
	seg_type = another.seg_type;
	offset = another.offset;
	next_seg_offset = another.next_seg_offset;
	pre_seg_offset = another.pre_seg_offset;
	assign_time = another.assign_time;
	update_time = another.update_time;
	create_time = another.create_time;
	block_size = another.block_size;
	block_count = another.block_count;
	pre_seg_dfile_id = another.pre_seg_dfile_id;
	pre_seg_dfile_path = another.pre_seg_dfile_path;
	next_seg_dfile_id = another.next_seg_dfile_id;
	next_seg_dfile_path = another.next_seg_dfile_path;

	content_buffer = another.content_buffer;
}

segment::segment(segment && another) {
	d_file = another.d_file;
	magic = another.magic;
	id = another.id;
	status = another.status;
	length = another.length;
	seg_type = another.seg_type;
	offset = another.offset;
	next_seg_offset = another.next_seg_offset;
	pre_seg_offset = another.pre_seg_offset;

	assign_time = another.assign_time;
	create_time = another.create_time;
	update_time = another.update_time;

	block_size = another.block_size;
	block_count = another.block_count;

	pre_seg_dfile_id = another.pre_seg_dfile_id;
	pre_seg_dfile_path = another.pre_seg_dfile_path;
	next_seg_dfile_id = another.next_seg_dfile_id;
	next_seg_dfile_path = another.next_seg_dfile_path;

	//move another content buffer to this
	content_buffer = another.content_buffer;
	another.content_buffer = nullptr;
}

segment::~segment() {
	if (content_buffer != nullptr) {
		delete[] content_buffer;
	}
}

void segment::fill_to_buff(char_buffer & buff) {
	//TODO fill segment's content
}

void segment::fill_head_to_buff(char_buffer &buff) {
	buff << magic << id << offset << status << length << seg_type << create_time
			<< assign_time << update_time << block_size << block_count
			<< pre_seg_offset << pre_seg_id << next_seg_offset << next_seg_id
			<< span_dfile_flag;
	if ((span_dfile_flag & PRE_SEG_SPAN_DFILE_BIT) == 1) {
		buff << pre_seg_dfile_id;
	}
	if ((span_dfile_flag & NEXT_SEG_SPAN_DFILE_BIT) == 2) {
		buff << next_seg_dfile_id ;
	}
}

bool segment::operator ==(const segment & other) {
	return other.id == id && magic == other.magic && other.status == status
			&& seg_type == other.seg_type && create_time == other.create_time
			&& length == other.length;
}

bool segment::operator !=(const segment & other) {
	return other.id != id || magic != other.magic || other.status != status
			|| seg_type != other.seg_type || create_time != other.create_time
			|| length != other.length;
}

segment & segment::operator=(const segment & another) {
	if (&another != this) {
		magic = another.magic;
		d_file = another.d_file;
		id = another.id;
		status = another.status;
		length = another.length;
		seg_type = another.seg_type;
		offset = another.offset;
		next_seg_offset = another.next_seg_offset;
		pre_seg_offset = another.pre_seg_offset;
		assign_time = another.assign_time;
		update_time = another.update_time;
		create_time = another.create_time;
		block_size = another.block_size;
		block_count = another.block_count;
		pre_seg_dfile_id = another.pre_seg_dfile_id;
		pre_seg_dfile_path = another.pre_seg_dfile_path;
		next_seg_dfile_id = another.next_seg_dfile_id;
		next_seg_dfile_path = another.next_seg_dfile_path;

		if (another.has_buffer()) {
			assign_content_buffer();
			memcpy(content_buffer, another.content_buffer,
					length - segment_head_size);
		}
	}

	return *this;
}

void segment::fetch_head_from_buff(common::char_buffer & buff) {
	buff >> magic >> id >> offset >> status >> length >> seg_type >> create_time
				>> assign_time >> update_time >> block_size >> block_count
				>> pre_seg_offset >> pre_seg_id >> next_seg_offset >> next_seg_id
				>> span_dfile_flag;

	if ((span_dfile_flag & PRE_SEG_SPAN_DFILE_BIT) == 1) {
		buff >> pre_seg_dfile_id;
	}
	if ((span_dfile_flag & PRE_SEG_SPAN_DFILE_BIT ) == 2) {
		buff >> next_seg_dfile_id;
	}
}

int segment::flush() {
	if (d_file && d_file->is_open()) {
		return d_file->flush_segment(this);
	}
	return SEGMENT_NOT_ASSIGNED;
}

int segment::assign_block(data_block &blk) {
	int r = length - segment_head_size - block_size * block_count;
	if (r > block_size) {
		int off = block_size * block_count;
		if (has_buffer()) {
			blk.ref(off, content_buffer + off, block_size - block_header_size);
			blk.init_header();
			time(&blk.header->assign_time);
		} else {
			blk.init(off, block_size - block_header_size);
		}
		block_count++;
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int segment::assign_block(data_block * blk) {
	int r = length - segment_head_size - block_size * block_count;
	if (r > block_size) {
		int off = block_size * block_count;
		if (has_buffer()) {
			blk->ref(off, content_buffer + off, block_size - block_header_size);
			blk->init_header();
			time(&blk->header->assign_time);
		} else {
			blk->init(off, block_size - block_header_size);
		}
		block_count++;
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int segment::remove_block(data_block & blk) {
	blk.set_flag(REMOVED_BLK_BIT);
	if (blk.ref_flag) {
		char * p = (char *) &blk.header;
		memcpy(content_buffer + blk.offset, p, block_header_size);
		return sdb::SUCCESS;
	} else {
		return flush_block(blk);
	}
}

int segment::flush_block(data_block &blk) {
	return d_file->write_block(*this, blk);
}

int segment::read_block(data_block &blk) {
	if (blk.offset + block_size > length) {
		return BLK_OFF_OVERFLOW;
	} else if (blk.offset % block_size != 0) {
		return BLK_OFF_INVALID;
	} else if (has_buffer()) {
		blk.ref(blk.offset, content_buffer + blk.offset,
				block_size - block_header_size);
		return SUCCESS;
	} else {
		return d_file->read_block(*this, blk);
	}
}

int segment::read_block(data_block * blk) {
	if (blk->offset + block_size > length) {
		return BLK_OFF_OVERFLOW;
	} else if (has_buffer()) {
		blk->ref(blk->offset, content_buffer + blk->offset,
				block_size - block_header_size);
		return SUCCESS;
	} else {
		return d_file->read_block(this, blk);
	}
}

} /* namespace storage */
} /*  namespace sdb */
