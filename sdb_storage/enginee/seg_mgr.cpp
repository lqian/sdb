/*
 * seg_mgr.cpp
 *
 *  Created on: Aug 5, 2015
 *      Author: lqian
 */

#include "seg_mgr.h"

#include <dirent.h>
#include "../common/sio.h"
#include "../common/encoding.h"

namespace sdb {
namespace enginee {

int seg_mgr::load() {
	// check directory existed
	for (auto & path : data_file_paths) {
		if (!sio::exist_file(path)) {
			if (!sio::make_dir(path)) {
				return sdb::FAILURE;
			}
		}
	}

	//open all data files, TODO segment replacement policy in future

	int r = sdb::SUCCESS;
	for (auto & path : data_file_paths) {
		r = do_load(path);
	}

	return r;
}

void seg_mgr::flush(data_file *pdf) {
	segment seg;
	mtx.lock();
	while (pdf->has_next_segment(seg)) {
		auto it = seg_map.find(seg.get_id());
		if (it != seg_map.end()) {
			it->second->flush();  // flush segment
			seg_map.erase(it);
			delete it->second;  //free segment object
		}
	}
	df_map.erase(pdf->get_id());
	mtx.unlock();
}

void seg_mgr::flush_all_segment() {
	for (auto it = seg_map.begin(); it != seg_map.end(); ++it) {
		it->second->flush();
	}
}

void seg_mgr::close() {
	flush_all_segment();

	//close all data_file
	for (auto it = df_map.begin(); it != df_map.end(); ++it) {
		it->second->close();
	}
}

void seg_mgr::clean() {
	df_map.clear();
	seg_map.clear();
	data_file_paths.clear();
	path_df_map.clear();
}

segment * seg_mgr::find_segment(ulong id) {
	auto it = seg_map.find(id);
	return it != seg_map.end() ? it->second : nullptr;
}

int seg_mgr::get_row(ulong seg_id, uint blk_off, int idx, char_buffer & buff) {
	int r = sdb::FAILURE;
	segment * seg = find_segment(seg_id);
	if (seg != nullptr) {
		mem_data_block blk;
		blk.offset = blk_off;
		r = seg->read_block(blk);
		if (r) {
			r = blk.get_row_by_idx(idx, buff);
		}
	}
	return r;
}

int seg_mgr::get_row(const data_item_ref * dif, char_buffer & buff) {
	int r = sdb::FAILURE;
	segment * seg = find_segment(dif->seg_id);
	if (seg) {
		mem_data_block blk;
		blk.offset = dif->blk_off;
		r = seg->read_block(blk);
		if (r) {
			r = blk.get_row_by_idx(dif->row_idx, buff);
		}
	}
	return r;
}

int seg_mgr::get_row(const data_item_ref & dif, char_buffer & buff) {
	return get_row(&dif, buff);
}

int seg_mgr::write(ulong seg_id, uint blk_off, int row_idx, const char * buff,
		const int & len) {
	int r = sdb::FAILURE;
	segment * seg = find_segment(seg_id);
	if (seg != nullptr) {
		mem_data_block blk;
		blk.offset = blk_off;
		r = seg->read_block(blk);
		if (r) {
			r = blk.update_row_by_index(row_idx, buff, len);
		}
	}

	return r;
}

int seg_mgr::write(const data_item_ref *dif, const char * buff,
		const int & len) {
	int r = sdb::FAILURE;
	segment * seg = find_segment(dif->seg_id);
	if (seg != nullptr) {
		short st = seg->get_seg_type();
		if (st == segment_type::data_segment_type) {
			mem_data_block blk;
			blk.offset = dif->blk_off;
			r = seg->read_block(blk);
			if (r) {
				r = blk.update_row_by_index(dif->row_idx, buff, len);
			}
		}
		else if (st == segment_type::index_segment_type) {
			//update index page
		}
	}

	return r;
}

int seg_mgr::do_load(const string & pathname) {
	list<string> files;
	string fullname;
	ulong mid = 0;
	data_file * mdf = nullptr;
	int r = sio::list_file(pathname, files);
	if (r) {
		for (auto it = files.begin(); it != files.end(); it++) {
			fullname = pathname;
			// the file name is data file id
			ulong id = strtoul(it->data(), nullptr, 16);
			fullname += "/";
			fullname += it->data();
			data_file *pdf = new data_file(id, fullname);

			auto ret = df_map.insert(std::make_pair(id, pdf));
			if (ret.second) {
				if (ret.first->second->open()) {
					do_load_seg(pdf);  // load segment for the data file
					if (id >= mid) {
						mid = id;
						mdf = ret.first->second;
					}
				} else {
					throw "open data file failure:" + fullname;
				}
			} else {
				return sdb::FAILURE;
			}

			// the last data file with max id
			if (mdf != nullptr && mid > local_data_file_id) {
				local_data_file_id = mid;
			}
		}

		path_df_map.erase(pathname);
		path_df_map.insert(std::make_pair(pathname, mdf));
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

int seg_mgr::do_load_seg(data_file * df) {
	segment tmp;
	while (df->has_next_segment(tmp)) {
		segment * seg = new segment(tmp);
		if (df->next_segment(seg) == sdb::SUCCESS) {
			seg_map.insert(std::make_pair(tmp.get_id(), seg));
			if (tmp.get_id() > this->local_segment_id) {
				this->local_segment_id = tmp.get_id();
			}
		} else {
			return sdb::FAILURE;
		}
	}

	return sdb::SUCCESS;
}

int seg_mgr::assign_data_file(data_file * pdf) {
	static int path_round = 0;

	int r = sdb::FAILURE;
	mtx.lock();

	unsigned long nid = ++local_data_file_id;
	char *fn = new char[16];
	ultoa(nid, fn);

	auto it = path_df_map.begin();

	string pathname;
	if (it == path_df_map.end()) {
		pathname = *data_file_paths.begin();
	} else {
		for (int i = 0; i < path_round; i++, it++) {
		}
		pathname = it->first;
	}

	if (pdf == nullptr) {
		pdf = new data_file;
	}
	pdf->set_id(nid);
	pdf->set_path(pathname + "/" + fn);
	delete[] fn;

	auto mit = df_map.insert(std::make_pair(nid, pdf));
	if (mit.second) {
		if (pdf->open()) {
			path_df_map.erase(pathname);
			path_df_map.insert(std::make_pair(pathname, pdf));

			//swith to next path for next round
			path_round++;
			path_round %= path_df_map.size();
			r = sdb::SUCCESS;
		} else {
			df_map.erase(nid);
		}
	} else {
		pdf = nullptr;
	}

	mtx.unlock();
	return r;
}

int seg_mgr::assign_data_file(data_file * pdf,
		map<string, data_file *>::iterator it) {
	int r = sdb::FAILURE;

	unsigned long nid = ++local_data_file_id;
	char *fn = new char[16];
	ultoa(nid, fn);

	string pathname = it->first;
	pdf->set_id(nid);
	pdf->set_path(pathname + "/" + fn);

	auto mit = df_map.insert(std::make_pair(nid, pdf));
	if (mit.second) {
		if (pdf->open()) {
			it->second = pdf;
			r = sdb::SUCCESS;
		} else {
			pdf = nullptr;
		}
	}

	return r;
}

/*
 *
 */
int seg_mgr::assign_segment(segment * & seg) {
	int r = sdb::FAILURE;
	static int round = 0;
	mtx.lock();

	if (seg == nullptr) {
		seg = new segment();
	}
	seg->set_id(++local_segment_id);

	if (seg->no_length()) {
		seg->set_length(seg_default_length);
	}

	int ms = path_df_map.size();
	auto it = path_df_map.begin();
	for (int i = 0; i < round % ms; i++, it++)
		;

	data_file *pdf = it->second;
	if (pdf == nullptr) {
		data_file * pdf = new data_file;
		if ((r = assign_data_file(pdf, it))) {
			r = pdf->assign_segment(seg);
		}
	} else {
		if (pdf->has_free_space()) {
			r = pdf->assign_segment(seg);
		} else {
			data_file * pdf = new data_file;
			if ((r = assign_data_file(pdf, it))) {
				r = pdf->assign_segment(seg);
			}
		}
	}
	round++;

	// try assign segment in others directory
	for (int i = 0; i < ms && r != sdb::SUCCESS; i++) {
		if (it == path_df_map.end()) {
			it = path_df_map.begin();
		} else {
			it++;
		}

		data_file *pdf = it->second;
		if (pdf == nullptr) {
			data_file * pdf = new data_file;
			if ((r = assign_data_file(pdf, it))) {
				r = pdf->assign_segment(seg);
			}
		} else {
			if (pdf->has_free_space()) {
				r = pdf->assign_segment(seg);
			} else {
				data_file * pdf = new data_file;
				if ((r = assign_data_file(pdf, it))) {
					r = assign_segment(seg);
				}
			}
		}
		round++;
	}

	if (r == sdb::SUCCESS && seg->has_assigned()) {
		seg_map.insert(std::make_pair(seg->get_id(), seg));
	}

	round %= ms;

	mtx.unlock();
	return r;
}

data_file * seg_mgr::find_data_file(ulong id) {
	auto it = df_map.find(id);
	return it != df_map.end() ? it->second : nullptr;
}

void seg_mgr::set_datafile_paths(const string & paths) {
	data_file_paths.clear();
	add_datafile_paths(paths);
}

void seg_mgr::add_datafile_paths(const string & paths) {
	std::size_t found = 0, pos = 0;
	while ((found = paths.find(',', pos)) != std::string::npos) {
		if (pos != found) {
			data_file_paths.push_back(paths.substr(pos, found));
		}
		pos = found + 1;
	}

	data_file_paths.push_back(paths.substr(pos, paths.length()));
}

void seg_mgr::remove_files() {
	auto it = data_file_paths.begin();
	for (; it != data_file_paths.end(); ++it) {
		list<string> files;
		string path = *it;
		sio::list_file(path, files);
		for (auto fi = files.begin(); fi != files.end(); ++fi) {
			sio::remove_file(path + "/" + *fi);
		}
	}
}
seg_mgr::seg_mgr() {
}

void seg_mgr::set_seg_default_length(int length) {
	seg_default_length = length;
}

seg_mgr::~seg_mgr() {
	for (auto it = df_map.begin(); it != df_map.end(); ++it) {
		delete it->second;
	}
}

} /* namespace enginee */
} /* namespace sdb */
