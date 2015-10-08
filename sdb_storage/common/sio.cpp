/*
 * io.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include "sio.h"
#include "../sdb_def.h"

namespace sio {
bool exist_file(const char *pathname, int mode) {
	return access(pathname, mode) == 0;
}

bool exist_file(const std::string & pathname, int mode) {
	return access(pathname.c_str(), mode);
}

bool exist_file(const char *pathname) {
	return access(pathname, F_OK) == 0;
}

bool exist_file(const std::string & pathname) {
	return access(pathname.c_str(), R_OK) == 0;
}

bool remove_file(const std::string & path) {
	return unlink(path.c_str()) == 0;
}

int list_file(const std::string &pathname, std::list<std::string> & files,
		unsigned char filter) {
	DIR* dp;
	struct dirent* ep;
	dp = opendir(pathname.data());
	if (dp != NULL) {
		while (ep = readdir(dp)) {
			if (ep->d_type == filter) {
				files.push_back(std::string(ep->d_name, ep->d_reclen));
			}
		}
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}

list<string> list_file(const string & pathname, const string & suffix,
		unsigned char filter) {
	list<string> files;
	DIR* dp;
	struct dirent* ep;
	dp = opendir(pathname.data());
	int sl = suffix.length();
	if (dp != NULL) {
		while (ep = readdir(dp)) {
			if (ep->d_type == filter) {
				string fn(ep->d_name, ep->d_reclen);
				size_t found = fn.rfind(suffix);
				if (found != string::npos) {
					int fl = fn.length();
					if (fl >= sl && found == fl - sl) {
						files.push_back(fn);
					}
				}
			}
		}
	}
	return files;
}

int make_dir(const std::string &pathname, int t_mode) {
	int status = 0;
	std::string tmp = pathname;
	int found = 0, pos = 1;
	if (tmp.find('/') == 0) {
		while ((found = tmp.find('/', pos)) != std::string::npos) {
			status = mkdir(tmp.substr(0, found).c_str(), t_mode);
			pos = found + 1;
			if (status != 0 && errno == EEXIST) {
				continue;
			} else {
				return sdb::FAILURE;
			}
		}

		if (pathname.find_last_of('/') != pathname.length() - 1) {
			status = mkdir(tmp.c_str(), t_mode);
			if (status != 0 && errno != EEXIST) {
				return sdb::FAILURE;
			}
		}
		return sdb::SUCCESS;
	} else {
		return sdb::FAILURE;
	}
}
}

