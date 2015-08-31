/*
 * io.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
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
}

