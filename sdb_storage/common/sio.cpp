/*
 * io.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#include <unistd.h>
#include <stdio.h>
#include "sio.h"

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
}

