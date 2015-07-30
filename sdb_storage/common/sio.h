/*
 * io.h
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#ifndef IO_H_
#define IO_H_

#include <string>

namespace sio {

bool exist_file(const char * path, int mode);

bool exist_file(const std::string & path, int mode);

bool exist_file(const char * path);

bool exist_file(const std::string & path);

bool remove_file(const std::string &path);
}
;

#endif /* IO_H_ */
