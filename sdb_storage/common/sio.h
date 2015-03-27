/*
 * io.h
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#ifndef IO_H_
#define IO_H_

#include <string>

namespace sdb_io {

bool exist_file(char * path, int mode);

bool exist_file(std::string & path, int mode);

bool exist_file(char * path);

bool exist_file(std::string & path);
}
;

#endif /* IO_H_ */
