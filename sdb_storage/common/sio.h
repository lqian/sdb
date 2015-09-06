/*
 * io.h
 *
 *  Created on: Dec 15, 2014
 *      Author: linkqian
 */

#ifndef IO_H_
#define IO_H_

#include <string>
#include <list>
#include <dirent.h>
#include <sys/stat.h>

namespace sio {

bool exist_file(const char * path, int mode);

bool exist_file(const std::string & path, int mode);

bool exist_file(const char * path);

bool exist_file(const std::string & path);

bool remove_file(const std::string &path);

int list_file(const std::string &pathname, std::list<std::string> & files, unsigned char filter = DT_REG);

int make_dir(const std::string &pathname, int t_mode = S_IRWXU);
}
;

#endif /* IO_H_ */
