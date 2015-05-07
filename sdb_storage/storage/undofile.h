/*
 * undofile.h
 *
 *  Created on: Apr 20, 2015
 *      Author: linkqian
 */

#ifndef STORAGE_UNDOFILE_H_
#define STORAGE_UNDOFILE_H_

namespace sdb {
namespace storage {

class undo_file {
public:
	undo_file();
	virtual ~undo_file();
};

} /* namespace storage */
} /* namespace sdb */

#endif /* STORAGE_UNDOFILE_H_ */
