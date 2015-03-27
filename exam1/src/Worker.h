/*
 * Worker.h
 *
 *  Created on: Mar 24, 2015
 *      Author: linkqian
 */

#ifndef WORKER_H_
#define WORKER_H_

namespace sdb {
namespace common {

class Worker {
public:
	void execute();

	Worker();
	virtual ~Worker();
};

} /* namespace common */
} /* namespace sdb */

#endif /* WORKER_H_ */
