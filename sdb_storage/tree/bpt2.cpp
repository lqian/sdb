/*
 * bpt2.cpp
 *
 *  Created on: Oct 26, 2015
 *      Author: lqian
 */

#include "bpt2.h"

namespace sdb {
namespace tree {

int fsn_lpage::assign_lnode(_lnode &ln) {
	int r = header->node_count;
	int ns = header->node_size;
	int off = header->node_count * ns;
	if (off + ns <= length) {
		ln.ref(buffer + off, ns);
		header->node_count++;
	} else {
		r = sdb::FAILURE;
	}
	return r;
}

bpt2::bpt2() {
	// TODO Auto-generated constructor stub

}

bpt2::~bpt2() {
	// TODO Auto-generated destructor stub
}

} /* namespace tree */
} /* namespace sdb */
