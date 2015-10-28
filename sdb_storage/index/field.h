/*
 * field.h
 *
 *  Created on: Oct 28, 2015
 *      Author: lqian
 */

#ifndef FIELD_H_
#define FIELD_H_

#include "../enginee/sdb.h"

namespace sdb {
namespace index {
using namespace sdb::enginee;

struct bool_key_field: _key_field {
	inline bool_key_field() {
		type = bool_type;
		field_len = 1;
	}
};

struct short_key_field: _key_field {
	inline short_key_field() {
		type = short_type;
		field_len = 2;
	}
};

struct ushort_key_field: _key_field {
	inline ushort_key_field() {
		type = ushort_type;
		field_len = 2;
	}
};

struct int_key_field: _key_field {
	inline int_key_field() {
		type = int_type;
		field_len = 4;
	}
};

struct uint_key_field: _key_field {
	inline uint_key_field() {
		type = uint_type;
		field_len = 4;
	}
};

struct long_key_field: _key_field {
	inline long_key_field() {
		type = long_type;
		field_len = 8;
	}
};

struct ulong_key_field: _key_field {
	inline ulong_key_field() {
		type = ulong_type;
		field_len = 8;
	}
};

struct float_key_field: _key_field {
	inline float_key_field() {
		type = float_type;
		field_len = 4;
	}
};

struct double_key_field: _key_field {
	inline double_key_field() {
		type = double_type;
		field_len = 8;
	}
};

struct varchar_key_field: _key_field {
	inline varchar_key_field() {
		type = varchar_type;
	}

	virtual ~varchar_key_field() {
	}
};
}
}

#endif /* FIELD_H_ */
