/*
 * db.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef DB_H_
#define DB_H_

#include <stdlib.h>
#include <string>

namespace sdb {
namespace enginee {

const std::string sdb_meta_file_extension(".sdb");
const std::string sdb_lock_file_extension(".lock");

const int unassign_inner_key = 0;

const int MAX_FIELD_COUNT = 255;
const int MAX_DB_NAME_SIZE(64);

enum field_type {
	unknow_type = 0x09,
	bool_type = 0x10,
	short_type,
	int_type,
	long_type,
	float_type,
	double_type,
	time_type,

	ushort_type,
	uint_type,
	ulong_type,
	ufloat_type,
	udouble_type,

	unsigned_char_type = 0x40,
	unsigned_short_type,
	unsigned_int_type,
	unsigned_long_type,
	unsigned_float_type,
	unsigned_double_type,

	char_type = 0x100,
	varchar_type
};

enum sdb_status {
	sdb_unknown,
	sdb_opening = 1000,
	sdb_opened,
	sdb_ready,
	sdb_in_using,
	sdb_readonly,
	sdb_exited,
	sdb_failured = 2000
};

enum sdb_table_status {
	sdb_table_ready,
	sdb_table_opening,
	sdb_table_opened,
	sdb_table_in_using,
	sdb_table_closed,
	sdb_table_open_failure = 2000,
	sdb_table_checksum_failure,
	sdb_table_other_failure
};


/*################################################
 *
 * define for datafile and segment mgr
 *
 ################################################*/

const int ASSIGN_DATA_FILE_FAILURE = -0x400;
const int ASSIGN_SEGMENT_FAILURE = -0x401;
const int ASSIGN_BLOCK_FAILURE = -0x402;
const int SEGMENG_NOT_EXISTED = -0x403;


/*################################################
 *
 * define for log mgr
 *
 ################################################*/
const int NOT_FIND_TRANSACTION = -1;
const int FIND_TRANSACTION_START = 1;
const int CONTINUE_TO_FIND = 2;

const int LOG_BLK_SPACE_NOT_ENOUGH = -0X500;
const int OUTOF_ENTRY_INDEX = -0X501;
const int DATA_LENGTH_TOO_LARGE = -0X502;
const int INIT_LOG_FILE_FAILURE = -0x503;
const int LOG_STREAM_ERROR = -0X504;
const int LOCKED_LOG_MGR_PATH = -0X505;
const int LOCK_STREAM_ERROR = -0X506;
const int OUT_LOCK_LOGMGR_FAILURE = -0X507;
const int LOG_FILE_IS_ACTIVE = -0X508;
const int INVALID_OPS_ON_ACTIVE_LOG_FILE = -0X509;
const int MISSING_LAST_CHECK_POINT = -0X50A;
const int CHECKPOINT_STREAM_ERROR = -0X50B;
const int EXCEED_LOGFILE_LIMITATION = -0X50C;
const int NO_LOG_FILE_CHECK = -0X50D;

}
}

#endif /* DB_H_ */
