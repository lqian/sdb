/*
 * encoding.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef ENCODING_H_
#define ENCODING_H_

typedef unsigned char uchar;
typedef unsigned short ushort;
typedef unsigned int uint;
typedef unsigned long ulong;

const int UNSIGNED_LONG_CHARS(8);
const int LONG_CHARS(8);
const int DOUBLE_CHARS(8);
const int FLOAT_CHARS(4);
const int INT_CHARS(4);
const int SHORT_CHARS(2);
const int BOOL_CHARS(1);
const int CHAR_LEN(1);

char* to_chars(const double &l);
char* to_chars(const float &f);
char* to_chars(const short &i);
char* to_chars(const int &i);
char* to_chars(const unsigned int &ui);
char* to_chars(const long &l);
char* to_chars(const unsigned long & ul);
char* to_chars(const bool &b);

double to_double(const char * p_chars);
float to_float(const char* p_chars);
short to_short(const char* p_chars);
unsigned short to_ushort(const char* p_chars);
int to_int(const char* p_chars);
unsigned int to_uint(const char* p_chars);
bool to_bool(const char* p_chars);
long to_long(const char* p_chars);
unsigned long to_ulong(const char* p_chars);

#endif /* ENCODING_H_ */
