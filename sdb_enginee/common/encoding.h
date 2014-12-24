/*
 * encoding.h
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#ifndef ENCODING_H_
#define ENCODING_H_

 const int LONG_CHARS(8) ;
 const int DOUBLE_CHARS(8) ;
 const int FLOAT_CHARS (4);
 const int INT_CHARS (4);
 const int BOOL_CHARS(1);

char* to_chars(const double &l) ;
char* to_chars(const float &f) ;
char* to_chars(const int &i) ;
char* to_chars(const long &l) ;
char* to_chars(const bool &b);


double to_double(const char * p_chars) ;
float to_float(const char* p_chars) ;
int to_int(const char* p_chars) ;
bool to_bool(const char* p_chars);
long to_long(const char* p_chars);


#endif /* ENCODING_H_ */
