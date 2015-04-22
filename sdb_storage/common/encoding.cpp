/*
 * encoding.cpp
 *
 *  Created on: Dec 12, 2014
 *      Author: linkqian
 */

#include "encoding.h"

char* to_chars(const double &l) {
	char * pv = (char *) &l;
	char * pc = new char[DOUBLE_CHARS];
	for (int i = 0; i < DOUBLE_CHARS; i++) {
		pc[i] = pv[i];
	}
	return pc;
}

char* to_chars(const float &f) {
	char * pv = (char *) &f;
	char * pc = new char[FLOAT_CHARS];
	for (int i = 0; i < FLOAT_CHARS; i++) {
		pc[i] = pv[i];
	}
	return pc;
}

char* to_chars(const int &i) {
	char * pv = (char *) &i;
	char * pc = new char[INT_CHARS];
	for (int j = 0; j < INT_CHARS; j++) {
		pc[j] = pv[j];
	}
	return pc;
}

char* to_chars(const unsigned int &i) {
	char * pv = (char *) &i;
	char * pc = new char[INT_CHARS];
	for (int j = 0; j < INT_CHARS; j++) {
		pc[j] = pv[j];
	}
	return pc;
}

char* to_chars(const short &i) {
	char * pv = (char *) &i;
	char * pc = new char[SHORT_CHARS];
	for (int j = 0; j < SHORT_CHARS; j++) {
		pc[j] = pv[j];
	}
	return pc;
}

char* to_chars(const long &l) {
	char * pv = (char *) &l;
	char * pc = new char[LONG_CHARS];
	for (int j = 0; j < LONG_CHARS; j++) {
		pc[j] = pv[j];
	}
	return pc;
}

char * to_chars(const unsigned long &ul) {
	char * pv = (char *) &ul;
	char * pc = new char[UNSIGNED_LONG_CHARS];
	for (int j = 0; j < UNSIGNED_LONG_CHARS; j++) {
		pc[j] = pv[j];
	}
	return pc;
}
char* to_chars(const bool &b) {
	char c = b ? 1 : 0;
	char * pc = &c;
	return pc;
}

double to_double(const char * p_chars) {
	double * pd = (double *) p_chars;
	double d = (*pd);
	return d;
}

float to_float(const char* p_chars) {
	float * pf = (float *) p_chars;
	float f = *pf;
	return f;
}
int to_int(const char* p_chars) {
	int * pi = (int *) p_chars;
	int i(*pi);
	return i;
}

unsigned int to_unsigned_int(const char* p_chars) {
	unsigned int * pui = (unsigned int *) p_chars;
	unsigned int ui(*pui);
	return ui;
}

short to_short(const char* p_chars) {
	short * pi = (short *) p_chars;
	short i(*pi);
	return i;
}

bool to_bool(const char* p_char) {
	return (*p_char) == 1;
}

long to_long(const char* p_char) {
	long *pl = (long *) p_char;
	long l(*pl);
	return l;
}

unsigned long to_unsigned_long(const char* p_char) {
	unsigned long *pul = (unsigned long *) p_char;
	unsigned long ul(*pul);
	return ul;
}

