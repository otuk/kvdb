
/**
   Public header file for kvdb services

 */


#ifndef _KVDB_H
#define _KVDB_H

#include <stdint.h>
#include <stdio.h>

typedef enum {XSMALL=10000, SMALL=100000, MEDIUM=1000000, LARGE=10000000, XLARGE=100000000} dbsize;

typedef struct {
  void* data;
} kvdb_s;


kvdb_s* create_kvdb(const char* name, dbsize size,
		    uint16_t keysize, uint16_t valsize);
kvdb_s* load_kvdb(const char* name);
int     disconnect_kvdb(kvdb_s* db);

int add_kvdb(kvdb_s* db, const char* key, const char* val);
int set_kvdb(kvdb_s* db, const char* key, const char* val);
int del_kvdb(kvdb_s* db, const char* key);
char* get_kvdb(kvdb_s* db, const char* key);

int count_kvdb();
char* get_name_kvdb(kvdb_s* db);


#endif
