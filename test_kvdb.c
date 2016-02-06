
#include "kvdb.h"
#include <stdlib.h>  // exit
#include <string.h> //memset.h
#include <assert.h>

#define TDB_SIZE XSMALL
#define TDB_KEYSIZE 128
#define TDB_VALSIZE 256


kvdb_s* test_create_database(char* name){
  if (name == NULL){
    // name = "./kv001.db";
    name = "/home/totem/kv001.db";
  }
  kvdb_s* kdb = create_kvdb(name, TDB_SIZE,
			    TDB_KEYSIZE, TDB_VALSIZE);
  assert(kdb != NULL);
  //printf("TEST: Succesfully created db\n");
  return kdb;
}


kvdb_s* test_load_database(char* name){
  if (name == NULL){
    // name = "./kv001.db";
    name = "/home/totem/kv001.db";
  }
  kvdb_s* kdb = load_kvdb(name);
  assert(kdb != NULL);
  //printf("TEST: Succesfully loaded db\n");
  return kdb;
}


#define DATASIZE 3
char* g_key[]={"tolga temel", "tolga", "temel"};
char* g_val[]={"val0", "val1tolga", "val2temel"};


int test_add_keyval(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = add(kdb, g_key[i], g_val[i]);
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_get_keyval(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char* val = get(kdb, g_key[i]);
    //printf("For key %s Got value %s\n", g_key[i], g_val[i]);
    assert(0 == strcmp(val, g_val[i]));
    if (val) free(val);
  }  
  return i;
}


int test_get_nonexistent_key(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[256]={0};
    snprintf(key, 256, "abcd-xyz-abcd-this is not in db %d", i);
    char* val = get(kdb, key);
    //printf("For key %s Got value %s\n", key, val);
    assert(NULL == val );
    if(val) free(val);
  }  
  return i;
}


int test_add_null_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYfornullval%d",i);
    int ret = add(kdb, key, NULL);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_add_null_key(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = add(kdb, NULL, g_val[i]);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_add_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYforzerolenVAL%d",i);
    int ret = add(kdb, key, "");
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_get_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYforzerolenVAL%d",i);
    char*  retval = get(kdb, key);
    assert(0 == strcmp(retval, ""));
    if (retval) free(retval);
  }
  return i;
}


int test_add_zerolen_key(kvdb_s* kdb){
  int i = 1;
  int ret = add(kdb, "", "Zero len key's value");
  assert(ret == 1);
  //printf("TEST: Wrote 1 more: %d\n", i);
  return i;
}

int test_get_zerolen_key(kvdb_s* kdb){
  int i = 1;
  char* zerokeyval = "Zero len key's value";
  char* retval = get(kdb, "");
  assert(0 == strcmp(retval, zerokeyval));
  if (retval) free(retval);
  return i;
}


int test_disconnect(kvdb_s* kdb){
  if (kdb){
    //printf("Let's clean up by freeing\n");  
    int ret = disconnect_kvdb(kdb);
    //if (ret <0) fprintf(stderr, "TEST: Cannot free db %s for test\n", name);
    assert(ret > 0);
  }
  return 1;
}


int test_add_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    char val[TDB_VALSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    snprintf(val, TDB_VALSIZE, "VALUE(%d) = VALUE(%d) = VALUE(%d)="
	     ,i,i,i);
    int ret = add(kdb, key, val);
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_re_add_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    char val[TDB_VALSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    snprintf(val, TDB_VALSIZE, "VALUE(%d) = VALUE(%d) = VALUE(%d)="
	     ,i,i,i);
    int ret = add(kdb, key, val);
    //it shd not allow
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}


int test_get_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    char val[TDB_VALSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    snprintf(val, TDB_VALSIZE, "VALUE(%d) = VALUE(%d) = VALUE(%d)="
	     ,i,i,i);
    char* retval = get(kdb, key);
    assert(0 == strcmp(retval, val));
    if (retval) free(retval);
  }
  return i;
}

int test_set_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    char val[TDB_VALSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    snprintf(val, TDB_VALSIZE, "NewValue(%d) =*=", i);
    int ret = set(kdb, key, val);
    assert(ret == 1);
    //printf("TEST: Set 1 more: %d\n", i);
  }
  return i;
}

int test_get_afterset_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    char val[TDB_VALSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    snprintf(val, TDB_VALSIZE, "NewValue(%d) =*=", i);
    char* retval = get(kdb, key);
    assert(0 == strcmp(retval, val));
    if (retval) free(retval);
    //printf("TEST: Got 1 more: %d\n", i);
  }
  return i;
}


int test_set_null_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYfornullval%d",i);
    int ret = set(kdb, key, NULL);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_set_null_key(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = set(kdb, NULL, g_val[i]);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_set_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYforzerolenVAL%d",i);
    int ret = set(kdb, key, "");
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  return i;
}

int test_get_afterset_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "KEYforzerolenVAL%d",i);
    char*  retval = get(kdb, key);
    assert(0 == strcmp(retval, ""));
    if (retval) free(retval);
  }
  return i;
}

int test_set_zerolen_key(kvdb_s* kdb){
  int i = 1;
  int ret = set(kdb, "", "Zero len key's value - UPDATED");
  assert(ret == 1);
  //printf("TEST: Wrote 1 more: %d\n", i);
  return i;
}

int test_get_afterset_zerolen_key(kvdb_s* kdb){
  int i = 1;
  char* zerokeyval = "Zero len key's value - UPDATED";
  char* retval = get(kdb, "");
  assert(0 == strcmp(retval, zerokeyval));
  if (retval) free(retval);
  return i;
}



/****
 * 
 *  test runner
 *
 */
int main(int argc, char *argv[])
{
  char* name = argv[1];

  kvdb_s* kdb = test_create_database(name);
  test_add_keyval(kdb); 
  test_get_keyval(kdb);
  test_disconnect(kdb);
  kdb = NULL;

  kdb = test_load_database(name);
  test_get_keyval(kdb);
  test_get_nonexistent_key(kdb);
  test_add_null_key(kdb);
  test_add_null_val(kdb);
  test_add_zerolen_key(kdb);
  test_add_zerolen_val(kdb);
  test_get_zerolen_val(kdb);
  test_get_zerolen_key(kdb);

  int LOADSIZE = 19900;
  test_add_many(kdb, LOADSIZE);
  test_get_keyval(kdb);
  test_re_add_many(kdb, LOADSIZE);
  test_get_many(kdb, LOADSIZE);
  test_get_keyval(kdb);
  test_set_many(kdb, LOADSIZE);
  test_get_afterset_many(kdb, LOADSIZE);
  test_set_null_key(kdb);
  test_set_null_val(kdb);
  test_set_zerolen_key(kdb);
  test_get_afterset_zerolen_key(kdb);
  test_set_zerolen_val(kdb);
  test_get_afterset_zerolen_val(kdb);
  
  test_disconnect(kdb);
  kdb = NULL;
  
  printf("Test cases executed succesfully\n");
  return 0;
}
