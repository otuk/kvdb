
#include "kvdb.h"
#include <stdlib.h>  // exit
#include <string.h> //memset.h
#include <assert.h>

#define TDB_SIZE XSMALL
#define TDB_KEYSIZE 128
#define TDB_VALSIZE 256

#define TEST_VERBOSE 1

#define ZEROLENVAL_KEY  "Zerolen Value's key%d"
#define ZEROLENKEY_VAL  "zero length key's value"
#define UPD_ZEROLENKEY_VAL  "Updated zero length key's value"
#define ZLVSIZE 5

int g_teststep = 0;

kvdb_s* test_create_database(char* name){
  if (name == NULL){
    // name = "./kv001.db";
    name = "/home/totem/kv001.db";
  }
  kvdb_s* kdb = create_kvdb(name, TDB_SIZE,
			    TDB_KEYSIZE, TDB_VALSIZE);
  assert(kdb != NULL);
  //printf("TEST: Succesfully created db\n");
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return kdb;
}


#define DATASIZE 3
char* g_key[]={"tolga temel", "tolga", "temel"};
char* g_val[]={"val0", "val1tolga", "val2temel"};


int test_add_minikv(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = add(kdb, g_key[i], g_val[i]);
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}

int test_get_minikv(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char* val = get(kdb, g_key[i]);
    //printf("For key %s Got value %s\n", g_key[i], g_val[i]);
    assert(0 == strcmp(val, g_val[i]));
    if (val) free(val);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}

int test_add_null_key(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = add(kdb, NULL, g_val[i]);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}

int test_add_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<ZLVSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, ZEROLENVAL_KEY, i);
    int ret = add(kdb, key, "");
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}

int test_get_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<ZLVSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, ZEROLENVAL_KEY, i);
    char*  retval = get(kdb, key);
    assert(0 == strcmp(retval, ""));
    if (retval) free(retval);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}


int test_add_zerolen_key(kvdb_s* kdb){
  int i = 1;
  int ret = add(kdb, "", ZEROLENKEY_VAL);
  assert(ret == 1);
  //printf("TEST: Wrote 1 more: %d\n", i);
  return i;
}

int test_get_zerolen_key(kvdb_s* kdb){
  int i = 1;
  char* zerokeyval = ZEROLENKEY_VAL;
  char* retval = get(kdb, "");
  assert(0 == strcmp(retval, zerokeyval));
  if (retval) free(retval);
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
  return i;
}


int test_disconnect(kvdb_s* kdb){
  if (kdb){
    //printf("Let's clean up by freeing\n");  
    int ret = disconnect_kvdb(kdb);
    //if (ret <0) fprintf(stderr, "TEST: Cannot free db %s for test\n", name);
    assert(ret > 0);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);  
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
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
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}

int test_set_null_key(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret = set(kdb, NULL, g_val[i]);
    assert(ret == 0);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}

int test_set_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<ZLVSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, ZEROLENVAL_KEY, i);
    int ret = set(kdb, key, "");
    assert(ret == 1);
    //printf("TEST: Wrote 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}

int test_get_afterset_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<ZLVSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, ZEROLENVAL_KEY, i);
    char*  retval = get(kdb, key);
    assert(0 == strcmp(retval, ""));
    if (retval) free(retval);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}

int test_set_zerolen_key(kvdb_s* kdb){
  int i = 1;
  int ret = set(kdb, "", UPD_ZEROLENKEY_VAL);
  assert(ret == 1);
  //printf("TEST: Wrote 1 more: %d\n", i);
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}

int test_get_afterset_zerolen_key(kvdb_s* kdb){
  int i = 1;
  char* zerokeyval = UPD_ZEROLENKEY_VAL;
  char* retval = get(kdb, "");
  assert(0 == strcmp(retval, zerokeyval));
  if (retval) free(retval);
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


int test_count(int ec){
  assert(count() == ec);
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return 1;  
}


int test_delmini_kv(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    int ret  = del(kdb, g_key[i]);
    assert(1 == ret);
    char* retval = get(kdb, g_key[i]);
    assert(NULL == retval);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


int test_get_afterdelmini_kv(kvdb_s* kdb){
  int i =0;
  for (; i<DATASIZE; i++){
    char* val = get(kdb, g_key[i]);
    //printf("For key %s Got value %s\n", g_key[i], g_val[i]);
    assert(NULL == val);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


int test_del_many(kvdb_s* kdb, int LOADSIZE){
  int i =0;
  for (; i<LOADSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    int ret = del(kdb, key);
    assert(1 == ret);
    char* retval = get(kdb, key);
    assert(NULL == retval);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


int test_get_afterdel_many(kvdb_s* kdb, int LOADSIZE){
  int i =LOADSIZE -1;
  for (; i>0; i--){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, "%dkey%d-%d",i,i,i);
    char* retval = get(kdb, key);
    assert(NULL == retval);
    if (retval) free(retval);
    //printf("TEST: Got 1 more: %d\n", i);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


//
int test_del_zerolen_val(kvdb_s* kdb){
  int i =0;
  for (; i<ZLVSIZE; i++){
    char key[TDB_KEYSIZE]={0};
    snprintf(key, TDB_KEYSIZE, ZEROLENVAL_KEY, i);
    int ret  = del(kdb, key);
    assert(1 == ret);
  }
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return i;
}


//
int test_del_zerolen_key(kvdb_s* kdb){
  char* key = "";
  int ret  = del(kdb, key);
  assert(1 == ret);
  if(TEST_VERBOSE) printf("Test step: %d completed\n", ++g_teststep);
  return 1;
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
  test_count(0);
  test_add_minikv(kdb);  //adds 3 tolga etc
  test_get_minikv(kdb);
  test_count(3);
  test_disconnect(kdb);
  kdb = NULL;

  kdb = test_load_database(name);
  test_count(3); // confirm still 3 after load
  test_get_minikv(kdb);
  test_get_nonexistent_key(kdb);
  test_add_null_key(kdb);
  test_add_null_val(kdb);
  test_add_zerolen_key(kdb); //ads 1
  test_add_zerolen_val(kdb); //adds 5
  test_count(9);
  test_get_zerolen_val(kdb);
  test_get_zerolen_key(kdb);

  int LOADSIZE = (2 * TDB_SIZE) - 9; // set to max it
  test_add_many(kdb, LOADSIZE); // adds loadsize many
  test_count(LOADSIZE+9);
  test_get_minikv(kdb);  
  test_re_add_many(kdb, LOADSIZE); // adds 0/none
  test_count(LOADSIZE+9);
  test_get_many(kdb, LOADSIZE);
  test_get_minikv(kdb);
  test_set_many(kdb, LOADSIZE);
  test_get_afterset_many(kdb, LOADSIZE);
  test_set_null_key(kdb);
  test_set_null_val(kdb);
  test_set_zerolen_key(kdb);
  test_get_afterset_zerolen_key(kdb);
  test_set_zerolen_val(kdb);
  test_get_afterset_zerolen_val(kdb);
  test_count(LOADSIZE+9);

  test_delmini_kv(kdb); // deletes 3 - tolga etc
  test_get_afterdelmini_kv(kdb); 
  test_count(LOADSIZE+6);
  test_get_afterset_many(kdb, LOADSIZE);
  test_del_many(kdb, LOADSIZE);
  test_get_afterdel_many(kdb, LOADSIZE);
  test_count(6); 
  test_del_zerolen_key(kdb); // deletes 1
  test_count(5);
  test_del_zerolen_val(kdb);  //deletes 5
  test_count(0);

  test_add_zerolen_key(kdb); // adds 1
  test_add_zerolen_val(kdb); //adds 5
  test_count(6);
  test_add_many(kdb, 2*TDB_SIZE - 6);
  test_count(2*TDB_SIZE); // max reached
  test_set_many(kdb, 2*TDB_SIZE - 6);
  test_get_afterset_many(kdb, 2*TDB_SIZE - 6);
  test_count(2*TDB_SIZE); // confirm max
  test_del_many(kdb, 2*TDB_SIZE - 6); // delete all
  test_count(6);
  test_set_zerolen_val(kdb); 
  test_del_zerolen_val(kdb); //deletes 3
  test_del_zerolen_key(kdb);  //deletes 1
  test_count(0);
  
  test_disconnect(kdb);
  kdb = NULL;
  
  printf("Test cases executed succesfully\n");
  return 0;
}
