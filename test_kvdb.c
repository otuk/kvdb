
#include "kvdb.h"
#include <stdlib.h>  // exit
#include <string.h> //memset.h
#include <assert.h>


kvdb_s* test_create_database(char* name){
  if (name == NULL){
    // name = "./kv001.db";
    name = "/home/totem/kv001.db";
  }
  kvdb_s* kdb = create_kvdb(name, XSMALL, 128, 256);
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
    //printf("For key %s Got value %s\n", key1, val_1);
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


int test_disconnect(kvdb_s* kdb){
  if (kdb){
    //printf("Let's clean up by freeing\n");  
    int ret = disconnect_kvdb(kdb);
    //if (ret <0) fprintf(stderr, "TEST: Cannot free db %s for test\n", name);
    assert(ret > 0);
  }
  return 1;
}


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
  test_disconnect(kdb);
  kdb = NULL;
  
  printf("Test cases executed succesfully\n");
  return 0;
}
