
#include "kvdb.h"
#include <stdlib.h>  // exit
#include <string.h> //memset.h

int main(int argc, char *argv[])
{

  char* name = "./kv001.db";
  kvdb_s* kdb = create_kvdb(name, XSMALL, 128, 256);
  if (kdb == NULL){
    fprintf(stderr, "TEST: Cannot create db %s for test", name);
    exit(1);
  }
  
  printf("TEST: Succesfully created db\n");
  add(kdb, "tolga temel", "val1");
  printf("TEST: Wrote 1 \n");
  
 
  printf("Let's clean up by freeing\n");  
  if (free_kvdb(kdb)<0){
    fprintf(stderr, "TEST: Cannot free db %s for test\n", name);
  }  
    
  return 0;
}
