
#include <sys/types.h>  //open ftruncate
#include <sys/stat.h>  //open
#include <fcntl.h>  //open
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h> //mmap &munmap
#include <unistd.h>  // close(fd) ftruncate
#include <stdio.h> // FILE*

#include "_tnt_util.h" // common utilities
FILE* myerrorstream = NULL;  // this can be set to any stream

#include "kvdb.h"  //public header
#include "_kvdb_pr.h"  //private header
#include "hash_32.h" //FVN1A hash header for mimimum collison

/**
 *  KVDB's mapped file's fixed layout:
 *  1 * header (kvhdr_s),  
 *  size * hash-index of key-value locations (kidx_s), 
 *  stacksize * stack of removed location addresses (kidx_s), 
 *  dbsizefactor * key-value record entries (kv_s) 
**/

// offset constants used in accessing parts of mapped memory
static uint32_t OFFSET_KIDX;
static uint32_t OFFSET_STACK;
static uint32_t OFFSET_KV;
static uint32_t KVR_SIZE;

// db header contains all the configuration numbers db needs
static kvhdr_s* g_kv_hdr;


// initialize and write db header info to file
static void set_kvhdr(kvhdr_s* mdat, dbsize hashsize, uint16_t keysize,
		       uint16_t valsize, uint32_t maxecount,
		      uint32_t maxscount, uint32_t flen){
  g_kv_hdr = mdat;  // start of mapped memory
  g_kv_hdr->hashsize = hashsize; // hash max size 
  g_kv_hdr->keysize = keysize; 
  g_kv_hdr->valsize = valsize;
  g_kv_hdr->ecount = 0; //current stored element count
  g_kv_hdr->scount = 0; //available deleted location count 
  g_kv_hdr->maxecount = maxecount; //max db storage allowance
  g_kv_hdr->maxscount = maxscount; //max delete stack allowance
  g_kv_hdr->flen = flen;
}


// calculates the constants to be used for memory access
static void calculate_constants(dbsize size, uint16_t keysize,
				uint16_t valsize){
  //printf( "size of dbsize is %u\n", (uint32_t) sizeof(dbsize));
  //printf( "size of k index is %u\n", (uint32_t) sizeof(kidx_s));
  //printf( "size of kvhdr_s is %u\n", (uint32_t) sizeof(kvhdr_s));
  OFFSET_KIDX = sizeof(kvhdr_s);
  OFFSET_STACK = (size * sizeof(kidx_s)) + OFFSET_KIDX;
  OFFSET_KV = (DEL_STACK_FACTOR * size *  sizeof(kidx_s))
              + OFFSET_STACK;
  KVR_SIZE =  sizeof(uint32_t)  //kv_s struct
            + sizeof(uint32_t)
            + (sizeof(char) * keysize)
            + (sizeof(char) * valsize);//kv_s struct
}


//initializes enveloping and opaque structures 
static kvdb_s* initialize_db_artifacts(const char* name, int fd,
				       kvdata_s* mdat, uint16_t keysize,
				       uint16_t valsize, dbsize hashsize){
  kvdb_s* db_m = malloc(sizeof(kvdb_s));
  HANDLE(!db_m, 0, return NULL,
	 "Cannot allocate memory for kvdb_s.\n");
  db_m->name = strdup(name);
  db_m->keysize = keysize;
  db_m->valsize = valsize;
  kvdata_s* data_m = malloc(sizeof(kvdata_s));
  HANDLE(!data_m, 0, return NULL,
	 "Cannot allocate memory for kvdb data_m.\n");
  db_m->data = data_m;   

  data_m->fd = fd;
    data_m->mdat = mdat;
  return db_m;
}


/*
 *  create a new kvdb
 *
 *
 */
kvdb_s* create_kvdb(const char* name, dbsize size,
		    uint16_t keysize, uint16_t valsize){  
  int fd = open(name, O_RDWR | O_CREAT | O_EXCL,  S_IRUSR | S_IWUSR);
  HANDLE(fd < 0, 0, return NULL,
	 "Cannot create file for kvdb_s for RW access %s\n", name);
  calculate_constants(size, keysize, valsize);
  uint32_t len = OFFSET_KV + ( DB_SIZE_FACTOR * size * KVR_SIZE);
  ftruncate(fd, len);

  void* mdat = mmap(NULL, len,
		    PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED,
		    fd, 0);
  HANDLE(mdat == MAP_FAILED, 0, return NULL,
	 "Cannot map memory for kvdb data_m.\n");
  
  kvdb_s* db_m = initialize_db_artifacts(name, fd, mdat, keysize, valsize, size);
  kvdata_s* data_m= db_m->data;
  set_kvhdr(data_m->mdat, size, keysize, valsize,
	    DB_SIZE_FACTOR * size,
	    DEL_STACK_FACTOR * size,
	    len);

  printf("set maxscount %u\n", DEL_STACK_FACTOR * size);
  printf("set maxscount %u\n", g_kv_hdr->maxscount);

  return db_m;  
}


// simply point the hdr pointer to the start of mapped memory
static void read_into_header(kvhdr_s* mdat){
  g_kv_hdr = mdat;
  printf("loaded maxscount %u\n", g_kv_hdr->maxscount);
  printf("loaded scount %u\n", g_kv_hdr->scount);
  printf("loaded maxecount %u\n", g_kv_hdr->maxecount);
  printf("loaded ecount %u\n", g_kv_hdr->ecount);
  printf("loaded keysize %u\n", g_kv_hdr->keysize);
  printf("loaded valsize %u\n", g_kv_hdr->valsize);
  printf("loaded hashsize %u\n", g_kv_hdr->hashsize);
  printf("loaded flen %u\n", g_kv_hdr->flen);
  
  
}


/*
 *  Load and existing kvdb with file name
 *
 *
 */
kvdb_s* load_kvdb(const char* name){
  int fd = open(name, O_RDWR,  S_IRUSR | S_IWUSR);
  HANDLE(fd < 0, 0, return NULL,
	 "Cannot load file for kvdb_s for RW access %s\n", name);
  struct stat sb;
  HANDLE(fstat(fd, &sb) == -1, 0, return NULL,
	 "Cannot read file stats %s\n", name);
  HANDLE(!S_ISREG (sb.st_mode), 0, return NULL,
	 "%s is not a normal file\n", name);

  void* mdat =  mmap(NULL, sb.st_size,
		     PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED,
		     fd, 0);
  HANDLE(mdat == MAP_FAILED, 0, return NULL,
	 "Cannot map memory for kvdb data_m.\n");
  
  read_into_header(mdat);

  calculate_constants(g_kv_hdr->hashsize, g_kv_hdr->keysize,
		      g_kv_hdr->valsize);
  kvdb_s* db_m = initialize_db_artifacts(name, fd, mdat,
					 g_kv_hdr->keysize,
					 g_kv_hdr->valsize,
					 g_kv_hdr->hashsize);

  return db_m;  
}



/*
 *  disconnect from kvdb and claan up resources
 *
 *
 */
int disconnect_kvdb(kvdb_s* db_m){
  if(db_m){
    printf(" before disconnect %u\n",g_kv_hdr->maxscount);
    if(db_m->name){
      free((void*) db_m->name);
    }
    kvdata_s* data_m = (kvdata_s*) db_m->data; 
    if (data_m){
      if (data_m->mdat){
	msync(data_m->mdat, g_kv_hdr->flen, MS_SYNC);
	munmap(data_m->mdat, g_kv_hdr->flen);
      }
      if(data_m->fd > 0){ 	
	close(data_m->fd);
      }
      free((void*)data_m);
    }
    free((void*) db_m);
  }

  return 1;
}


//
static void add_hdr_count(){
  g_kv_hdr->ecount++;
}


//get the index location (that will store  the kvoffset for hash)
static uint32_t get_kidx_offset(uint32_t hash){
  return (hash*sizeof(kidx_s)) + OFFSET_KIDX;
}


//first available kv location offset
static uint32_t get_kv_fa_offset(){
  return (g_kv_hdr->ecount * KVR_SIZE ) + OFFSET_KV;
}  


//mother of all, start of mapped memory area
static char* get_mdat(kvdb_s* db_m){
  kvdata_s* kvd = (kvdata_s*) db_m->data;
  return  kvd->mdat;
}


//refit has to maximum index sie for db 
inline static uint32_t resize_hash(uint32_t hash){
  HANDLE(g_kv_hdr->hashsize <= 0, 1, ,
	 "Aborting to preserve data integrity, \
          unexpected hashsize observed in kvdb./n");
  hash %= g_kv_hdr->hashsize;
  return hash;
}


inline static char* get_key_ptr(kv_s* ckv){
  return (char*) ckv + 2*sizeof(uint32_t); 
}


inline static char* get_val_ptr(kv_s* ckv){
  return get_key_ptr(ckv) + g_kv_hdr->keysize;  
}


//write a new kv record at newkv location
static void write_newkv(kv_s* newkv,
			const char* key, const char* val,
			uint32_t prev){
  newkv->prev = prev;
  newkv->next = 0;
  strncpy(get_key_ptr(newkv), key, g_kv_hdr->keysize);
  strncpy(get_val_ptr(newkv), val, g_kv_hdr->valsize);
  add_hdr_count();   
}


//given hash what does the idx have for corresponding kv location
static uint32_t get_current_idxkv_offset(char* mdat, uint32_t hash){
  kidx_s* newidx =(kidx_s*) (mdat + get_kidx_offset(hash));
  return newidx->idx; 
}
  

// returns 1 if the idx location has anything but 0
static int is_hash_already_populated(char* mdat, uint32_t hash){
  return get_current_idxkv_offset(mdat, hash) ==0 ? 0: 1;
}


//
inline static int del_stack_exists(){
  return  g_kv_hdr->scount > 0 ? 1: 0;
}


//
static uint32_t get_deleted_location_offset(char* mdat){
  // pick up the last stack content
  kidx_s* lds =(kidx_s*) (mdat + OFFSET_STACK +
			  (g_kv_hdr->scount - 1) * sizeof(kidx_s));
  uint32_t kvlocoffset = lds->idx; 
  // reduce stack count
  g_kv_hdr->scount--;
  //also set last stack to 0
  lds->idx = 0;
  return kvlocoffset;
}


// call this to get where to write the key-value record 
static uint32_t get_kv_offset(char* mdat){
  uint32_t kv_offset = 0;
  if(del_stack_exists()){
    //first reuse any location that was deleted
    kv_offset = get_deleted_location_offset(mdat);
  }else{
    // nodeletions, then get the first available kv offset
    kv_offset =  get_kv_fa_offset();  
  }
  HANDLE(kv_offset == 0, 1, , "Abnormal kv offset noticed in kv db aborting, to preserve data integrity\n");
  return kv_offset;
}


//
inline static int check_keys_match(kv_s* lkv, const char* key){
  return (0==strncmp(key, get_key_ptr(lkv),
		     g_kv_hdr->keysize)) ? 1: 0;
}


//start from currkvoffset and follow to the end of chain, return last
static uint32_t get_last_kv_offset(char* mdat, const char* key,
			      uint32_t currkvoffset){  
  //either return last kv offset or return 0 if key exists
  kv_s* lkv = NULL;
  uint32_t intkvoffset = currkvoffset;
  // follow the kv chain to the end
  // but return 0 if there is a matching key,
  // because we do not want to add the same key
  do{
    lkv = (kv_s*)(mdat + intkvoffset);
    if(check_keys_match(lkv, key)){
      // keys match return 0
      return 0;
    }else if (lkv->next == 0){
      return intkvoffset;
    }else{
      intkvoffset = lkv->next;
    }  
  }while(1);
  return 0;
}


//
static int write_newindex(char* mdat,
			  const char* key, const char* val,
			  uint32_t hash, uint32_t kvlocoffset){  
  kidx_s* newidx = NULL;
  // see if there is a previous entry with same key
  // if there is do not write anything/return 0 adds
  // if no-existing-keys match, either add it as the first kv record,
  // or add it at the end of the chain for the same hash
  if(is_hash_already_populated(mdat, hash)>0){
    // there is already a key with same hash
    uint32_t currkvoffset = get_current_idxkv_offset(mdat, hash);
    // follow the kv-chain and populate to last one's next  field
    kv_s* lkv = NULL;
    uint32_t lkvoffset = 0;
    if( (lkvoffset = get_last_kv_offset(mdat, key, currkvoffset)) ){
      //we have the offset for new kv record
      // we also have the last kv record
      lkv = (kv_s*)(mdat + lkvoffset);
      lkv->next = kvlocoffset;
      kv_s* newkv = (kv_s*)(mdat + kvlocoffset);
      write_newkv(newkv, key, val, lkvoffset);
      return 1;
    }else{
      //same key must have been found;
      // dont write index or kv
      return 0;
    }  
  }else{
    //easy path just place in the index list
    newidx =(kidx_s*) (mdat + get_kidx_offset(hash));
    newidx->idx = kvlocoffset; // key val location
    kv_s* newkv = (kv_s*)(mdat + kvlocoffset);
    write_newkv(newkv, key, val, 0);
    return 1;
  }  
  return 0;
}


inline static int max_reached(){
  return (g_kv_hdr->ecount +1 > g_kv_hdr->maxecount) ? 1: 0;
}

inline static int notgood_key_value(const char* key,
				    const char* val){
  return (!key || !val ) ?  1: 0;
}


/*
 *  Add a new key - value pair to the db
 *
 *
 */
int add(kvdb_s* db_m, const char* key, const char* val){
  if (max_reached() || notgood_key_value(key,val)){
    return 0;
  }  
  char* mdat = get_mdat(db_m);
  uint32_t hash  = (uint32_t)fnv_32_str( (char*) key, FNV1_32_INIT);
  //printf("header hashsize is %d\n",g_kv_hdr->hashsize);  
  hash  = resize_hash(hash);
  // get where is the correct place to write, but dont write yet 
  uint32_t kvlocoffset = get_kv_offset(mdat);
  if (write_newindex(mdat, key, val, hash, kvlocoffset)){
    // wrote 1, return 1;
    return 1;
  }else{
    return 0;
  }
}


//copy the value of "the current kv" record to a mallocated address
static void copy_value(kv_s* ckv, char** val_m){
  *val_m = malloc(sizeof(char) * g_kv_hdr->valsize);
  strncpy(*val_m,
	  (char*) ckv + 2*sizeof(uint32_t) + g_kv_hdr->keysize,
	  g_kv_hdr->valsize);
}



/*
 *  Given a key get the value from db
 *
 *
 */
char* get(kvdb_s* db_m, const char* key){
  char* mdat = get_mdat(db_m);
  uint32_t hash = (uint32_t) fnv_32_str((char*) key, FNV1_32_INIT);
  hash = resize_hash(hash);
  if(is_hash_already_populated(mdat, hash)){
    // if hashlocation is populated goto the location
    uint32_t ckvoffset =  get_current_idxkv_offset(mdat, hash);
    do{
      kv_s* ckv = (kv_s*)(mdat + ckvoffset);
      if (ckv->next != 0){
	// the next location is populated
	// but check this key to see if they match 
	if(check_keys_match(ckv, key)){
	  // we found a match  with key no need to search more
	  char* val_m = NULL;
	  //printf("Found key %s\n", key);
	  copy_value(ckv, &val_m);
	  return val_m;
	}else{
	  // the keys did not match, bu there is next location
	  //lets try that
	  ckvoffset = ckv->next;
	  continue;
	}
      }else if(check_keys_match(ckv, key)){
	// this is the last in the chain and keys matched
	char* val_m = NULL;
	//printf("Found key %s\n", key);
	copy_value(ckv, &val_m);
	return val_m;
      }else{
	// this was the last one,
	// but the key did not match
	return NULL;
      }	
    }while(1);
    return NULL;
  }else{
    //if hashlocation is 0 return null immediately
    return NULL;
  }
}



/*
 *  Given a key and value set the value in db
 *
 *
 */
int set(kvdb_s* db_m, const char* key, const char* val){
  if (notgood_key_value(key,val)){
    return 0;
  }  
  char* mdat = get_mdat(db_m);
  uint32_t hash = (uint32_t) fnv_32_str((char*) key, FNV1_32_INIT);
  hash = resize_hash(hash);
  if(is_hash_already_populated(mdat, hash)){
    // if hashlocation is populated goto the location
    uint32_t ckvoffset =  get_current_idxkv_offset(mdat, hash);
    do{
      kv_s* ckv = (kv_s*)(mdat + ckvoffset);
      if (ckv->next != 0){
	// the next location is populated
	// but check this key to see if they match 
	if(check_keys_match(ckv, key)){
	  // we found a match  with key no need to search more
	  strncpy(get_val_ptr(ckv), val, g_kv_hdr->valsize);
	  //printf("Found key to set %s\n", key);
	  return 1;
	}else{
	  // the keys did not match, bu there is next location
	  //lets try that
	  ckvoffset = ckv->next;
	  continue;
	}
      }else if(check_keys_match(ckv, key)){
	// this is the last in the chain and keys matched
	strncpy(get_val_ptr(ckv), val, g_kv_hdr->valsize);
	//printf("Found key to set %s\n", key);
	return 1;
      }else{
	// this was the last one,
	// but the key did not match
	return 0;
      }	
    }while(1);
    return 0;
  }else{
    //if hashlocation is 0 return 0 immediately
    return 0;
  }
}


//
static void update_idx_kv_links(char* mdat, uint32_t hash,
			   uint32_t pkvoffset, uint32_t dkvoffset,
			   uint32_t nkvoffset){
  kv_s* dkv = (kv_s*) (mdat + dkvoffset);
  if(pkvoffset){
    //there is a previous kv in the chain before dkv
    if(nkvoffset){
      // there is also a next kv
      kv_s* pkv = (kv_s*) (mdat + pkvoffset);
      pkv->next = nkvoffset;
      kv_s* nkv = (kv_s*) (mdat + nkvoffset);
      nkv->prev = pkvoffset;
    }else{
      // no next kv we are deleting last in kv chain
      kv_s* pkv = (kv_s*) (mdat + pkvoffset);
      pkv->next = 0;
    }
  }else{
    // nothing previous, dkv is the first in kv chain
    if(nkvoffset){
      //there is a next one after this first dkv
      kidx_s* idxkv =(kidx_s*) (mdat + get_kidx_offset(hash));
      idxkv->idx = nkvoffset;
      kv_s* nkv = (kv_s*) (mdat + nkvoffset);
      nkv->prev = 0;
    }else{
      // this is the first and the only one
      kidx_s* idxkv =(kidx_s*) (mdat + get_kidx_offset(hash));
      idxkv->idx = 0;
    }
  }
  //reset the dkv data area
  dkv->prev = 0;
  dkv->next = 0;
  strncpy(get_key_ptr(dkv), "", g_kv_hdr->keysize);
  strncpy(get_val_ptr(dkv), "", g_kv_hdr->valsize);
}


//
static int add_to_del_stack(char* mdat, uint32_t dkvoffset){
  if (g_kv_hdr->scount + 1 > g_kv_hdr->maxscount){
    fprintf(stderr,"scount maxscount %u , %u \n",g_kv_hdr->scount, g_kv_hdr->maxscount);
    fprintf(stderr,"ecount maxecount %u , %u \n",g_kv_hdr->ecount, g_kv_hdr->maxecount);
  }
  HANDLE( g_kv_hdr->scount + 1 > g_kv_hdr->maxscount,
	  0, return 0,
	  "Trying to delete more than elements in the db - not expected");
  kidx_s* nds =(kidx_s*) (mdat + OFFSET_STACK +
			  g_kv_hdr->scount * sizeof(kidx_s));
  nds->idx = dkvoffset;
  g_kv_hdr->scount++;
  g_kv_hdr->ecount--;
  return 1;
}


/*
 *  Given a key delete the key-value record from db
 *
 *
 */
int del(kvdb_s* db_m, const char* key){
  if (!key){
    return 0;
  }  
  char* mdat = get_mdat(db_m);
  uint32_t hash = (uint32_t) fnv_32_str((char*) key, FNV1_32_INIT);
  hash = resize_hash(hash);
  if(is_hash_already_populated(mdat, hash)){
    // if hashlocation is populated goto the location
    uint32_t ckvoffset = get_current_idxkv_offset(mdat, hash);
    uint32_t pkvoffset = 0;
    uint32_t nkvoffset = 0;
    do{
      kv_s* ckv = (kv_s*)(mdat + ckvoffset);
      nkvoffset = ckv->next;
      if (ckv->next != 0){
	// the next location is populated
	// but check this key to see if they match 
	if(check_keys_match(ckv, key)){
	  // we found a match  with key no need to search more
	  // lets delete if we can add to location to del stack
	  if(add_to_del_stack(mdat, ckvoffset)){
	    //was able to increase delete stack
	    update_idx_kv_links(mdat, hash, pkvoffset, ckvoffset, nkvoffset);
	    return 1;
	  }else{
	    return 0;
	  }
	}else{
	  // the keys did not match, bu there is next location
	  //lets try that
	  pkvoffset = ckvoffset;
	  ckvoffset = ckv->next;
	  continue;
	}
      }else if(check_keys_match(ckv, key)){
	// this is the last in the chain and keys matched
	if(add_to_del_stack(mdat, ckvoffset)){
	  //was able to increase delete stack
	  update_idx_kv_links(mdat, hash, pkvoffset, ckvoffset, nkvoffset);
	  return 1;
	}else{
	  return 0;
	}
      }else{
	// this was the last one,
	// but the key did not match
	return 0;
      }	
    }while(1);
    return 0;
  }else{
    //if hashlocation is 0 return 0 immediately
    return 0;
  }
}


/*
 *  Return the numbers of keys (and hence values) stored
 *
 *
 */
int count(){
  return g_kv_hdr->ecount;
}

/** to do

int has(kvdb_s* db_m, const char* key){

}





**/
