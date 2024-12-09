#ifndef __UTIL_H__
#define __UTIL_H__

using namespace std;
//#include "dedupconfig.h"
//#include "dedupn.h"
#include "writebuf.h"
#include "iotracereader.h"
#include "storagenode.h"

// Disk operation time calculation related macros 
#define NBLKS_PER_CYLINDER	(32768)
#define TRACK(blk)		(blk / NBLKS_PER_CYLINDER)
#define SEEKBASE		(10.0)
#define MINORSEEKTIME(blk1, blk2)	((abs((int)(TRACK(blk2) - TRACK(blk1))) * 0.00005))
#define ROTDELAY		(0.5 * (60000.0 / 10000.0))


int md5hashIndex(void *h, unsigned int HASH_SIZE);
void assign_md5hash(void * dest, const void * src);
int compare_md5hash(const void * h1, const void * h2);
void assign_uint(void * dest, const void * src);
int compare_uint(const void * v1, const void * v2);
void DiskRead(StorageNode *sn, unsigned long long int ts, int disk, 
		unsigned long long int block, unsigned int count);
void DiskWrite(StorageNode *sn, unsigned long long int ts, int disk, 
		unsigned long long int block, unsigned int count);
unsigned char *minHash(unsigned char list[][16], int count);
unsigned char *minHash2(WriteReqBuf *w, int count);
unsigned char *minHash3(HashList *w, int count);
void displayIORecord(IORecord4K &rec);
void displayIORecord(IORecordHome &rec);
int readNextSegment(IOTraceReader *iot[], int iotcount, int *pid, 
		int *op, unsigned long long int *ts1, 
		unsigned long long int *ts2, segment4k *s1, 
		segmenthome *s2, int *trno);
int readNextFile(IOTraceReader *iot[], int iotcount, 
		struct FileBlocks *fb, int *trno);

void gc(void);

void display_io_response(void);
void display_iwrite_response(void);
unsigned long clock_start(void);
unsigned long clock_restart(void);
unsigned long clock_suspend(void);
unsigned long clock_stop(void);
unsigned long clock_add(unsigned long v);
unsigned long disk_start(void);
unsigned long disk_restart(void);
unsigned long disk_suspend(void);
unsigned long disk_stop(void);
unsigned long disk_timeadd(unsigned long v);

unsigned long clock_start(StorageNode *sn);
unsigned long clock_restart(StorageNode *sn);
unsigned long clock_suspend(StorageNode *sn);
unsigned long clock_stop(StorageNode *sn);
unsigned long clock_add(StorageNode *sn, unsigned long v);
unsigned long clock_get(void);
unsigned long clock_get(int clk);
unsigned long disk_start(StorageNode *sn);
unsigned long disk_restart(StorageNode *sn);
unsigned long disk_suspend(StorageNode *sn);
unsigned long disk_stop(StorageNode *sn);
unsigned long disk_timeadd(StorageNode *sn, unsigned long v);

void lockEntry(CacheEntry *c);
void unlockEntry(CacheEntry *c);

#endif
