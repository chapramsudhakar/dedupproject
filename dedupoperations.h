#ifndef	__DEDUPOPERATIONS_H__
#define	__DEDUPOPERATIONS_H__
#include "iotracereader.h"

class StorageNode;

void processSegmentReadNative(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processSegmentReadHDS(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processSegmentReadFull(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processFileReadApp(struct FileBlocks *fb, StorageNode *sn,
		unsigned int pba[],
		unsigned long long int ts1, unsigned long long int ts2);
void processSegmentWriteNative(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processSegmentWriteHDS(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processSegmentWriteFull(struct segment4k *s, StorageNode *sn,
		unsigned long long int ts1, unsigned long long int ts2);
void processFileWriteApp(struct FileBlocks *fb, StorageNode *sn,
		int tno, unsigned int pba[],
		unsigned long long int ts1, unsigned long long int ts2);

#endif
