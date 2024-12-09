#ifndef __BITMAP_H__
#define __BITMAP_H__

using namespace std;

#include "dedupconfig.h"
#include "dedupn.h"

struct bm {
	unsigned int maxBlocks;
	unsigned int freeBlockCount;
};

class BitMap {
private:
	struct fblist {
		unsigned int            fbstart;
		unsigned int            count;
		struct fblist           * next;
		struct fblist           * prev;
	};

#define FLISTLEN        4096
	struct fblist flist[FLISTLEN];
	struct fblist *fl, *head, *tail;
	unsigned int bmlen;
	unsigned char bitmap[1024*1024/8];      // 1 MB number of blocks information
	volatile unsigned int from, to;  // Starting block number and ending
                                        // block number in the in-core bitmap
	unsigned int pos;
	struct bm bm_header;
	char bmfile[256];
	fstream bmfs;
	//unsigned long long int pbaoffset;
	unsigned long long int allocBlocks1(int nb, unsigned int pos);
	int freeVals(unsigned int n);
	void initializeFreeList(void);
	unsigned int getVals(unsigned int n);
	int getBitMap(); 		// Get the present bitmap details
	int getNextBitMap(); 		// Get next part of the bitmap
	int findNextBitSet(int p); 	// Return next bit number which is 
					// set to one starting from p. 
					// If not found, returns -1.
	void setBlock(unsigned int b);
	
public:
	void initializeBitMap(); 	// Initialize bitmap
	//BitMap(char *bname, unsigned long long int offset); 		
	BitMap(char *bname); 		
					// Initial opening of the bitmap
	unsigned long long int allocBlocks(int nb); 
					//Allocates nb number of contiguous 
					//blocks of size BLOCKSIZE
	void freeBlock(unsigned long long int b); 
					// Free a block b
	void displayBitMapStatistics(void);
	void updateBitMap(); 		// Update Bit Map
	void closeBitMap(void);
};

#endif
