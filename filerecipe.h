#ifndef __FILERECIPE_H__
#define __FILERECIPE_H__

using namespace std;

#include "freelist.h"

#include "darc.h"
#include "lru.h"

#define IIHMINKEYS	15
#define IIHMAXKEYS 	(2*IIHMINKEYS+1)	// Size of the node will be 000 ~ 512
#define IIHMINDEGREE	(IIHMINKEYS+1)	// Minimum degree
 
struct IntIntIntBTNode {
	unsigned int 	btn_key[IIHMAXKEYS]; 	
				// Keys, first lba block addresses of files 
	unsigned int	btn_value[IIHMAXKEYS];	// Number of blocks
	unsigned int	btn_value1[IIHMAXKEYS];	// FBlist first entry 
	unsigned int	btn_ptrs[IIHMAXKEYS+1];
	unsigned int    btn_ucount;
	// First node's btn_ucount contains total number of
	// used nodes in the entire index file.
	// Ponters to btree nodes (numbers)
	unsigned short  btn_used;       // Is this used node or free node
	unsigned short  btn_leaf;       // Is this leaf node
	unsigned short  btn_count;      // Count of keys
	//unsigned char   btn_padding[4];	//round to 512 bytes
};

struct IIIBTVal {
	unsigned int 	key;
	unsigned int 	val;
	unsigned int	val1;
	struct IIIBTVal 	*next, *prev;
	struct IIIBTVal	*tnext, *tprev;
	unsigned short	flags;
};

class StorageNode;

class IIIBTree {
private:
	char		bt_fname[100];	// Name of the index file
	fstream		bt_fs;		// File stream
	unsigned int	bt_nblks;	// Current number of blocks
	unsigned int	bt_maxblks;	// Maximum number of blocks
	unsigned int	bt_incr;
	unsigned int 	bt_hashsize;	// Cache hash list size

#ifdef	DARCP
	DARC		*bt_btcache;	// btree node cache
#else
	LRU		*bt_btcache;	// btree node cache
#endif

	//struct IBTVal	*keyValCache;
	struct IIIBTVal	*FL;
	unsigned int	KVCacheSize;
	unsigned int 	KVHASH;
	struct IIIBTVal	**KVQ;
	struct IIIBTVal	*KVTail;
	struct IIIBTVal	*KVHead;

	unsigned int nextFreeIIIBTreeNode;
	FreeList *flist;

	int nodeCount;
	int maxlevel;
	StorageNode *bt_sn;

	// Free node by writing zero bytes in the node
	void freeNode(unsigned int n);

	// Find a one free (unused) node
	unsigned int findFreeNode(void);
	
	// Split the child node into two nodes
	void splitChild(IntIntIntBTNode *par, unsigned int parNo,
			int i, IntIntIntBTNode *child, unsigned int childNo);

	// Insert a key and its address in a non-full Node
	void insertNonFull(IntIntIntBTNode *b, unsigned int bNo, 
			unsigned int key, unsigned int val, unsigned int val1);

	// Left shift value and lba elements position 'from' to 'to' 
	void leftShiftVal(IntIntIntBTNode *b, int from, int to, int count);

	// Left shift ptrs position 'from' to 'to' 
	void leftShiftPtrs(IntIntIntBTNode *b, int from, int to, int count);

	// right shift value and lba elements position 'from' to 'to'
	void rightShiftVal(IntIntIntBTNode *b, int from, int to, int count);
	
	// right shift ptrs position 'from' to 'to'
	void rightShiftPtrs(IntIntIntBTNode *b, int from, int to, int count);

	// Copy ptrs from b1 to b2 from position 'from' to 'to'
	void copyPtrs(IntIntIntBTNode *b1, IntIntIntBTNode *b2, 
		int from, int to, int count);

	// Copy val and lba elements from b1 to b2 from position 'from' to 'to'
	void copyVal(IntIntIntBTNode *b1, IntIntIntBTNode *b2, 
		int from, int to, int count);

	// Modified root/root of a subtree, whose child is y, is 
	// to be written with proper checking.
	void writeModifiedRoot(IntIntIntBTNode *x, unsigned xno, 
		IntIntIntBTNode *y, unsigned int * yno);

	void findMin(unsigned int *key, unsigned int *value, 
		unsigned int *value1, IntIntIntBTNode *x, unsigned int xno);

	void findMax(unsigned int *key, unsigned int *value, 
		unsigned int *value1, IntIntIntBTNode *x, unsigned int xno);
	int deleteVal1(unsigned int key, unsigned int val, unsigned int val1, 
		IntIntIntBTNode *x, unsigned int xno);
	// Read a node from the index file
	int readNode(unsigned int n, IntIntIntBTNode *b);

	// Write a node into the index file
	int writeNode(unsigned int n, IntIntIntBTNode *b);

	// Returns the block if found in the cache otherwise read and add the
	// node to cache 
	IntIntIntBTNode * getNode(unsigned int n);

	// Writes the block, into cached block, 
	// otherwise creates a new cached copy
	int putNode(unsigned int n, IntIntIntBTNode *b);

	// Releases the block, if modified then set the dirty flag
	void releaseNode(unsigned int n, unsigned short dirty);
	void kvdelete(unsigned int i, IIIBTVal *c);
	void kvprepend(unsigned int i, IIIBTVal *c);
	IIIBTVal * kvsearch(unsigned int i, unsigned int key);
	IIIBTVal * kvalloc(void);

public:
	unsigned long int keycount;
	//////////////////////////////////////////////
	unsigned long long int    BT_BTNODE_READCOUNT;
	unsigned long long int    BT_BTNODE_READ_CACHE_HIT;
	unsigned long long int    BT_BTNODE_WRITECOUNT;
	unsigned long long int    BT_BTNODE_WRITE_CACHE_HIT;
	unsigned long long int	  BT_BTNODE_EFFECTIVE_WRITES;
	unsigned long long int	  BT_BTNODE_FLUSH_WRITES;

	unsigned long long int    BT_BTNODE_READCOUNT_READ;
	unsigned long long int    BT_BTNODE_WRITECOUNT_READ;
	unsigned long long int    BT_BTNODE_READCOUNT_WRITE;
	unsigned long long int    BT_BTNODE_WRITECOUNT_WRITE;
	unsigned long long int	  BTSEARCH;
	unsigned long long int	  BTUPDATE;
	unsigned long long int	  BTINSERT;
	unsigned long long int 	  BTDELETE;
	/////////////////////////////////////////////

	IntIntIntBTNode 	bt_root;	// Root node

	// Constructor
	IIIBTree(const char fname[100], unsigned int mblocks, 
		unsigned int hashsize, unsigned int incr, StorageNode *sn);  
	

	// Destructor, writes root node and closes the index file
	~IIIBTree();

	// Initialize the index file
	void initialize(void);

	void searchRange(unsigned int l, unsigned int *key,
		unsigned int *val, unsigned int *val1, 
		IntIntIntBTNode *node, unsigned int nodeNo);
	// Search for a value in the tree under node
	void searchVal(unsigned int key, unsigned int *resNodeNo, 
		int *pos, unsigned int *val, unsigned int *val1,
		IntIntIntBTNode *node, unsigned int nodeNo);

	// Insert a key and its address
	void insertVal(unsigned int key, unsigned int val, unsigned int val1);

	void updateVal(unsigned int key, unsigned int val, unsigned int val1,
		IntIntIntBTNode *node, unsigned int nodeNo);
	// Update the value of an exisiting key in the node at the 
	// specified index position
	void updateVal(unsigned int key, unsigned int nodeNo, 
		unsigned int pos, unsigned int val, unsigned int val1);

	// Delete a value from the tree under node x
	int deleteVal(unsigned int key, unsigned int val, unsigned int val1, 
		IntIntIntBTNode *x, unsigned int xno);
	
	// Display the entire btree, under node x
	int display(IntIntIntBTNode *x, unsigned int xno, int level);

	// Display the one node of btree
	void displayNode(IntIntIntBTNode *x, unsigned int xno, int level);
	void enableCache(unsigned int size);
	void enableKeyValCache(unsigned int size);

	// Cache flush operation
	void flushCache(int disk);
	int countKeys(struct IntIntIntBTNode *x, int level);
	void displayStatistics(void);
	void enumKeys(IntIntIntBTNode *x, unsigned int xno, int level);
	int enumKeysVals(IntIntIntBTNode *x, int level, unsigned int key[], 
		unsigned int val[], unsigned int val1[], unsigned int ind);

	//int insertValWrap(unsigned int key, unsigned int val, unsigned long long int rtime, unsigned long long int wtime);
	//int updateValWrap(unsigned int key, unsigned int val, unsigned long long int rtime, unsigned long long int wtime);
	//int searchValWrap(unsigned int key, unsigned int *val, unsigned long long int rtime, unsigned long long int wtime);
	int insertValWrap(unsigned int key, unsigned int val, unsigned int val1);
	int updateValWrap(unsigned int key, unsigned int val, unsigned int val1);
	int searchValWrap(unsigned int key, unsigned int *val, unsigned int *val1);
	void searchRangeWrap(unsigned int l, unsigned int *key,
		unsigned int *val, unsigned int *val1);
	int deleteValWrap(unsigned int key);
	void flushValWrap(void);
};

#endif
