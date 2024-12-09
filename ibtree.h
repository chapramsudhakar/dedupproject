#ifndef __IBTREE_H__
#define __IBTREE_H__

using namespace std;
#include "freelist.h"

#include "darc.h"
#include "lru.h"

#define IMINKEYS	20
#define IMAXKEYS 	(2*IMINKEYS+1)	// Size of the node will be 000 ~ 512
#define IMINDEGREE	(IMINKEYS+1)	// Minimum degree
 
struct INTBTNode {
	unsigned int 	btn_key[IMAXKEYS]; 	// Keys
	unsigned int	btn_value[IMAXKEYS];	// matching block addresses
	unsigned int	btn_ptrs[IMAXKEYS+1];
	unsigned int    btn_ucount;
	// First node's btn_ucount contains total number of
	// used nodes in the entire index file.
	// Ponters to btree nodes (numbers)
	unsigned short  btn_used;       // Is this used node or free node
	unsigned short  btn_leaf;       // Is this leaf node
	unsigned short  btn_count;      // Count of keys
	unsigned char   btn_padding[4];	//round to 512 bytes
};

struct IBTVal {
	unsigned int 	key;
	unsigned int 	val;
	struct IBTVal 	*next, *prev;
	struct IBTVal	*tnext, *tprev;
	unsigned short	flags;
};

class StorageNode;

class IBTree {
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
	struct IBTVal	*FL;
	unsigned int	KVCacheSize;
	unsigned int 	KVHASH;
	struct IBTVal	**KVQ;
	struct IBTVal	*KVTail;
	struct IBTVal	*KVHead;

	unsigned int nextFreeIBTreeNode;
	FreeList *flist;

	int nodeCount;
	int maxlevel;
	StorageNode	*bt_sn;

	// Free node by writing zero bytes in the node
	void freeNode(unsigned int n);

	// Find a one free (unused) node
	unsigned int findFreeNode(void);
	
	// Split the child node into two nodes
	void splitChild(INTBTNode *par, unsigned int parNo,
			int i, INTBTNode *child, unsigned int childNo);

	// Insert a key and its address in a non-full Node
	void insertNonFull(INTBTNode *b, unsigned int bNo, 
			unsigned int key, unsigned int val);

	// Left shift value and lba elements position 'from' to 'to' 
	void leftShiftVal(INTBTNode *b, int from, int to, int count);

	// Left shift ptrs position 'from' to 'to' 
	void leftShiftPtrs(INTBTNode *b, int from, int to, int count);

	// right shift value and lba elements position 'from' to 'to'
	void rightShiftVal(INTBTNode *b, int from, int to, int count);
	
	// right shift ptrs position 'from' to 'to'
	void rightShiftPtrs(INTBTNode *b, int from, int to, int count);

	// Copy ptrs from b1 to b2 from position 'from' to 'to'
	void copyPtrs(INTBTNode *b1, INTBTNode *b2, int from, int to, int count);

	// Copy val and lba elements from b1 to b2 from position 'from' to 'to'
	void copyVal(INTBTNode *b1, INTBTNode *b2, int from, int to, int count);

	// Modified root/root of a subtree, whose child is y, is 
	// to be written with proper checking.
	void writeModifiedRoot(INTBTNode *x, unsigned xno, 
		INTBTNode *y, unsigned int * yno);

	void findMin(unsigned int *key, unsigned int *value, INTBTNode *x, unsigned int xno);

	void findMax(unsigned int *key, unsigned int *value, INTBTNode *x, unsigned int xno);
	int deleteVal1(unsigned int key, unsigned int val, INTBTNode *x, unsigned int xno);
	// Read a node from the index file
	int readNode(unsigned int n, INTBTNode *b);

	// Write a node into the index file
	int writeNode(unsigned int n, INTBTNode *b);

	// Returns the block if found in the cache otherwise read and add the
	// node to cache 
	INTBTNode * getNode(unsigned int n);

	// Writes the block, into cached block, 
	// otherwise creates a new cached copy
	int putNode(unsigned int n, INTBTNode *b);

	// Releases the block, if modified then set the dirty flag
	void releaseNode(unsigned int n, unsigned short dirty);
	void kvdelete(unsigned int i, IBTVal *c);
	void kvprepend(unsigned int i, IBTVal *c);
	IBTVal * kvsearch(unsigned int i, unsigned int key);
	IBTVal * kvalloc(void);

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

	INTBTNode 	bt_root;	// Root node

	// Constructor
	IBTree(const char fname[100], unsigned int mblocks, unsigned int hashsize, unsigned int incr, StorageNode *sn);  
	

	// Destructor, writes root node and closes the index file
	~IBTree();

	// Initialize the index file
	void initialize(void);

	void searchRange(unsigned int l, unsigned int r, unsigned int *key,
		unsigned int *val, INTBTNode *node, unsigned int nodeNo);
	// Search for a value in the tree under node
	void searchVal(unsigned int key, unsigned int *resNodeNo, 
			int *pos, unsigned int *val, 
			INTBTNode *node, unsigned int nodeNo);

	// Insert a key and its address
	void insertVal(unsigned int key, unsigned int val);

	void updateVal(unsigned int key, unsigned int val, INTBTNode *node, unsigned int nodeNo);
	// Update the value of an exisiting key in the node at the 
	// specified index position
	void updateVal(unsigned int key, unsigned int nodeNo, unsigned int pos, unsigned int val);

	// Delete a value from the tree under node x
	int deleteVal(unsigned int key, unsigned int val, INTBTNode *x, unsigned int xno);
	
	// Display the entire btree, under node x
	int display(INTBTNode *x, unsigned int xno, int level);

	// Display the one node of btree
	void displayNode(INTBTNode *x, unsigned int xno, int level);
	void enableCache(unsigned int size);
	void enableKeyValCache(unsigned int size);

	// Cache flush operation
	void flushCache(int disk);
	int countKeys(struct INTBTNode *x, int level);
	void displayStatistics(void);
	void enumKeys(INTBTNode *x, unsigned int xno, int level);
	int enumKeysVals(INTBTNode *x, int level, unsigned int key[], unsigned int val[], unsigned int ind);

	int insertValWrap(unsigned int key, unsigned int val);
	int updateValWrap(unsigned int key, unsigned int val);
	int searchValWrap(unsigned int key, unsigned int *val);
	void searchRangeWrap(unsigned int l, unsigned int r, unsigned int *key,
		unsigned int *val);
	int deleteValWrap(unsigned int key);
	void flushValWrap(void);
};

#endif
