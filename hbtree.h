#ifndef __HBTREE_H__
#define __HBTREE_H__

using namespace std;

#include "freelist.h"

#include "lru.h"
#include "darc.h"

#define HMINKEYS		9
#define HMAXKEYS 	(2*HMINKEYS+1)	// Size of the node will be 000 ~ 512
#define HMINDEGREE	(HMINKEYS+1)	// Minimum degree
 
struct HashBTNode {
	unsigned char 	btn_key[HMAXKEYS][16]; // Keys
	unsigned int	btn_value[HMAXKEYS];     // matching block addresses
	unsigned int	btn_ptrs[HMAXKEYS+1];
	unsigned int    btn_ucount;
	// First node's btn_ucount contains total number of
	// used nodes in the entire index file.
	// Ponters to btree nodes (numbers)
	unsigned short  btn_used;       // Is this used node or free node
	unsigned short  btn_leaf;       // Is this leaf node
	unsigned short  btn_count;      // Count of keys
	unsigned char   btn_padding[40];//round to 512 bytes
};

struct HBTVal {
	unsigned char    key[16];
	unsigned int    val;
	struct HBTVal   *next, *prev;
	struct HBTVal   *tnext, *tprev;
	unsigned short  flags;
};

class StorageNode;
class HASHBTree {
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


	struct HBTVal   *FL;
	unsigned int    KVCacheSize;
	unsigned int	KVHASH;
	struct HBTVal   **KVQ;
	struct HBTVal   *KVTail;
	struct HBTVal   *KVHead;

	FreeList	*flist;
	unsigned int	nextFreeHBTreeNode;
	int nodeCount;
	int maxlevel;
	StorageNode	*bt_sn;

	// Free node by writing zero bytes in the node
	void freeNode(unsigned int n);

	// Find a one free (unused) node
	unsigned int findFreeNode(void);
	
	// Split the child node into two nodes
	void splitChild(struct HashBTNode *par, unsigned int parNo,
			int i, struct HashBTNode *child, unsigned int childNo);

	// Insert a key and its address in a non-full Node
	void insertNonFull(struct HashBTNode *b, unsigned int bNo, 
			unsigned char * key, unsigned int val);

	// Left shift value and lba elements position 'from' to 'to' 
	void leftShiftVal(struct HashBTNode *b, 
			int from, int to, int count);

	// Left shift ptrs position 'from' to 'to' 
	void leftShiftPtrs(struct HashBTNode *b, int from, int to, int count);

	// right shift value and lba elements position 'from' to 'to'
	void rightShiftVal(struct HashBTNode *b, int from, int to, int count);
	
	// right shift ptrs position 'from' to 'to'
	void rightShiftPtrs(struct HashBTNode *b, int from, int to, int count);

	// Copy ptrs from b1 to b2 from position 'from' to 'to'
	void copyPtrs(struct HashBTNode *b1, struct HashBTNode *b2, 
			int from, int to, int count);

	// Copy val and lba elements from b1 to b2 from position 'from' to 'to'
	void copyVal(struct HashBTNode *b1, struct HashBTNode *b2, 
			int from, int to, int count);

	// Modified root/root of a subtree, whose child is y, is 
	// to be written with proper checking.
	void writeModifiedRoot(struct HashBTNode *x, unsigned xno, 
		struct HashBTNode *y, unsigned int * yno);

	void findMin(unsigned char *key, unsigned int *value, HashBTNode *x, unsigned int xno);

	void findMax(unsigned char *key, unsigned int *value, HashBTNode *x, unsigned int xno);
	int deleteVal1(unsigned char * key, unsigned int val, struct HashBTNode *x, unsigned int xno);

	// Read a node from the index file
	int readNode(unsigned int n, struct HashBTNode *b);

	// Write a node into the index file
	int writeNode(unsigned int n, struct HashBTNode *b);

	// Returns the block if found in the cache otherwise read the node,
	// and add to cache 
	HashBTNode * getNode(unsigned int n);

	// Writes the block, into cached block,
	// otherwise creates a new cached copy
	int putNode(unsigned int n, HashBTNode *b);

	// Releases the block, if modified then set the dirty flag
	void releaseNode(unsigned int n, unsigned short dirty);

	void kvdelete(unsigned int i, HBTVal *c);
	void kvprepend(unsigned int i, HBTVal *c);
	HBTVal * kvsearch(unsigned int i, unsigned char *key);
	HBTVal * kvalloc(void);

public:
	unsigned long int keycount;
	////////////////////////////////////////////////////
	unsigned long long int	BT_BTNODE_READCOUNT;
	unsigned long long int	BT_BTNODE_READ_CACHE_HIT;
	unsigned long long int	BT_BTNODE_WRITECOUNT;
	unsigned long long int	BT_BTNODE_WRITE_CACHE_HIT;
	unsigned long long int	BT_BTNODE_EFFECTIVE_WRITES;
	unsigned long long int	BT_BTNODE_FLUSH_WRITES;

	unsigned long long int	BT_BTNODE_READCOUNT_READ;
	unsigned long long int	BT_BTNODE_WRITECOUNT_READ;
	unsigned long long int	BT_BTNODE_READCOUNT_WRITE;
	unsigned long long int	BT_BTNODE_WRITECOUNT_WRITE;
	unsigned long long int    BTSEARCH;
	unsigned long long int    BTUPDATE;
	unsigned long long int    BTINSERT;
	unsigned long long int    BTDELETE;

	///////////////////////////////////////////////////

	struct HashBTNode 	bt_root;	// Root node

	// Constructor
	HASHBTree(const char fname[100], unsigned int mblocks, unsigned int hashsize, unsigned int incr, StorageNode *sn);  
	

	// Destructor, writes root node and closes the index file
	~HASHBTree();

	// Initialize the index file
	void initialize(void);

	// Search for a value in the tree under node
	void searchVal(unsigned char * key, unsigned int *resNodeNo, 
			int *pos, unsigned int *val, 
			struct HashBTNode *node, unsigned int nodeNo);

	// Insert a key and its address
	void insertVal(unsigned char * key, unsigned int val);

	void updateVal(unsigned char *key, unsigned int val, struct HashBTNode * node, unsigned int nodeNo);

	// Update the value of an exisiting key in the node at the 
	// specified index position
	void updateVal(unsigned char *key, unsigned int nodeNo, unsigned int pos, unsigned int val);

	// Delete a value from the tree under node x
	int deleteVal(unsigned char * key, unsigned int val, struct HashBTNode *x, unsigned int xno);
	
	// Display the entire btree
	int display(struct HashBTNode *x, unsigned int xno, int level);

	// Display the one node of btree
	void displayNode(struct HashBTNode *x, unsigned int xno, int level);
	void enableCache(unsigned int size);
	void enableKeyValCache(unsigned int size);

	// Cache flush operation
	void flushCache(int disk);
	
	void enumKeys(HashBTNode *x, unsigned int xno, int level);
	int countKeys(struct HashBTNode *x, int level);
	void displayStatistics(void);

	// Wrapper routines
	int insertValWrap(unsigned char * key, unsigned int val);
	int updateValWrap(unsigned char * key, unsigned int val);
	int searchValWrap(unsigned char * key, unsigned int *val);
	int deleteValWrap(unsigned char * key);
	void flushValWrap(void);
};

#endif
