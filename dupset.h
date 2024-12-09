#ifndef __DUPSET_H__
#define __DUPSET_H__

using namespace std;

//#include "dedupconfig.h"
//#include "dedupn.h"

struct Vertex {
	unsigned int	lba;	// elements lba number
	unsigned int	par;	// Representative parent lba number
	struct Vertex	*csptr;	// Child or sibling pointer
	struct Vertex   *hnext; // Hash list pointers
	struct Vertex   *hprev; 
};


struct Vertex * addVertex(unsigned int lba, unsigned int par);
struct Vertex * findVertex(unsigned int lba);
void displayDupSet(void);

#endif
