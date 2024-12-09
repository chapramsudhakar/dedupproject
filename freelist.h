#ifndef __FREELIST_H__
#define __FREELIST_H__

using namespace std;
//#include "dedupconfig.h"
//#include "dedupn.h"


class FreeList {
private:
	unsigned int	*fvals;
	int		size;
	int		head;
	int		tail;
	int		count;
	char		name[30];

public:
	FreeList(const char fname[], int n);
	unsigned int getVal(void);
	int freeVal(unsigned int v);
};
	
#endif
