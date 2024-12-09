
#ifndef __BUCKCOMN_H__
#define __BUCKCOMN_H__

using namespace std;

struct pbaList {
        unsigned int pba;
        unsigned char fp[16];
        unsigned int refcount;
        unsigned short flags;
        struct pbaList *next;
};

struct BKTVal {
	unsigned int	key;
	struct pbaList	*val;
	struct BKTVal	*next, *prev;
	struct BKTVal	*tnext, *tprev;
};

struct pbaList * insertPlNode(struct pbaList *lh, struct pbaList *node);

#endif




