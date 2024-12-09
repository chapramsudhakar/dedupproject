#include <string.h>
#include "freelist.h"

FreeList::FreeList(const char fname[], int n) {
	strcpy(name, fname);
	size = n;

	fvals = new unsigned int[n];
	head = tail = count = 0;
}

unsigned int FreeList::getVal(void){
	unsigned int v;

	if (count > 0) {
		v = fvals[head];
		head = (head + 1) % size;
		count--;
	}
	else v = 0;

	return v;
}

int FreeList::freeVal(unsigned int v){
	if (count == size) // full
		return 0;

	fvals[tail] = v;
	tail = (tail + 1) % size;
	count++;
	return 1;
}


