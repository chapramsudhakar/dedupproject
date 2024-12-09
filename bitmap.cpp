#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>

using namespace std;

#include "bitmap.h"

void BitMap::initializeFreeList(void) {
	int i;
	struct fblist *f1;

	head = tail = NULL;
	f1 = fl = &flist[0];

	f1->prev = NULL;
	for (i=1; i<FLISTLEN; i++) {
		f1->next = &flist[i];
		flist[i].prev = f1;
		f1 = &flist[i];
	}
	f1->next = NULL;
}

unsigned int BitMap::getVals(unsigned int n) {
	unsigned int v = 0;
	struct fblist *f1;

	f1 = head;
	while (f1 != NULL) {
		if (f1->count >= n) {
			v = f1->fbstart;
			f1->count -= n;
			f1->fbstart += n;
			if (f1->count == 0) {
				// remove f1
				if (f1->prev == NULL)
					head = f1->next;
				else (f1->prev)->next = f1->next;

				if (f1->next == NULL)
					tail = f1->prev;
				else (f1->next)->prev = f1->prev;

				f1->prev = NULL;
				f1->next = fl;

				if (fl != NULL) fl->prev = f1;
				fl = f1;
			}
			break;
		}
		else f1 = f1->next;
	}

	return v;
}

int BitMap::freeVals(unsigned int n) {
	struct fblist *f1, *f2, *f3;

	if (head == NULL) {
		f1 = fl;
		fl = fl->next;
		if (fl != NULL)
			fl->prev = NULL;

		f1->fbstart = n;
		f1->count = 1;
		f1->prev = f1->next = NULL;
		head = tail = f1;
		return 1;
	}
	else {

		f1 = head;
		f2 = NULL;

		while ((f1 != NULL) && ((f1->fbstart + f1->count) <= n)) {
			f2 = f1;
			f1 = f1->next;
		}

		if (f2 == NULL) {
			if ((n+1) == f1->fbstart) {
				f1->fbstart = n;
				f1->count++;
				return 1;
			}
			else {
				if (fl != NULL) {
					// If free node exists
					f3 = fl;
					fl = fl->next;
					if (fl != NULL)
						fl->prev = NULL;

					f3->fbstart = n;
					f3->count = 1;
					f3->prev = NULL;
					f3->next = f1;
					f1->prev = f3;
					head = f3;
					return 1;
				}
				else return 0; // Otherwise ignore
			}
		}
		else {
			if (f1 == NULL) {
				if ((f2->fbstart + f2->count) == n) {
					f2->count++;
					return 1;
				}
				else {
					if (fl != NULL) {
						// If free node exists
						f3 = fl;
						fl = fl->next;
						if (fl != NULL)
							fl->prev = NULL;

						f3->fbstart = n;
						f3->count = 1;
						f3->prev = f2;
						f3->next = NULL;
						f2->next = f3;
						tail = f3;
						return 1;
					}
					else return 0; // Otherwise ignore
				}
			}
			else {
				if ((f2->fbstart + f2->count) == n) {
					f2->count++;
					if ((f2->fbstart + f2->count) == f1->fbstart) {
						f2->count += f1->count;
						// Remove f1
						f2->next = f1->next;
						if (f1->next != NULL) 
							(f1->next)->prev = f2;
						else tail = f2;

						f1->prev = NULL;
						f1->next = fl;
						if (fl != NULL)
							fl->prev = f1;
						fl = f1;
					}
					return 1;
				}
				else if ((n+1) == f1->fbstart) {
					f1->fbstart = n;
					f1->count++;
					return 1;
				}
				else {
					if (fl != NULL) {
						// If free node exists
						f3 = fl;
						fl = fl->next;
						if (fl != NULL)
							fl->prev = NULL;

						f3->fbstart = n;
						f3->count = 1;
						f3->prev = f2;
						f3->next = f1;
						f2->next = f3;
						f1->prev = f3;
						return 1;
					}
					else return 0; // Otherwise ignore
				}
			}
		}
	}
}

// Initialize bitmap
void BitMap::initializeBitMap()  {
	ofstream fs;
	unsigned char map[128];
	int i;

	fs.open(bmfile, ios::out | ios::binary | ios::trunc);
	if (fs.fail()) {
		cout << "Bitmap file opening failure\n";
		exit(0);
	}
	fs.clear();

	// Write Bit Map Header
	bm_header.maxBlocks = MAXBLOCKS;	// Defined in dedupn.h
	bm_header.freeBlockCount = MAXBLOCKS;
	fs.write((char *)&bm_header, sizeof(struct bm));
		
	// Prepare and write bitmap with all 1's
	for (i=0; i<128; i++)
		map[i] = 0xFF;	

	for (i=0; i<MAXBLOCKS/1024; i++)
		fs.write((char *)map, sizeof(map));

	fs.close();
	cout << "Bitmap for " << MAXBLOCKS << " blocks is created in " << bmfile << endl;
}

// Initial opening of the bitmap
//BitMap::BitMap(char *bname, unsigned long long int offset) {
BitMap::BitMap(char *bname) {
	strcpy(bmfile, bname);
	//pbaoffset = offset;
	
	bmlen = (MAXBLOCKS/8);
	pos = 0;
	bmfs.open(bmfile, ios::in | ios::out | ios::binary);

	if (bmfs.fail()) {
		cout << "Bitmap file opening failure\n";
		exit(0);
	}
	bmfs.clear();

	bmfs.read((char *)&bm_header, sizeof(struct bm));
	getBitMap();

	initializeFreeList();
}

// Update and close the bitmap file
void BitMap::closeBitMap() {
	updateBitMap();
	bmfs.close();
	bmfs.clear();
}

// Get the present bitmap details
int BitMap::getBitMap() {
	// Starting and ending block number of the bitmap	
	from = 1;
	to = 1024 * 1024;

	if (to > MAXBLOCKS) to = MAXBLOCKS;

	bmfs.seekg(sizeof(struct bm));
	bmfs.read((char *)bitmap, sizeof(bitmap));

	// Return the successfully read byte count
	return bmfs.gcount();
}

// Get next part of the bitmap
int BitMap::getNextBitMap() {
	int size;

	// Starting and ending block number of the bitmap	
	from = to + 1;
	to = to + 1024 * 1024;

	if (to > MAXBLOCKS) to = MAXBLOCKS;
	if (from >= to) return -1; // Reached the end of the bitmap

	size = (to - from + 1) / 8;
	bmfs.seekg(sizeof(struct bm) + ((unsigned long)(from - 1) / 8));
	bmfs.read((char *)bitmap, size);

	// Return the successfully read byte count
	return bmfs.gcount();
}

// Update Bit Map
void BitMap::updateBitMap() {
	bmfs.seekp(0);
	bmfs.write((char *)&bm_header, sizeof(struct bm));
	bmfs.seekp(sizeof(struct bm) + ((unsigned long)(from - 1) / 8));
	bmfs.write((char *)bitmap, (to - from + 1)/8);
}

// Return next bit number which is set to one starting from p
// If not found, returns -1.
int BitMap::findNextBitSet(int p) {
	unsigned int i, j, done = 0;
	int next = -1;

	i = p / 8;
	j = p % 8;
	if (i >= ((to - from + 1)/8)) return -1;
	while (!done) {
		if ((bitmap[i] & (0x1 << (8-j-1))) != 0) {
			next = i * 8 + j;
			done = 1;
		}
		else j++;

		if (j >= 8) {
			i++; j = 0;

			// Reached the end of the bitmap?
			if (i >= ((to - from + 1) / 8)) {
				next = -1;
				done = 1;
			}
		}
	}

	return next;
}

//Allocates nb number of contiguous blocks of size BLOCKSIZE
unsigned long long int BitMap::allocBlocks(int nb) {
	//static unsigned int pos = 0;
	unsigned int bno;
	int done = 0, l;

	// Check from pos.
	if ((bno = getVals(nb)) > 0) return bno;


	bno = allocBlocks1(nb, pos);

	if (bno != 0) done = 1;

	while (done == 0) {
		// Get next bit map and continue the search
		updateBitMap();
		l = getNextBitMap();
		if (l < 0) break;	// Reached end
		pos = 0;
		bno = allocBlocks1(nb, pos);
		if (bno != 0) done = 1;
	}

	if (done == 0) {
		// Restart from the beginning of the bitmap
		getBitMap();
		pos = 0;

		while (done == 0) {
			bno = allocBlocks1(nb, pos);
			if (bno != 0) done = 1;
			else  {
				l = getNextBitMap();
				if (l < 0) break;
			}
		}
	}
		
			
	// Advance pos.
	if (bno != 0) { 
		pos += nb;
		//bno += pbaoffset;
	}

	//return (bno + pbaoffset);
	return bno;
}

//Allocates nb number of contiguous blocks of size BLOCKSIZE, 
// starting from pos position.
unsigned long long int BitMap::allocBlocks1(int nb, unsigned int pos) {
	unsigned int i;
	int j, k, l;
	int start, count; 

	// Allocation is not yet done
	start = findNextBitSet(pos);
	if (start < 0) return 0;

	i = (start+1) / 8;
	j = (start+1) % 8;
	count = 1;

	// Check for sufficient number of contiguous free blocks
	while ((count < nb) && (i < (to - from + 1)/8)) {
		if ((bitmap[i] & (0x1 << (8-j-1))) != 0) {
			j++;
			count++;
			if (j >= 8) {
				i++; j = 0;
			}
		}
		else {
			// Contiguously nb number of 
			// blocks not found from start
			// Find next free block position
			pos = i * 8 + j + 1;
			start = findNextBitSet(pos);

			if (start < 0) return 0;

			i = (start+1) / 8;
			j = (start+1) % 8;
			count = 1;
		}
	}

	// Found the nb number of blocks?
	if (count == nb) {
		// Mark the bits as allocated
		l = start / 8;
		j = start % 8;
		for (k=0; k<count; k++) {
			bitmap[l] = bitmap[l] & (~(0x1 << (8-j-1)));
			j++;
			if (j >= 8) {
				l++; j = 0;
			}
		}
		bm_header.freeBlockCount -= nb;
	}
	else return 0;

	// Return starting block number
	return (start + from);
}

// Set a specific block as allocated one
void BitMap::setBlock(unsigned int b) {
	int i, j; //, k;
	int size;

	if ((b < from) || (b > to)) {
		// Block number is not in the present bitmap segment.
		// Write the present bitmap segment and get the 
		// new bitmap segment
		updateBitMap();

		// Find the lower 1 MB boundary block number
		from = (((b - 1) / (1024 * 1024)) * (1024 * 1024)) + 1;
		to = from + 1024 * 1024 - 1;

		if (to > MAXBLOCKS) to = MAXBLOCKS;

		size = (to - from + 1) / 8;
		bmfs.seekg(sizeof(struct bm) + ((unsigned long)(from - 1) / 8));
		bmfs.read((char *)bitmap, size);
	}
	
	// Mark the bit as free
	i = (b-from) / 8;
	j = (b-from) % 8;
	bitmap[i] = bitmap[i] & (~(0x1 << (8-j-1)));
	bm_header.freeBlockCount--;
}

// Free a block b
void BitMap::freeBlock(unsigned long long int b) {
	int i, j; 
	int size;
	unsigned char fbmp[128];
	unsigned int ffrom, tto;

	//b = b - pbaoffset;

	if (freeVals(b) > 0) return;

	if ((b < from) || (b > to)) {
		// Block number is not in the present bitmap segment.
		// Write the present bitmap segment and get the 
		// new bitmap segment

		// Find the lower 1 MB boundary block number
		ffrom = (((b - 1) / 1024) * (1024)) + 1;
		tto = ffrom + 1024 - 1;

		if (tto > MAXBLOCKS) tto = MAXBLOCKS;

		size = (tto - ffrom + 1) / 8;
		bmfs.seekg(sizeof(struct bm) + ((unsigned long)(ffrom - 1) / 8));
		bmfs.read((char *)fbmp, size);
		i = (b-ffrom) / 8;
		j = (b-ffrom) % 8;
		fbmp[i] = fbmp[i] | (0x1 << (8-j-1));
		bmfs.seekp(sizeof(struct bm) + ((unsigned long)(ffrom - 1) / 8));
		bmfs.write((char *)fbmp, size);

		bm_header.freeBlockCount++;
	}
	else {
		// Mark the bit as free
		i = (b-from) / 8;
		j = (b-from) % 8;
		bitmap[i] = bitmap[i] | (0x1 << (8-j-1));
		bm_header.freeBlockCount++;
	}
}

void BitMap::displayBitMapStatistics(void) {
	cout << "Bitmap : " << bm_header.freeBlockCount << " / " 
		<< bm_header.maxBlocks << " from : " << from 
		<< " to : " << to << endl;
}
