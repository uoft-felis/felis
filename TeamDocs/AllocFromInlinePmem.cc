#include <stdlib.h>
#include <iostream>
#include <assert.h>

// To run: g++ -std=c++0x AllocFromInlinePmem.cc
// ./a.out

using namespace std;
#define VHANDLE_METDATA 64
#define INLINE_VERSION_ARRAY 32
#define INLINE_MINIHEAP_MASK1 1
#define INLINE_MINIHEAP_MASK2 1
#define INLINE_MINIHEAP 158
#define VHANDLE_TOTAL (VHANDLE_METDATA + INLINE_VERSION_ARRAY + \
        INLINE_MINIHEAP_MASK1 + INLINE_MINIHEAP_MASK2 + \
        INLINE_MINIHEAP)

class Vhandle
{
    public:

    Vhandle() {
        char *mask1Ptr = (char *)this + (VHANDLE_METDATA + INLINE_VERSION_ARRAY); // Store Byte Offset
        char *mask2Ptr = mask1Ptr + INLINE_MINIHEAP_MASK1; // Store Byte Offset
        *mask1Ptr = 0;
        *mask2Ptr = 0;
    }

    /*
    1) Try offest [two 8-bit in mask space] | uses all bits for byte granularity
    2) Try 32 mask [one 8-bit in vhandle] | use 5-bits / wastes 3-bits for 32byte granularity
    3) Try 16 mask [two 8-bit in mask space] uses 10-bits / wastes 6-bits for 16byte granularity
    */
    char *AllocFromInlinePmem_offset(size_t sz) {
        printf("Size: %ld\n", sz);

        // Check requests size fits in miniheap
        if (sz > INLINE_MINIHEAP) return nullptr;

        // Mask Offset stores byte offset = [0 to 158]
        char *mask1Ptr = (char *)this + (VHANDLE_METDATA + INLINE_VERSION_ARRAY); // Store Byte Offset
        char *mask2Ptr = mask1Ptr + INLINE_MINIHEAP_MASK1; // Store Byte Offset
        char *startOfMiniHeap = mask2Ptr + INLINE_MINIHEAP_MASK2;

        printf("Mask1: %p | Mask2: %p | MiniHeap: %p\n", mask1Ptr, mask2Ptr, startOfMiniHeap); 
        printf("Mask1Val: %d | Mask2Val: %d\n", static_cast<unsigned char>(*mask1Ptr), static_cast<unsigned char>(*mask2Ptr)); 

        // Check to make sure miniheap not full - Very big last version stored
        int trackedSize = (*mask1Ptr == *mask2Ptr && *mask1Ptr == 0) ? 0 : static_cast<unsigned char>(*mask2Ptr) - static_cast<unsigned char>(*mask1Ptr) + 1;
        printf("TrackedSize: %d\n", trackedSize); 

        if (trackedSize >= INLINE_MINIHEAP) return nullptr;
        else if (trackedSize <= 0) {
            *mask1Ptr = 0;
            *mask2Ptr = sz - 1;
            // Return address start of new allocation
            printf("Init | Mask1Val: %d | Mask2Val: %d\n\n", static_cast<unsigned char>(*mask1Ptr), static_cast<unsigned char>(*mask2Ptr)); 
            return startOfMiniHeap + *mask1Ptr;
        }

        // find closest space for allocation
        int diffSpaceAtFront = (*mask1Ptr >= sz) ? *mask1Ptr - sz : -1; // Extra space between end of new allocation and start of last
        int diffSpaceAtEnd = INLINE_MINIHEAP - *mask2Ptr; // Space after end of last allocation
        diffSpaceAtEnd = (diffSpaceAtEnd >= sz) ? diffSpaceAtEnd - sz : -1; // Extra space after end of new allocation and end of miniheap 
        printf("diffSpaceAtFront: %d | diffSpaceAtEnd: %d\n", diffSpaceAtFront, diffSpaceAtEnd); 
        
        // Check if there was an error in above
        if (0 > diffSpaceAtEnd && 0 > diffSpaceAtFront) {
            // Error occured
            return nullptr;
        }

        // Check if space at front is closest in size - less wasted space
        bool addFront = false;
        if (0 > diffSpaceAtEnd) {
            addFront = true;
        }
        else if (0 > diffSpaceAtFront) {
            addFront = false;
        }
        else if (diffSpaceAtFront <= diffSpaceAtEnd) {
            addFront = true;
        }
        else {
            addFront = false;
        }

        // Track New Allocation
        if (addFront) {
            *mask1Ptr = 0;
            *mask2Ptr = sz - 1;
        }
        else {
            *mask1Ptr = *mask2Ptr + 1; // New start is taken from after last end
            *mask2Ptr = *mask1Ptr + sz - 1; 
        }

        // Return address start of new allocation
        printf("Front: %d | Mask1Val: %d | Mask2Val: %d\n\n", addFront, static_cast<unsigned char>(*mask1Ptr), static_cast<unsigned char>(*mask2Ptr)); 
        return startOfMiniHeap + *mask1Ptr;
    }
};

int main() {
    char *mem = (char *)malloc(VHANDLE_TOTAL);
    {
        Vhandle *vPtr = new (mem) Vhandle(); 
    
        // 158
        int *val1 = (int*) vPtr->AllocFromInlinePmem_offset(sizeof(int));
        *val1 = 56;
        // 154
        int *val2 = (int*) vPtr->AllocFromInlinePmem_offset(sizeof(int));
        *val2 = 64;
        // 150
        char *val3 = (char*) vPtr->AllocFromInlinePmem_offset(sizeof(char)*15);
        for (int i = 0; i < 15; ++i) {
            val3[i] = (char) i;
        }
        // 135
        char *val4 = (char*) vPtr->AllocFromInlinePmem_offset(sizeof(char)*64);
        // 71
        char *val5 = (char*) vPtr->AllocFromInlinePmem_offset(sizeof(char)*69);
        // 2
        char *val6 = (char*) vPtr->AllocFromInlinePmem_offset(sizeof(char)*5);
        val6 = (char*) vPtr->AllocFromInlinePmem_offset(sizeof(char)*2);
    }
    free(mem);
}
