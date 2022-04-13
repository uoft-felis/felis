#ifndef KEY_ENTRY_TYPES
#define KEY_ENTRY_TYPES

#include <stdio.h>
#include <stdlib.h>

struct entry_pair {
    uint64_t first;
    uint64_t second;

    entry_pair() {
        first = 0;
        second = 0;
    }

    entry_pair(uint64_t v1, uint64_t v2) {
        first = v1;
        second = v2;
    }

    // entry_pair operator<<(int shift) {
    //     entry_pair result = entry_pair(first << shift, second);
    //     return result;
    // }

    // entry_pair operator>>(int shift) {
    //     entry_pair result = entry_pair(first >> shift, second);
    //     return result;
    // }



};

#endif