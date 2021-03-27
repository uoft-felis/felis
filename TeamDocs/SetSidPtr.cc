#include <iostream>
 
using namespace std;
 
int var[10]; // Miniheap
int  *ptr = &var[0]; // Ptr1

int* getPtr() {
    int **lPtr = &ptr;
    return *lPtr;
}

void setPtr(int *newPtr) {
    int **lPtr = &ptr;
    *lPtr = newPtr;
}

int main () {
   var[0] = 3000;
   var[1] = 15;
   var[2] = 4654;
   var[9] = 9865;
   cout << *getPtr() << endl; 
   setPtr(ptr + 1); 
   cout << *getPtr() << endl; 

   return 0;
}

