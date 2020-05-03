#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H
#define MAX_BLOOM_FILTER 100000


//Create and initialize bloom filter to zeroes. 
int *bloom = calloc(MAX_BLOOM_FILTER, sizeof(int));

/*
The implementation of murmur hash function
*/
int murmur(char *key){
    int index = 0;
    
    //Your code goes here

    return index;
}

  
/*
The implementation of fnv has function.
*/
int fnv(char *key){
    int index = 0;

    //YOur code goes here.. 

    return index; 
}


/*
The implementation of hashMix hash function. 
*/
int hashMix(char *key){
    int index = 0;

    //YOur code goes here... 

    return index; 
}

void addKey(char *key){

    //Hash key with murmur function. 


    //Hash the key with fnv function. 

    
    //Hash the key with hashmix function. 


    //Set the hashed-positions of the bloom filter to 1. 

}

int keyExist(char *key){

    int exist = 0;

   //Hash key with murmur function. 


    //Hash the key with fnv function. 

    
    //Hash the key with hashmix function. 


    /*If values at hashed-positions of bloom filters are 1s. 
    Set exist =1, and return 
    */


    return exist; 
}

#endif 