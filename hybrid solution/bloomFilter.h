#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H
#define MAX_BLOOM_FILTER 100000

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 

//Create and initialize bloom filter to zeroes. 
int bloomFilter[MAX_BLOOM_FILTER]; 

/*
The implementation of murmur hash function
*/
int murmur(char *key){
    int index = 0;
    
    ///ND: DUmmpy implementation.

    for(unsigned int i=0; i < strlen(key); i++){
        index += (int)key[i];
    }

    //TODO: Your code goes here

    return (index + 1)% MAX_BLOOM_FILTER;
}

  
/*
The implementation of fnv has function.
*/
int fnv(char *key){
    int index = 0;
    
    ///ND: DUmmpy implementation.

    for(unsigned int i=0; i < strlen(key); i++){
        index += (int)key[i];
    }

    //TODO: Your code goes here

    return (index +  2)% MAX_BLOOM_FILTER;
}


/*
The implementation of hashMix hash function. 
*/
int hashMix(char *key){
    int index = 0;
    
    ///NB: DUmmpy implementation.

    for(unsigned int i=0; i < strlen(key); i++){
        index += (int)key[i];
    }

    return (index + 3)% MAX_BLOOM_FILTER;
}

void addKey(char *key, int bloom[]){

    //Hash key with murmur function. 
    int murmur_index = murmur(key);

    //Hash the key with fnv function. 
    int fnv_index = fnv(key);

    //Hash the key with hashmix function. 
    int hash_m_index = hashMix(key);

    //Set the hashed-positions of the bloom filter to 1.
    bloom[murmur_index] = 1;
    bloom[fnv_index] = 1;
    bloom[hash_m_index] = 1;  

}

int keyExist(char *key, int bloom[]){

    int exist = 0;

    //Hash key with murmur function. 
    int murmur_index = murmur(key);

    //Hash the key with fnv function. 
    int fnv_index = fnv(key);

    //Hash the key with hashmix function. 
    int hash_m_index = hashMix(key);

    /*If values at hashed-positions of bloom filters are 1s. 
    Set exist =1, and return 
    */
    if(bloom[murmur_index] == 1 
        && bloom[fnv_index] == 1 
        && bloom[hash_m_index] == 1){
            exist = 1;
        }


    return exist; 
}

#endif 