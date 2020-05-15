#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 
#include <time.h> 

/*User define header files.*/
#include "./fileManager.h"
#include "./bloomFilter.h"

int numOfJoinTuples = 0;
const char* delim = "|"; 

char *lineFromR2WithoutJoinColumn(char *line, int index, const char *delim);
char ** equiJoin(char **table_1, int table_1_ln, int joinColPos, const char* delim);

int main(int argc, char *argv[]){
    /*
    Main program logic goes here...
    */

    //Get arguments 
    if(argc != 4){
        fprintf(stderr, "Usage: program file1.txt file2.txt 2\n");
        exit(EXIT_FAILURE);
    }


    char *r1_name = argv[1]; /*Name of file one.*/
    char *r2_name = argv[2]; /*Name of file two.*/
    int joinColPos = atoi(argv[3]) + 1; /*Position of join column.*/
    size_t r1_length = 0; /*Number of rows in table 1*/
    size_t r2_length = 0;  /*Number of rows in table 2*/
    unsigned int tupleSize = 0;
    char **r1 = NULL;
    char **r2 = NULL;
    int numTuples = 0;
    time_t w_time; 

    for(unsigned int i=0; i < MAX_BLOOM_FILTER; i++){
        bloomFilter[i] = 0; 
    }

    w_time = clock();

    //Read file one into table
    r1 = readFile(r1_name, joinColPos);
    r1_length = num_rows;

    for(unsigned int i=0; i < r1_length; i++){
        char line_1[1024];
        strcpy(line_1, r1[i]);
        char *joinColumn = splitLine(line_1, joinColPos, delim);

        addKey(joinColumn, bloomFilter);
    }

    //Read file two into table
    r2 = readFile(r2_name, joinColPos);
    r2_length = num_rows;

            
    //Collect tuples likely to be part of the join. 
    char **tuples = (char **)malloc(r2_length * sizeof(char *));
    if(tuples == NULL){
        fprintf(stderr, "Cannot allocate memory\n");
        exit(EXIT_FAILURE);
    }

    unsigned int index = 0;
    for(unsigned int i=0; i < r2_length; i ++){
        char line[1024];
        strcpy(line, r2[i]);

        char *joinColumn = splitLine(line, joinColPos, delim);

        //Hash join column values using the three
        //hash functions and  check whether the 
        //corresponding positions in the bloom filter
        //are already set to 1s.  
        if(keyExist(joinColumn, bloomFilter) ==1){
            int lineSIze = strlen(r2[i]);
            *(tuples + index) = malloc((lineSIze + 1) * sizeof(char));
            strcpy(*(tuples + index), r2[i]);
            index ++;
        }

    }
    numTuples = index;

    //Put in hashtable for easy access
    for(unsigned int k=0; k < numTuples; k++){
        //Add items to hash table 
        char cp_line[1024];
        strcpy(cp_line, tuples[k]);

        char *joinColumn = splitLine(cp_line, joinColPos, delim);
        addItem(joinColumn, tuples[k]); 

    }

    char **results = equiJoin(r1, r1_length, joinColPos, delim);

    //Write results to file (R3.txt)
    char resultsFile[] = "Serial_R3.txt";

    writeIntoFile(results, numOfJoinTuples, resultsFile);   

           
    //free the array of strings 
    for (size_t i=0; i< r1_length; i++){
        free(*(r1 + i));
    }

    free(r1);

    //free array of strings 
    for(size_t j=0; j< numOfJoinTuples; j++){
        free(*(results + j));
    }
    free(results);

    //free array of strings 
    for(size_t i=0; i< r2_length; i++){
        free(*(r2 + i));
    }
    free(r2);

    //free array of strings
    for(size_t  j=0; j < numTuples; j++){
        free(*(tuples +j));
    }

    free(tuples);

    //free hashtable
    releaseHashTable();

    w_time = clock() - w_time; 

    printf("Serial Program elapsed time: %f\n",((double)w_time)/CLOCKS_PER_SEC);
    return 0;

}


char *lineFromR2WithoutJoinColumn(char *line, int index, const char *delim)
{

    int i= 0;
    char *found;
    //Try to allocate space for relation(table)
    char *lineToJoin = (char *)malloc(1024 * sizeof(char)); 

    //If not successful 
    if(lineToJoin == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    strcpy(lineToJoin, "");

    //Split line
    while( (found = strsep(&line, delim)) != NULL){
      if(index != i){
          strcat(lineToJoin, delim);
          strcat(lineToJoin, found);
      }
        i ++; 
    }
    //strcat(lineToJoin, "\n");
    return lineToJoin;
}


char ** equiJoin(char **table_1, int table_1_ln, int joinColPos, const char* delim){
    
    //Try to allocate space for relation(table)
    size_t index = 0;
    size_t resultsSize = 100;    
    char **results = (char **)malloc(resultsSize * sizeof(char *)); 

    //If not successful 
    if(results == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }
    
    for(unsigned int i=0; i < table_1_ln; i++){

        char line[1024];
        strcpy(line, table_1[i]);
        
        char *joinColumn = splitLine(line, joinColPos, delim);

        int numOfItems = numOfItemsInBucket(joinColumn);

        //if there items
        if (numOfItems != 0){
            item **joinLines = findItemsInBucket(joinColumn);
            
            
            for(unsigned int j=0; j< numOfItems; j++){

                //Get start and end indexes of join column in line
                char line_copy[1024];
                strcpy(line_copy, joinLines[j]->line);

                char *lineToJoin = lineFromR2WithoutJoinColumn(
                               line_copy, joinColPos, delim);

                //Reallocate memory
                if (index >= resultsSize){
                    //increase number of rows and realloc 
                    resultsSize +=resultsSize; 

                    char **temp = realloc(results, resultsSize*sizeof(char *));
                    if (temp == NULL ){
                        fprintf(stderr, "Unable to allocate memory\n");
                        exit(EXIT_FAILURE);
                    }

                    results = temp; 
                }
                
                //join lines from R1 and R2
                char result[1024];
                table_1[i][strlen(table_1[i])-1] = '\0';
                strcpy(result, table_1[i]);

                lineToJoin[strlen(lineToJoin)-1] = '\0';
                strcat(result, lineToJoin);
                strcat(result, "\n");

                free(lineToJoin);
                
                *(results + index)  = malloc(1024 * sizeof(char));

                //copy line into 
                strcpy(*(results + index), result);

                index ++;

            }
        } 
    } 
    numOfJoinTuples = index; 

    return results;
}
