#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 

#define RELATION_SIZE 100 

size_t num_rows = 0;

char **readFile(char *filename, int column){

    //Try to OPen read only file
    FILE *f = fopen(filename, "r");

    //If cannot open file
    if(f==NULL){
        fprintf(stderr, "Cannot open file\n");
        exit(EXIT_FAILURE);
    }

    //For reading each line from file. 

    //Read each line from file. 
    char *lineBuf = NULL;
    size_t n = 0;
    ssize_t lineLength = 0;
    size_t index = 0; 
    size_t rowChunk = 10;

    //Try to allocate space for relation(table)
    char **relation = (char **)malloc(rowChunk * sizeof(char *)); 

    //If not successful 
    if(relation == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    while((lineLength = getline(&lineBuf, &n, f)) != -1){

        if (index >= rowChunk){
            //increase number of rows and realloc 
            rowChunk +=rowChunk; 

            char **temp = realloc(relation, rowChunk*sizeof(char *));
            if (temp == NULL ){
                fprintf(stderr, "Unable to allocate memory\n");
                exit(EXIT_FAILURE);
            }

            relation = temp; 
        }

        lineBuf[strcspn(lineBuf, "\n")] = 0;
            
        //Allocate space on the heap for line
         *(relation + index)  = malloc((lineLength + 1) * sizeof(char));

        //copy line into 
        strcpy(*(relation + index), lineBuf);

        index ++; 
    }
    
    //relation[index] = NULL; //Mark end of relation. 
    num_rows = index; //set the number of rows in table

    //free linebuf utilized by `getline()`
    free(lineBuf);
    //Close file. 
    fclose(f);
    return relation; 
}


char *splitLine(char *line, int index, const char *delim){

    int i= 0;
    char *joinColomn, *found;

    //Split line
    while( (found = strsep(&line, delim)) != NULL){

        //Stop if reached the join column. 
        if (i == index){
            joinColomn = found;
            break; 
        }
        i ++;
    }
    return joinColomn;
}

void deleteFile(char *filename){
    if(remove(filename) ==0 ){
        printf("Removing file %s...\n", filename);
    }else{
        printf("Error removing file.\n");
    }
}


void writeIntoFile(char **results, char *filename){

    deleteFile(filename);

    FILE *f = fopen(filename, "wb");

    //If cannot open file
    if(f==NULL){
        fprintf(stderr, "Cannot open file\n");
        exit(EXIT_FAILURE);
    }

    int written = 0;
    written = fwrite(&results, sizeof(char), sizeof(results), f);
    if(written ==0){
        fprintf(stderr, "Error while writing to file\n");
        exit(EXIT_FAILURE);
    }

    fclose(f);
}

#endif 