#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 

#define RELATION_SIZE 100 

char **readFile(char *filename, int column){
    //YOur code goes here 

    //TODO: Create dynamic relation instead of static. 
    //Try to allocate space for relation(table)
    size_t relationSize = 100;    
    char **relation = (char **)malloc(relationSize * sizeof(char *)); 

    //If not successful 
    if(relation == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    //Try to OPen read only file
    FILE *f = fopen(filename, "r");

    //If cannot open file
    if(f==NULL){
        fprintf(stderr, "Cannot open file\n");
        exit(EXIT_FAILURE);
    }

    char *line = NULL;
    size_t lineSize = 1024;

    line  = (char *)malloc(lineSize * sizeof(char));

    if (line == NULL ){
        fprintf(stderr, "Unable to allocate memory\n");
        exit(EXIT_FAILURE);
    }

    //Read each line from file. 
    unsigned int index = 0; 
    while(getline(&line, &lineSize, f) != -1){

        //Add line to relation (table)
        relation[index] = line; 
        index +=1; 
    }
    
    relation[index] = NULL; //Mark end of relation. 

    //FREE LINE
    //free(line); 

    //Close file. 
    //close(f);
    return relation; 
}


char *splitLine(char *line, const char *delim){

    int index = 1; 
    int i= 0;
    char *joinColomn, *found;

    //Split line
    while( (found = strsep(&line, delim)) != NULL){

        //Stop if reached the join column. 
        if (i==index){
            joinColomn = found;
            break; 
        }
        i ++;
    }

    return joinColomn;
}

void deleteFile(char *filename){
    //YOur code goes here...
}


#endif 