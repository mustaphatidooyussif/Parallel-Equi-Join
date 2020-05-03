#include <stdlib.h> 
#include <stdio.h> 

#include <string.h> 
char *splitLine(char *line, char *delim);

int main(){
    char *found; 
    char *string = strdup("Hello|world|mustapha");
    
    found = splitLine(string, "|");
    printf("%s\n", found);

    return 0;
}

char *splitLine(char *line, char *delim){

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