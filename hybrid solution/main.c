#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 

/*Hbrid programming models.*/
#include <omp.h> 
#include "mpi.h"

/*User define header files.*/
#include "./fileManager.h"
#include "./bloomFilter.h"
#include "./queries.h"

#define RELATION_ONE 100
#define RELATION_TWO 100

const char* delim = "|"; 

int main(int argc, char *argv[]){
    /*
    Main program logic goes here...
    */

    //Get arguments 
    if(argc != 4){
        fprintf(stderr, "Usage: program file1.txt file2.txt 2\n");
        exit(EXIT_FAILURE);
    }

    int comm_size; /*Number of processes*/
    int my_rank; /*Process id number*/ 
    char *r1_name = argv[1]; /*Name of file one.*/
    char *r2_name = argv[2]; /*Name of file two.*/
    int joinColPos = atoi(argv[3]); /*Position of join column.*/
    int r1_length = 0; /*Number of rows in table 1*/
    int r2_length = 0;  /*Number of rows in table 2*/
    int tupleSize = 100;
    char *tuples[tupleSize];

    //TODO: Parallize this section. 
    //Initialize bloom filter to zeros. 
    size_t bloomFilterSize = sizeof(bloom)/sizeof(int);
    for(unsigned int i=0; i < bloomFilterSize; i++){
        bloom[i] = 0; 
    }



    /*Initialize the MPI environment*/
    MPI_Init(&argc, &argv);

    /**Get the number of processes*/
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size); 

    /*Get the rank of the process*/
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /*Can only be done by process number 0 (master process)*/
    if(my_rank == 0){
        //Read file one into a relation
        char **r1 = readFile(r1_name, joinColPos);
        pritf("%d\n", sizeoff(r1)/sizeof(*r1));

        //Read file two into a relation
        char **r2 = readFile(r2_name, joinColPos);

        //MPI_Send(r1, r1_length, MPI_CHAR, 1, 0, MPI_COMM_WORLD);
        //MPI_Send(r2, r2_length, MPI_CHAR, 2, 0, MPI_COMM_WORLD);
    }else{

        //Process one try to receive 
        if (rank == 1){

            //Receive table from process 0
            MPI_Recv(r1, r1_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //Hash join column and send bloom filter to node 2. 
            char *joinColumn;

            for(unsigned int j = 0; j < r1_length; j++){
                //Get the join column 
                joinColumn = splitLine(r1[j], delim);

                //Hash join column values using the three
                //hash functions and set the corresponding 
                 //position in the bloom filter to 1s. 
                 addKey(joinColumn, bloomFilter);
                }

            //Send the filled bloom filter to process 2
            MPI_Send(filledBloom, bloomFilterSize, MPI_INT, 2, 1, MPI_COMM_WORLD);


            //Receive possible join tuples from process 2
            //perform the equi-join and write the results to a file. 
            MPI_Recv(tuple, tupleSize, MPI_CHAR, 2, 1, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //DO equi-join 

            //Write results to file (R3.txt)

        }else if(rank == 2){
            //Receive table from process 0. 
            MPI_Recv(r2, r2_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //Receive filled bloom filter from process 1. 
            MPI_Recv(r2, r2_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //Collect tuples likely to be part of the join. 
            unsigned int index = 0;
            for(unsigned int k = 0;kj < r2_length; k++){
                //Get the join column 
                joinColumn = splitLine(r2[k], delim);

                //Hash join column values using the three
                //hash functions and  check whether the 
                //corresponding positions in the bloom filter
                //are already set to 1s.  
                 if(keyExist(joinColumn, bloomFilter)){
                     tuples[index] = r2[k];
                     index ++;
                 }
                }

            //Send the tuples likely to be part of the join to process 1
            MPI_Send(tuples, tupleSize, MPI_CHAR, 1, 2, MPI_COMM_WORLD);


        }else{
            fprintf("Number of process mismatched\n");
            exit(EXIT_FAILURE);
        }
        
    }

    //Finalize the MPI environment
    MPI_Finalize();

    printf("Hellooooo\n");
    
    return 0;
}