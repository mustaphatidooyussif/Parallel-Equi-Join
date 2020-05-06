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
    size_t r1_length = 0; /*Number of rows in table 1*/
    size_t r2_length = 0;  /*Number of rows in table 2*/
    unsigned int tupleSize = 0;

    unsigned int id = 0;
    unsigned int num_threads = 1; 

    //TODO: Parallize this section. 
    //Initialize bloom filter to zeros. 
    size_t bloomFilterSize = sizeof(bloomFilter)/sizeof(int);
    for(unsigned int i=0; i < bloomFilterSize; i++){
        bloomFilter[i] = 0; 
    }



    /*Initialize the MPI environment*/
    MPI_Init(&argc, &argv);

    /**Get the number of processes*/
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size); 

    /*Get the rank of the process*/
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    int tag_zero = 0;
    int tag_one = 1;
    int tag_two = 2; 

    /*
    STEP: 1
    Read the relations into two tables R1 and R2. And
    send R1 to node-1 and R2 to node-2. 
    */
    if(my_rank == 0){
        
        //Read file one into table
        char **r1 = readFile(r1_name, joinColPos);

        //Read file two into table
        char **r2 = readFile(r2_name, joinColPos);

        //Update size of tables 
        r1_length = (size_t) sizeof(r1)/sizeof(*r1);
        r2_length = (size_t) sizeof(r2)/sizeof(*r2);

        printf("Size of table 1(R1: %d\n", r1_length);
        printf("Size of table 2(R2: %d\n", r2_length);

        //TODO: CHECK LENGTH PASSED (r1_length x 1024)
        MPI_Send(&(r1[0][0]), r1_length, MPI_CHAR, 1, tag_zero, MPI_COMM_WORLD);
        //MPI_Send(&(r2[0][0]), r2_length, MPI_CHAR, 2, tag_zero, MPI_COMM_WORLD);
    }else{

        //Process one try to receive 
        if (my_rank == 1){
            printf("In other processses\n");
            /*
            STEP: TWO
            Hash join column values and send bloom the filter to node 2.
            

            //Receive table from process 0
            char **r1_rec = NULL;
            MPI_Recv(&(r1_rec[0][0]), r1_length , MPI_CHAR, 0, tag_zero, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //Hash join column and send bloom filter to node 2. 

            #pragma omp parallel private(id, num_threads)
            {
                char *joinColumn;
                
                id = (unsigned int)omp_get_thread_num();
                num_threads = (unsigned int)omp_get_num_threads();
                
                #pragma omp for schedule (static) private(joinColumn)
                for(unsigned int j = id; j < r1_length; j++){
                    //Get the join column 
                    joinColumn = splitLine(r1_rec[j], delim);

                    //Hash join column values using the three
                    //hash functions and set the corresponding 
                    //position in the bloom filter to 1s.
                    #pragma omp critical 
                        addKey(joinColumn, bloomFilter);
                }
            }
            

            //Send the filled bloom filter to process 2
            MPI_Send(bloomFilter, bloomFilterSize, MPI_INT, 2, tag_zero, MPI_COMM_WORLD);

            
            //STEP: FOUR
            //Receive possible join tuples from node 2 and perform 
            //equi-join. 
            

            //Receive possible join tuples from process 2
            //perform the equi-join and write the results to a file. 
            char **joinTuples = NULL;
            MPI_Recv(&(joinTuples[0][0]), tupleSize, MPI_CHAR, 2, tag_zero, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //DO equi-join 
            printf("----------------------------------------------\n");

            //Write results to file (R3.txt)

        }else if(my_rank == 2){


            char **r2_rec= NULL;

            //Receive table from process 0. 
            MPI_Recv(&(r2_rec[0][0]), r2_length, MPI_CHAR, 0, tag_zero, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            
            //STEP: THREE 
            //Find possible join tuples and send them to node 1.
            
            //Receive filled bloom filter from process 1. 
            int *bloomFilter_rec = NULL;
            MPI_Recv(bloomFilter_rec, bloomFilterSize, MPI_INT, 0, 0, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            //Collect tuples likely to be part of the join. 
            char **tuples = (char **)malloc(r2_length * sizeof(char *));
            if(tuples == NULL){
                fprintf(stderr, "Cannot allocate memory\n");
                exit(EXIT_FAILURE);
            }

            unsigned int tuple_index = 0;
            #pragma omp parallel private(id, num_threads)
            {
                id = (unsigned int)omp_get_thread_num();
                num_threads = (unsigned int)omp_get_num_threads();
                char  *joinColumn = NULL;

                #pragma omp for private(joinColumn)
                for(unsigned int k = 0;k < r2_length; k ++){
                    //Get the join column 
                    joinColumn = splitLine(r2_rec[k], delim);

                    //Hash join column values using the three
                    //hash functions and  check whether the 
                    //corresponding positions in the bloom filter
                    //are already set to 1s.  
                    if(keyExist(joinColumn, bloomFilter)){
                        tuples[tuple_index] = r2_rec[k];
                        tuple_index ++;

                    }
                }
            }
            
            tupleSize = (unsigned int)tuple_index;

            //Send the tuples likely to be part of the join to process 1
            MPI_Send(&(tuples[0][0]), tupleSize, MPI_CHAR, 1, tag_zero, MPI_COMM_WORLD);

        */

        }else{
            fprintf(stderr, "Number of process mismatched\n");
            exit(EXIT_FAILURE);
        }
        
    }

    //Finalize the MPI environment
    MPI_Finalize();

    //clearing memory
    /*free(r1);
    free(r2);
    free(tuples);
    free(r1_rec);
    free(r2_rec);
    free(bloomFilter_rec);
    free(bloomFilter);*/
    printf("Hellooooo\n");
    
    return 0;
}