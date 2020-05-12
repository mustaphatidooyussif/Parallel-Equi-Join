            //Hash join column and send bloom filter to node 2. 

            #pragma omp parallel private(id, num_threads)
            {
                char *joinColumn;
                
                id = (unsigned int)omp_get_thread_num();
                num_threads = (unsigned int)omp_get_num_threads();
                
                #pragma omp for schedule (static) private(joinColumn)
                for(unsigned int j = 0; j < recv_table_len; j++){
                    //Get the join column 
                    joinColumn = splitLine(r1_rec[j], joinColPos, delim);
                    printf("%s\n", r1_rec[j]);
                    //Hash join column values using the three
                    //hash functions and set the corresponding 
                    //position in the bloom filter to 1s.
                    #pragma omp critical 
                        addKey(joinColumn, bloomFilter);
                }
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
            