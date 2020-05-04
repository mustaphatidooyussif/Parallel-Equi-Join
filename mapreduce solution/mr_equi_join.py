from mrjob.job import MRJob 
from mrjob.compat import jobconf_from_env
import sys

R1_NAME = None 
R2_NAME = None 

class MREQuiJoin(MRJob):

    def configure_args(self):
        """
        This method configures commandline 
        options for the program. 
        The commandline options:
            --stop-words: the name of the file 
            containing the stop words. 
        """
        super(MREQuiJoin, self).configure_args()
        self.add_passthru_arg(
            "--column", 
            help = "specify the index of the join column. "
        )

        self.add_file_arg(
            "--R3", 
            help = "specify R3 file name. ",
            default = "R3.txt"
        )

    def mapper_init(self):
        """
        This method initializes or setup 
        the resources needed by the mapper function. 
        """
        self.table_delim_map = "|"
        self.column = int(self.options.column)

    def mapper(self, _, line):
        try:
            #Make tables names global
            global R1_NAME 
            global R2_NAME

            #Get the names of the tables. 
            input_file_name = jobconf_from_env('map.input.file')

            #Augment file name to line 
            line = input_file_name + "*" + line 

            #store the names of the tables.
            if input_file_name is not None:
                if R1_NAME is None:
                    R1_NAME = input_file_name 
                else:
                    if R1_NAME != input_file_name:
                        R2_NAME = input_file_name

            #get join column 
            key = line.split(self.table_delim_map)[self.column]
            yield key, line 

        except:
            print("Something is not right.")
            sys.exit(-1) 

    def reducer_init(self):
        """
        This method initializes or setup 
        the resources needed by the reducer function. 
        """
        self.table_delim_red = "|"
        self.r1 = []  ##table one
        self.r2 = []   #table two 

    def reducer(self, key, values):

        #Step through tuples of tables
        #Get the name of the table (the first string when `value` is split)
        #Add values to the r1(table 1) and r2(table 2) base on the table name.
        
        for value in values:
            table_name = value.split("*")[0]
            if table_name == R1_NAME:
                self.r1.append(value.split("*")[1])
            elif table_name == R2_NAME:
                self.r2.append(value.split("*")[1])
            else:
                print('No table name')
                
        #Get command lin options
        column = int(self.options.column)
        r3_name = self.options.R3


        #TODO: Perform join and write results to a file. 
        #Do equi-join
        for i in range(len(self.r1)):
            for j in range(len(self.r2)):
                r2_tuple = self.r2[j].split(self.table_delim_red)
                r2_tuple.pop(column)
                joined_tuple = self.r1[i] + "|".join(r2_tuple)

                yield None , joined_tuple

if __name__=="__main__":
    MREQuiJoin.run()