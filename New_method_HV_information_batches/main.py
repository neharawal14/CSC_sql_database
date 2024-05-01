from extracting_dpid import DpidUpdate
from extracting_HV import HVextraction
import uproot
import pandas as pd
import cx_Oracle
import argparse
#from nominal_HV_function_ME11 import nominal_HV_function_ME11 
from datetime import datetime
# Create a table in Oracle database
import time
result = None

#def read_file(input_file,batch_num) : 
def read_file(input_file) : 
    df= 'None'
    # Path to your ROOT file
    root_file_path = input_file 
    # The name of the tree within your ROOT file
    tree_name = 'tree'
    # Open the ROOT file
    with uproot.open(root_file_path) as file:
        # Access the tree
        tree = file[tree_name]
        nentries = file[tree_name].num_entries
        print(" opening the root file and tree")
        # Convert the tree into a Pandas DataFrame
        #df = tree.arrays(library='pd',entry_start=0, entry_stop=10)
        #start_entry = batch_num*1000000
        #end_entry = min( (batch_num+1)*1000000 ,nentries)

        #start_entry = 10
        #end_entry = 20
        #df = tree.arrays(library='pd', entry_start= start_entry, entry_stop = end_entry)
        df = tree.arrays(library='pd')

    return df
def print_dataframe(df): 
    # Now `df` is a Pandas DataFrame where each column corresponds to a branch in the ROOT tree
    print(df.head())  # Print the first few rows of the DataFrame
    print(df)  # Print the first few rows of the DataFrame

def write_file(output_file, df) :
    arrays = {col: df[col].to_numpy() for col in df.columns}
    with uproot.recreate(output_file) as f:
            f['tree'] = arrays



if __name__ == "__main__":
    start_time = time.time()
   
    parser = argparse.ArgumentParser()
    # Open the root file and covert to dataframe
    parser.add_argument('--chamber',type=str, help ="chamber name")
    parser.add_argument('--year',type=str, help ="input year")
    #parser.add_argument('--batch',type=str, help ="batch number")
    args = parser.parse_args()

    year = args.year
    chamber = args.chamber
    input_file= None
    #batch_number = args.batch
    print(" year ", year)
    print("chamber  ", chamber)
    if(year=="2016"):
        input_file = "/eos/home-n/nrawal/CSCAgeing/input_file_2016_CSCAgeing/csc_output_"+year+"_"+chamber+"_tree.root"
    if(year=="2017"):
    #        DUMMYFILENAME_2017_all_ME21HV2_tree.root
        input_file = "/eos/home-n/nrawal/CSCAgeing/input_file_2016_CSCAgeing/2017/DUMMYFILENAME_"+year+"_all_"+chamber+"_tree.root"
    if(year=="2018"):
    #        csc_output_2018_ME11a_tree.root
        input_file = "/eos/home-n/nrawal/CSCAgeing/input_file_2016_CSCAgeing/2018/csc_output_"+year+"_"+chamber+"_tree.root"

    #output_file ="csc_output_"+year+"_"+chamber+"_tree_new_HV_batch_"+batch_number+".root"
    output_file ="csc_output_"+year+"_"+chamber+"_tree_new_HV_second_test.root"
    output_csv_file ="csc_output_"+year+"_"+chamber+".csv"
    #df_initial = read_file(input_file, int(batch_number))
    df_initial = read_file(input_file)
    #print_dataframe(df_initial)
    dpid_obj = DpidUpdate(year)
    df_dpid_new = dpid_obj.extract_dpid(df_initial)
    print(" new data frame extracted dpid ")
    HV_obj = HVextraction(year, chamber) 
    print(" Starting HV extraction ")
    df_HV_new = HV_obj.extracting_HV(df_dpid_new)
    print(" performed HV extraction ")
    end_time = time.time()
    #print_dataframe(df_HV_new)
    # We updated our dataframe with HV values
    # Update our dataframe in root file
    write_file(output_file, df_HV_new)
    print(" start time ", start_time)
    print(" end time ", end_time)

    #df_HV_new.to_csv(output_csv_file, index=False, sep="\t") 


