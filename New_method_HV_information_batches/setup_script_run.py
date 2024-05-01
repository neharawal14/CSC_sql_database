import uproot
import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--chamber", type=str, help="input chamber")
    parser.add_argument("--year", type=str, help="year")
    args = parser.parse_args()

    chamber = args.chamber
    year = args.year
    input_file = "/eos/home-n/nrawal/CSCAgeing/input_file_2016_CSCAgeing/csc_output_"+year+"_"+chamber+"_tree.root"

    with uproot.open(input_file) as file:
        n_entries = file["tree"].num_entries
        #chunk_size = n_entries // num_chunks
        chunk_size = 1000000
        num_chunks = n_entries // chunk_size +1
        print(" n entries :", n_entries)
        print(" num chunks :", num_chunks)
    
    f = open(f"run_chunk_{chamber}_{year}.py", 'w')
    f.write(f"cd /afs/cern.ch/user/n/nrawal/work/sql_access/new_method_HV_information/CMSSW_14_0_1/src/ \n")
    f.write("cmsenv \n")
    f.write("cd /afs/cern.ch/user/n/nrawal/work/sql_access/New_method_HV_information_batches/ \n")
    for i in range(num_chunks):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, n_entries)
        f.write(f"python3 main.py --chamber {chamber} --year '2016' --batch {i}\n")
       # f.write(f"python3 main.py --chamber {chamber} --year '2016' --batch {i}\n")
       # f.write(f"{chamber}, {i}\n")
    f.close()

#    os.system("python3 run_chunk_{chamber}_{year}.py")
