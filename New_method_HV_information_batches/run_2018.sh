#!/bin/bash
arg1=$1
cd /afs/cern.ch/user/n/nrawal/work/sql_access/new_method_HV_information/CMSSW_14_0_1/src/
cmsenv
cd /afs/cern.ch/user/n/nrawal/work/sql_access/New_method_HV_information_batches/
echo 'python3 main.py --chamber ${arg1} --year "2018"'
python3 main.py --chamber $arg1 --year 2018
#tar -czvf output_${arg1}.tar.gz csc_output_2016_${arg1}_tree_new_HV.root
#python3 main.py --chamber "ME21HV1" --year "2016"
