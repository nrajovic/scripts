#!/usr/bin/env python

import time
import argparse
import sys
import subprocess
import os
import numpy as np
import glob

TIME_PATTERN = "%Y-%m-%dT%H:%M:%S"

def main():

    output = get_sacct_output(parameters.job_id)
    (begin, end, nodes) = parse_output(output)
    try:
        epoch_begin = convert_to_epoch(begin)
        epoch_end = convert_to_epoch(end)
    except: 
        sys.exit("\nOoops, looks like bad jobID is supplied. Double check it :)\n\nExiting ...\n")
        

    separated_nodes = separate_slurm_nodes(nodes)
    
    get_pwr_nodes(epoch_begin, epoch_end, separated_nodes, parameters.job_id)

    total_average_job_file_power(parameters.job_id, separated_nodes)


    return

def total_average_job_file_power(jobID, nodes):
    total_average_power=0.0

    for node in nodes:
        file = "{0}/{1}.pwr".format(jobID, node)
        print file
        try:
            data = np.genfromtxt(file, delimiter=',', names=True, dtype=None, usecols=(-1))
            total_average_power+=np.average(data['Value'])/1000.00
        except:
            sys.exit("\nOooops, looks like something is wrong with the power samples!\nExiting ...\n")
    
    print "Total average power is: {}".format(total_average_power)
    return


def get_pwr_nodes(start, end, nodes, jobID):
    if not os.path.exists(str(jobID)):
        os.makedirs(str(jobID))
    print "Filtered nodes: {}".format(nodes)
    for node in nodes:
        try:
            cmd = "dcdbquery -h mb.mont.blanc -r {0}-PWR {1} {2}".format(node, start, end, node)
            cmd = cmd.split(" ")
            power_out = open("{0}/{1}.pwr".format(jobID, node), 'w')
            p = subprocess.Popen(cmd, stdout=power_out, stderr=subprocess.PIPE)
        
            err = p.communicate()
            power_out.flush()
            power_out.close()
        except:
            sys.exit("\nDid you load module power_monitor?\nExiting ....\n")
    return



def separate_slurm_nodes(slurm_nodes):
    nodes_list = subprocess.check_output(['scontrol','show','hostname', slurm_nodes])
    nodes_list = nodes_list.split("\n")
    return nodes_list[0:-1]



def convert_to_epoch(slurm_time):
    epoch = int(time.mktime(time.strptime(slurm_time, TIME_PATTERN)))
    return epoch
    
def parse_output(output):
    try: 
        out_split = output.split("|")
        begin = out_split[-3]
        end = out_split[-2]
        nodes = out_split[-1] 
        print nodes
    except:
        sys.exit('\nSeems like your job output line is out-of-bound. Check --acct-line-no parameter you have supplied\n\n')
    return (begin, end, nodes)

def get_sacct_output(job_id):
    #print(job_id)
    p = subprocess.Popen(['sacct','-P','-j','{}'.format(job_id),"-o","JobName,Start,End,Nodelist"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if len(err) == 0:
        out = out.split("\n")
        print out
        try:
            out = out[1+int(parameters.ln)] #here is the assumption of the 2nd last line carrying our data we need
        except:
            sys.exit('\nSeems like your job output line is out-of-bound. Check --acct-line-no parameter you have supplied\n\n')
        return out

    else:
        sys.exit('An error occured while getting job accounting data. Exiting ...')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get power figures')

    parser.add_argument('--job-id', '-id', action='store',
                        dest="job_id",
                        help="SLURM JobID number for accounting",
                        required=True)

    parser.add_argument('--acct-line-no', '-ln', action='store',
                        dest="ln",
                        help="Line number in sacct output which contains relevant data. Indexing starts from 0 onwards.",
                        required=True)


    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
    global parameters
    parameters = parser.parse_args()
    main()
