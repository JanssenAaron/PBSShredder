import re
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

dependency_path = os.path.abspath( __file__ )
sys.path.append(dependency_path)

from classes import Job, sql_job, getBase



# Installation parameters
accounting_file_location = "/home/ccastdev/"
connection_string="postgresql://postgres:password@localhost/shredtarget"


field_names = ["account=",
"accounting_id=",
"alt_id=",
"array_indices=",
"ctime=",
"eligible_time=",
"end=",
"etime=",
"exec_host=",
"exec_vnode=",
"Exit_status=",
"group=",
"jobname=",
"pcap_accelerator=",
"pcap_node=",
"pgov=",
"project=",
"qtime=",
"queue=",
"resvID=",
"resvname=",
"run_count=",
"session=",
"start=",
"user="
]


# Returns job obj from a E record
def _get_job_from_record(id, fields):
    # Uses a list of indexs and known names of each field name to split on every delimiting space
    # May fail if a known field name is used in a value ( in format " knownname=")
    indexs=[]

    for field in field_names:

        index = fields.find(" "+field.strip())
        # If it does not find the field with a space the first field has a semicolon instead
        if(index==-1):
            index = (";"+fields).find(";"+field.strip())
        # If the field is not found
        if(index==-1):
            continue
        
        indexs.append(index)
    # Find all match to the PBSPro resource format eg " resources_used.someresourcename="
    # May fail if this format is used in values
    for match in re.finditer( r' resources_used\.\w+=', fields):

        indexs.append(match.start())

    for match in re.finditer( r' Resource_List\.\w+=', fields):

        indexs.append(match.start())
    #sorts the indexs to allow cuting out spaces inbetween fields
    indexs.sort()
    # Some magic that takes all indexs and creates a list made of the substrings from each index to the other
    # The result should be a list of substring with the splits done on the indexs provided
    entries = []
    for i, j in zip(indexs, indexs[1:] +[None]):

        entries.append(fields[i:j])
    
    output = { "resources_used": {}, "Resource_List": {}}

    # Takes each entry and splits it into a tag and value
    # The Resource_List and resources_used fields become sub dictionaries
    for entry in entries:

        tag, value = entry.split("=",1)
        tag = tag.strip()

        if tag.startswith("Resource_List") or tag.startswith("resources_used"):
            output[tag.split(".",1)[0]][tag.split(".",1)[1]] = value

        else:
            output[tag] = value

    return Job(id, output)

# Each parser method takes a string formatted in a specific way from the 
# PBSPro dcgm intergration and changes it to a normal value ( int, float, etc)

# \d+MHZ
def _clock_speed_parser(string):
    return _strip_n_from_values(string, 3)

# \d+\.\d+GB
# The intergration may use other units for memory
# Converting values may be required
def _mem_used_parser(string):
    return _strip_n_from_values(string, 2)

# \d+\.\d+W
def _energy_used_parser(string):
    return _strip_n_from_values(string, 1)

# \d+\.\d+(mins | secs | hrs)
def _gpu_duration_parser(string):

    output = _strip_n_from_values(string, 3)
    for key, value in output.items():
        if value.endswith("s"):
            output[key] = float(value[0:-1])/60/60
        if value.endswith("m"):
            output[key] = float(value[0:-1])/60 
    return(output)
        
# \d+\.\d+%
def _util_parser(string):
    return _strip_n_from_values(string, 1)

# Removes n chars from the end of the key-value pair's value
def _strip_n_from_values(string, n):
    output = {}
    data = _parse_gpu_per_node_stat(string)

    for key, value in data.items():
        output[key] = value[0:-n]
    return output

# dcgm intergration stats ending in _per_node_gpu have the format : "nodename:(gpuname:value+gpuname2:value2)+nodename2(gpuname:value3)"
# This splits that string into a dictonary
# of {nodename:gpuname = value, nodename:gpuname2=value2, nodename2:gpuname=value3}
def _parse_gpu_per_node_stat(string):

    output = {}
    #Splits the string by each of the nodes
    nodes = string.split(")+")
    
    # Deals with an extra ")" on the strings
    for i, node in enumerate(nodes):
        node = node.strip()
        if node.endswith(")"):
            node = node[0:-1]
        nodes[i] = node


    for node in nodes:
        node_name, gpus= node.split(":(")

        for gpu in gpus.split("+"):
            gpu_num, gpu_stat = gpu.split(":")

            id_of_gpu = node_name+":"+gpu_num

            output[id_of_gpu] = gpu_stat
    
    return output


#Gets Job objs from filename
def _get_jobs_from_file(filename):
    jobs = []
    with open(filename, "r") as f:
        for line in f.readlines():
            time, type, id, fields = line.split(';')
            if type != "E" or time.startswith("#"):
                continue
            job = _get_job_from_record(id, fields)

            jobs.append(job)
    return jobs

# Takes a parser key value pair and a dictionary
# The dictionary in the from
# { 
#   nodename:gpunum={key:value,key2:value2},
#   nodename:gpunum2={key:value3}
# }
# and adds the key and ( parsed ) value to the sub dictionary

def _sub_dict_parse(usage_dict, key, value, parser):
    for usageid, stat in parser(value).items():

        if usageid not in usage_dict.keys():
            usage_dict[usageid] = {}

        usage_dict[usageid][key] = stat
    return usage_dict

# User available code starting here
# IE if you want to use this code in a different way
# Like using a different sql library
# This is the start of more user usable code


# Gets sql objs from all files match the YYYYMMDD file name format for pbspro accounting logs in a target directory
def get_sql_objs_from_dir(dirname):
    jobs = []
    for filename in os.listdir(dirname):
        if re.match(r"2[0-9]{7}", filename):
            jobs+= _get_jobs_from_file(accounting_file_location+"/"+filename)

    return jobs



# Main execution
if __name__ == "__main__":

    # gets sql_objs
    jobs = get_sql_objs_from_dir(accounting_file_location)
    real_jobs = []

    # Exports to sql alchemly objs
    for job in jobs:
        real_jobs.append(sql_job(job.get_id(), job).export_to_alchemy())

    prod = True
    if prod:
    # Connects to mysql db
        engine = create_engine(connection_string)
        # Create database if needed
        #engine.execute("Create Database testjob")
        #MYSQL COMPAT
        #engine.execute("use testjob") # Which database to use

        # Creates the new tables ( Using the sql_alchemly classes as the schemas ) if they do not exist
        # May have issues if only a subset of tables exist
        try:
            metadata = getBase().metadata
            metadata.create_all(engine, checkfirst=True)
        except:
            pass  

        # Takes all alchemy objs and merges ( replaces old data with new data ) all records
        with Session(engine) as session:

            for job in real_jobs:
                session.merge(job)

            session.commit()