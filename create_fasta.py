import multiprocessing as mp 
import glob
import argparse
import os
from shutil import copyfile
import sys


# example how to run

# python3 create_fasta.py -i pan/* -o out.fa


parser = argparse.ArgumentParser(description="Turn plain genome files into fasta files by adding the filename as flag")
parser.add_argument('-i','--input', help="Path to input files that should be transformed",required=True,nargs='+')
parser.add_argument('-p','--temp',help="Folder for temporary files",required=False, default="tmp")
parser.add_argument('-o','--output', help="Output file name",required=True)
parser.add_argument('-t','--threads',default=4,type=int,required=False,help="Number parallel processes to execute. Default is 4.")


args = vars(parser.parse_args())


# create a temporary folder where to place files with headers

tmp_folder = args['temp']

if not os.path.exists(tmp_folder):
    os.makedirs(tmp_folder)


# list filenames that will get the fasta flag
file_names_tmp = glob.glob(args['input'][0]+'/*')
file_names = [i for i in file_names_tmp if i.endswith('.plain')]
#file_names = args['input']

input_len = len(file_names)

if input_len is 0:
  sys.exit("Input file length is 0, exiting")

print('Files to process: %i'%input_len)


# copy a single file and add fasta flag to it based on the file name

def create(path):
  # copy file and add flag
  name = os.path.basename(path)
  out = tmp_folder+'/'+name+'.fa'
  copyfile(path, out)
  with open(out, 'r+') as f:
      body = f.read()
      f.seek(0)
      f.write('>' + name + '\n' + body + '\n')

  # return new file names
  return out


# read and create a new file with fasta flag in parallel

pool = mp.Pool(processes=args['threads'])

# process every file in parallel and wait that every file is written

print("Creating temporary files in parallel")

results = pool.imap_unordered(create,file_names)


tmp_file_names = sorted(results)

print("Temporary files created\nConcatenate to output file: %s"%args['output'])


# create output file

with open(args['output'], 'w') as outfile:
  for fname in tmp_file_names:
      with open(fname) as infile:
          for line in infile:
              outfile.write(line)
