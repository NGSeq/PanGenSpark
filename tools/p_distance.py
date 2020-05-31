import numpy as np
import multiprocessing as mp
import argparse

parser = argparse.ArgumentParser(description="Calculate p-distance between two fasta/plain files")
parser.add_argument('-r','--reference', help="Reference fasta/plain file",required=True)
parser.add_argument('-a','--adhoc', help="Adhoc fasta/plain file",required=True)
parser.add_argument('-f','--fasta',
  help="If files are in fasta format, then 1. Otherwise 0. Default is 1.",default=1,type=int)

args = vars(parser.parse_args())

# calculate p-distance between the two sequences (https://www.megasoftware.net/web_help_7/hc_p_distance_nucleotide.htm)

def read_plain(path):
  with open(path) as f:
    content = f.read()
  return np.array(list(content))

def read_fasta(path):
  with open(path) as f:
    content = f.read()
  skip_flag = content.find('\n')+1
  return np.array(list(content[skip_flag:]))

ref = np.empty(1,dtype=np.character)
adhoc = np.empty(1,dtype=np.character)
# if in plain format
if args['fasta'] == 0:
  ref = read_plain(args['reference'])
  adhoc = read_plain(args['adhoc'])
else:
  ref = read_fasta(args['reference'])
  adhoc = read_fasta(args['adhoc'])


# calculate p distance

# total length

length = ref.shape[0]

same = (ref == adhoc).sum()

res = 1.0-float(same)/length

print('Difference: %i'%(length-same))
print('Accuracy: %.5f'%res)
