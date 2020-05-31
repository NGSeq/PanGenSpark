import numpy as np
import argparse
import glob
import multiprocessing as mp

parser = argparse.ArgumentParser(description="Calculate unrestricted heaviest path")
parser.add_argument('-p','--pos', help="Position folder",required=True)
parser.add_argument('-g','--pangenome', help="pangenome in fasta",required=True)
parser.add_argument('-l','--length', help="MSA length",required=True,type=int)
parser.add_argument('-n','--num', help="number of reference",required=True,type=int)

args = vars(parser.parse_args())

# create full sized matrix

matrix = np.zeros((args['num'],args['length']))


files = sorted(glob.glob(args['pos']))

# add 1 to each match
row = 0
for file in files:
  print("adding file %s to matrix"%file)
  with open(file) as f:
    for l in f:
      raw = l.split(" ")
      pos = int(raw[0])
      r_len = int(raw[1])
      matrix[row,pos:r_len] += 1
  row += 1

# matrix is now initialized

# find the maximum path

print("Calculating maximum path")
path = np.argmax(matrix,axis=0)
print("Writing to file")

def getMatch(ref,pos):
  cur = 0
  with open(args['pangenome']) as f:
    for l in f:
      if l.startswith('>'):
        continue
      elif cur<ref:
        cur += 1
        continue
      else:
        return l[pos]
  return 'N'
# map to correct rows
c_pos = 0
with open("adhoc","w") as f:
  for i in path:
    print("Processed: %.3f %% \r"%(float(c_pos)/args['length']*100),end="")
    f.write(getMatch(i,c_pos))
    c_pos += 1

