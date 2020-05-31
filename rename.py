import os
import glob
import argparse

parser = argparse.ArgumentParser(description="Rename fasta and pos files")
parser.add_argument('-p','--position', help="Input folder for position files",required=True)
parser.add_argument('-f','--plain', help="Input folder for plain fasta files",required=True)

args = vars(parser.parse_args())


# pos

pos = sorted(glob.glob(args['position']+"/*.pos"))

for index,element in enumerate(pos):
  os.rename(element,args['position']+"/mapped_reads_to%i.pos"%(index+1))

# gap

gap = sorted(glob.glob(args['plain']+"/*.gap_positions"))

for index,element in enumerate(gap):
  os.rename(element,args['plain']+"/recombinant.n%i.gap_positions"%(index+1))

# plain files

full = sorted(glob.glob(args['plain']+"/*.full"))

for index,element in enumerate(full):
  os.rename(element,args['plain']+"/recombinant.n%i.full"%(index+1))

plain = sorted(glob.glob(args['plain']+"/*.plain"))

for index,element in enumerate(plain):
  os.rename(element,args['plain']+"/recombinant.n%i.plain"%(index+1))
