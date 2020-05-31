import argparse
import sys

parser = argparse.ArgumentParser(description="Change list of multialigned files to column format and change gaps letters to '-'.\nOutput file name is pangenome.")
parser.add_argument('file', type=argparse.FileType('r'), nargs='+')

args = parser.parse_args()
with open('pangenome','w') as output:
  while True:
    for f in args.file:
      c = f.read(1)
      if not c:
        sys.exit()
      output.write(c if c!='N' else '-')
    output.write('\n')


    


