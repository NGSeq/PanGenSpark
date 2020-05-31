import re
import argparse



parser = argparse.ArgumentParser(description="Calculate metrics for VCF file when the reads are simulated from known sequence.")
parser.add_argument('-r','--reference', help="True sequence",required=True)
parser.add_argument('-v','--vcf', help="VCF file (predicted)",required=True)
parser.add_argument('-t','--true', help="VCF file generated from the known sequence",required=True)
parser.add_argument('-o','--output', help="Output file to append")

args = vars(parser.parse_args())

pan_data = ""
# load sequence from which the reads were generated
with open(args['reference']) as pan_file:
  pan_data = pan_file.readline()



# list of zeros and ones telling if the the match was true
# true positive or false positive
snv = list()
# indel true postives and false negatives

# process vcf file
indel = list()
with open(args['vcf']) as file1:
  for line in file1:
    if line.startswith('#'):
      continue
    else:
      # split line
      splitted = re.compile("\s+").split(line)
      pos = int(splitted[1])-1
      # compare this to the "true" file from which
      # the reads were simulated
      alt = splitted[4]
      alt_len = len(alt)
      # if snv
      if alt_len==1:
        snv.append(1 if pan_data[pos].upper()==alt else 0)
      # indel
      else:
        # indel length
        indel.append(1 if pan_data[pos:pos+alt_len].upper()==alt else 0)



# get number of variants for snv and indels
lines_snv = 0
lines_indel = 0
with open(args['true']) as vcf_true:
  for line in vcf_true:
    if line.startswith('#'):
      continue
    else:
      splitted = re.compile("\s+").split(line)
      if len(splitted[3])!=1:
          lines_indel += 1
      else:
        lines_snv += 1

# calculate metrics

# sensitivity/recall

snv_tp = snv.count(1)
print(lines_snv)
sensitivity_snv = 0 if lines_snv==0 else float(snv_tp)/lines_snv

indel_tp = indel.count(1)
sensitivity_indel = 0 if lines_indel==0 else float(indel_tp)/lines_indel


# precision

snv_fp = snv.count(0)
precision_snv = 0 if (snv_tp+snv_fp)==0 else float(snv_tp)/(snv_tp+snv_fp)

indel_fp = indel.count(0)
precision_indel = 0 if(indel_tp+indel_fp==0) else float(indel_tp)/(indel_tp+indel_fp)


def write(output,a1,a2):
  output.write(a1+': ' + '%.5f'%a2 + '\n')

def printer(a1,a2):
  print(a1+': ' + '%.5f'%a2)

printer('snv_tp',snv_tp)
printer('indel_tp',indel_tp)
printer('snv_fp',snv_fp)
printer('indel_fp',indel_fp)
printer('sensitivity_snv',sensitivity_snv)
printer('sensitivity_indel',sensitivity_indel)
printer('precision_snv',precision_snv)
printer('precision_indel',precision_indel)


# append output to logging file if parameter supplied
if args['output'] is not None:
  with open(args['output'],'a') as output:
    write(output,'sensitivity_snv',sensitivity_snv)
    write(output,'sensitivity_indel',sensitivity_indel)
    write(output,'precision_snv',precision_snv)
    write(output,'precision_indel',precision_indel)
