
awk -F" " 'NR==FNR{a[$1];next} ($2 in a) {print $1,$2,$3,$4,$5}' var_ids outbwa/var.flt.vcf > found_ids
