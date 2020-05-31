
LOG_FILE="runtime.log"
# number of threads for chic index
THREADS=16
# number of dictionary files for RLZ. 1 or 2 recommended
REF_SIZE=1
# number of threads for parallel align (for each bowtie align in the cluster)
THREADS_DIS=16
# number of parallel processes when creating plain files (removing gaps) and
# splitting the sam files
PAR_PROCESSES=2
# max match length for chic index
MATCH_LEN=80
# how many times each file is splitted e.g if the genome size is 50 and RLZ_SPLITS=5,
# the genome is split into 5 partitions each consisting of 10 characters
# if the number of patients is 100 we would create 5*100=500 partitions
# this number should be chosen based on patient's genome size
RLZ_SPLITS=2
# number of blocks for scorematrix
BLOCKS=30
# treeReduce depth in scorematrix
DEPTH=2
open_sem(){
    mkfifo pipe-$$
    exec 3<>pipe-$$
    rm pipe-$$
    local i=$1
    for((;i>0;i--)); do
        printf %s 000 >&3
    done
}
run_with_lock(){
    local x
    read -u 3 -n 3 x && ((0==x)) || exit $x
    (
    "$@" 
    printf '%.3d' $? >&3
    )&
}
