#!/usr/bin/env bash

CUR=$( pwd )

# compile needed accessories

mkdir modules
cd modules
git clone "https://gitlab.com/dvalenzu/CHIC.git"
git clone "https://gitlab.com/dvalenzu/PanVC.git"
git clone "https://github.com/mariusmni/radixSA.git"

yum install -y maven2
yum install -y gcc gcc-c++ autoconf automake
yum install -y cmake
yum install -y tbb-devel
yum install -y unzip
yum install -y mmv

cd ${CUR}
cd modules/CHIC/
make

cd ${CUR}
cd modules/CHIC/ext
./get_relz.sh

cd ${CUR}
cd modules/CHIC/src
make all

cd ${CUR}
mv modules/PanVC/components/normalize_vcf/ components/
mv modules/PanVC/components/sam_to_pos/ components/
cd modules/PanVC/components/pan_genome_index_real/ChicAligner-beta/ext
./compile.sh

cd ${CUR}
cd modules/radixSA/
make

cd ${CUR}
yum install -y curl-devel
yum install -y bzip2
wget https://github.com/samtools/samtools/releases/download/1.10/samtools-1.10.tar.bz2
tar -xf samtools-1.10.tar.bz2
cd samtools-1.10
make

#Tools for mapping variants to original reference genome positions if needed
#cd ${CUR}
#cd components/normalize_vcf
#git clone "https://github.com/lindenb/jvarkit.git"
#cd jvarkit
#make msa2vcf
#cd ${CUR}
#cd components/normalize_vcf/projector
#make

cd ${CUR}

pip install numpy
pip install biopython

#compile PanSpark
mvn package

#Copy needed binaries
cp -f modules/CHIC/src/chic_index components/index/chic_index
cp -f modules/CHIC/src/chic_align components/index/chic_align
cp -f modules/radixSA/radixSA ./
cp -f modules/PanVC/components/pan_genome_index_real/ChicAligner-beta/ext/LZ/RLZ_parallel/src/rlz_for_hybrid ./

mkdir -p /opt/chic/
cp -r components/index/ /opt/chic/
mkdir -p /mnt/tmp

#Setup worker nodes
for i in {1..25}; do
    ssh -tt -o "StrictHostKeyChecking no" node-$i mkdir /opt/chic/
    scp -o "StrictHostKeyChecking no" -r components/index/ node-$i:/opt/chic/
   
    ssh -tt -o "StrictHostKeyChecking no" node-$i mkdir -p $CUR/modules/CHIC/ext/
    scp -o "StrictHostKeyChecking no" -r  modules/CHIC/ext/BOWTIE2 node-$i:$CUR/modules/CHIC/ext/ 
    ssh -tt -o "StrictHostKeyChecking no" node-$i mkdir -p /opt/samtools       
    scp -o "StrictHostKeyChecking no"  samtools-1.10/* node-$i:/opt/samtools/
    ssh -tt -o "StrictHostKeyChecking no" node-$i mkdir /mnt/tmp

    #scp -o "StrictHostKeyChecking no" /opt/hadoop/etc/hadoop/* node-$i:/opt/hadoop/etc/hadoop/ &
    #scp /etc/hosts node-$i:/etc/hosts
done
