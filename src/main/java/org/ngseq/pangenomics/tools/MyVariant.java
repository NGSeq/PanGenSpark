package org.ngseq.pangenomics.tools;

import java.io.Serializable;

/**
 * Created by root on 5/3/17.
 */
public class MyVariant implements Serializable {

    private String bases;
    //private String genotype;
    private String alleles;
    private String reference;


    public String getBases() {
        return bases;
    }

    public void setBases(String bases) {
        this.bases = bases;
    }

    public String getAlleles() {
        return alleles;
    }

    public void setAlleles(String alleles) {
        this.alleles = alleles;
    }

    public String getReference() {
        return reference;
    }
    public void setReference(String reference) {
        this.reference = reference;
    }

        /*public void setGenotype(String genotype) {
            this.genotype = genotype;
        }
        public String getGenotype() {
            return genotype;
        }*/
}