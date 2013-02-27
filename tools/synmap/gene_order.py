#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import gzip
import os
import re
import shutil
import sys

logger = logging.getLogger("gene_order")
logger.setLevel(logging.INFO)

def main(input, output, gid1, gid2, feature1, feature2):
    ''' Returns a dag file
    
    If feature1 or feature2 are genomic then the dag will be reordered and
    generate a new file. Otherwise, the input dag file will be copied. 
    '''
    genomic_flag = 0

    if not(os.path.exists(input)):
        logging.error("Input file: (%s) not found.", input)

    if not(feature1 == "genomic") and not(feature2 == "genomic"):
        logging.warn("No genomic features were specified.")
        shutil.copyfile(input, output)
        return 0
    else:
        #FIXME: Is this for a future addition?
        # This was brought over from the perl script.
        # Genomic names need to be processed seperately 
        genomic_flag = 1

    logging.info("Opening %s for converting to gene order.", input)

    input_file = None
    if input.endswith("gz"):
        logging.info("Opening the input file with gzip.")
        with gzip.open(input, 'r') as fp:
            input_file = fp.readlines()
    else:
        logging.info("Opening the input file.")
        with open(input, 'r') as fp:
            input_file = fp.readlines()

    if not input_file:
        logging.error("The input file was unable to be opened.")
        return 1

    genomic_order = order_genes(input_file, feature1, feature2)
    logging.info("Writing %s for converting to gene order.", args.output)

    with open(output) as fp:
        for line in input_file:
            if line.startswith("#"):
                fp.write("%s\n" % line)
                continue
            
            fields = line.split("\t")
            fields.append("\n")

            if feature1 == "genomic":
                fields[2] = genomic_order[1][fields[0]][fields[1]]['order']
                fields[3] = genomic_order[1][fields[0]][fields[1]]['order']
            else:
                item = re.split("\|\|", fields[1])[7]
                fields[2] = item
                fields[3] = item

            if feature2 == "genomic":
                fields[6] = genomic_order[2][fields[4]][fields[5]]['order']
                fields[7] = genomic_order[2][fields[4]][fields[5]]['order']
            else:
                item = re.split("\|\|", fields[5])[7]
                fields[6] = item
                fields[7] = item
            
            fp.write("\t".join(fields))
    return 0 

def order_genes(data, feature1, feature2):
    '''Returns dictionary containing the genomic sequence
   
    The order of the genes in a genome are changed if the feature is genomic.
    The ordering of the genes is done by using the start position of its
    sequence.
    '''

    genomic_order = {}

    for line in input_file:
        if line.startswith("#"):
            continue

        fields = line.split("/t")
        # FIXME: What was this intended to do? It's from the perl script
        #items = map(lambda x: re.split('\|\|', x), [fields[1], fields[5]])

        if feature1 == "genomic":
            genomic_order[1] = {field[0] : {field[1] : {'start' : field[2]}}}

        if feature2 == "genomic": 
            genomic_order[2] = {field[4] : {field[5] : {'start' : field[6]}}}

    # Compares the gene by its starting position
    cmp_by_start_seq = lambda x : x['start']

    if feature1 == "genomic":
        for chromosome in genomic_order[1].iterkeys():
            genes = genomic_order[1][chromosome].values();
            sorted(genes, key=cmp_by_start_seq)
            
            for (order, gene) in enumerate(genes, start=1):
                gene[order] = order

    if feature2 == "genomic":
        for chromosome in genomic_order[2].iterkeys():
            genes = genomic_order[2][chromosome].values();
            sorted(items, key=cmp_by_start_seq)
            
            for (order, gene) in enumerate(genes, start=1):
                gene['order'] = order

    return genomic_order

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='''
        Reorders the genes contained in the dag file by ascending order.
        ''')

    parser.add_argument("input",
                        help="dag file to be ordered")
    parser.add_argument("output",
                        help="dag file that is produced.")
    parser.add_argument("gid1",
                        help="The first genome id.")
    parser.add_argument("gid2",
                        help="The second genome id.")
    parser.add_argument("feature1",
                        help="The feature of the first genome")
    parser.add_argument("feature2",
                        help="The feature of the second genome")
    args = parser.parse_args()

    ret = main(args.input, args.output, args.gid1, argsgid2, args.feature1,
         args.feature2)

    sys.exit(ret)
