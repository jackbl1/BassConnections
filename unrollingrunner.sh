#!/bin/bash
#SBATCH -e slurm.err
module load Python/2.7.11
python unrolling.py
