#!/bin/bash
## wikipi dump parse
#SBATCH --job-name="cdsc_sohw_wikipi; parse wikipedia dumps"
## Allocation Definition
#SBATCH --account=comdata-ckpt
#SBATCH --partition=ckpt
## Resources
## Nodes. This should always be 1 for parallel-sql.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
## Walltime (12 hours)
#SBATCH --time=24:00:00
## Memory per node
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH 
#SBATCH --chdir /gscratch/comdata/users/sohw/wikipi
#SBATCH --output=jobs/%A_%a.out
#SBATCH --error=jobs/%A_%a.out
##turn on e-mail notification
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sohyeonhwang@u.northwestern.edu

TASK_NUM=$(( SLURM_ARRAY_TASK_ID + $1))
TASK_CALL=$(sed -n ${TASK_NUM}p ./task_list.sh)
${TASK_CALL}
