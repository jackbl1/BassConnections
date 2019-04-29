import os
i = int(os.environ['SLURM_ARRAY_TASK_ID'])
fibonacci = [1,1,2,3,5,8,13,21]
print(i, fibonacci[i])
