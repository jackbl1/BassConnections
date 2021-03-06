import pandas as pd
import os, sys
from pathlib import Path
import time

start = time.time()
#This chunk of code loads every file from the data folder
#To select only certain data files, alter the 'dirs' variable
print("step 1")
#path = "./data/"
#dirs = os.listdir(path)
#for file in dirs:
    #print(file)
dirs = ["/Users/alexisangel/Desktop/MATRICULA_SUDESTE_2012.csv"]

print("step 2")
#df = pd.read_csv('MATRICULA_CO.csv')
col_names = ['FK_COD_ALUNO', 'NUM_IDADE', 'ID_ZONA_RESIDENCIAL']
newDF = pd.DataFrame(columns = col_names)
c = 0

student_ids = [118477336627, 116454441392, 121501283064, 122224207840, 
    122224327403, 116477980869, 122224667840, 121503544892, 121505126966]

print("step 3")
for file in dirs:
    print(dirs[0])
    chunksize = 10 ** 6
    counter=0
    for chunk in pd.read_csv(file, delimiter = '|', error_bad_lines=False, engine = 'python', usecols=col_names):
        counter+=1
        if (counter % 100) == 0:
            print(counter)
    end = time.time()
    print("file loaded in memory in", str(end-start), "seconds")

    print("step 4")
    #df_with_index = df.set_index(['FK_COD_ALUNO'])
    print("step 5")
    for id in student_ids:
        print(c)
        print(df[df['FK_COD_ALUNO'] == 118477336627])
        #newDF.loc[c] = df_with_index.loc[id]
        c+=1

print("step 6")
newDF.to_csv('/Users/alexisangel/Desktop/output.csv')
#print(c)

