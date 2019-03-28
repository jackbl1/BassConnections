import pandas as pd
import os, sys
from pathlib import Path
import time

start = time.time()

#This chunk of code loads every file from the data folder
#To select only certain data files, alter the 'dirs' variable
print("step 1")
dirs = ["C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2012_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2013_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2014_TRIM.csv",
    "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2015_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2016_TRIM.csv"]


print("step 2")
col_names = ['FK_COD_ALUNO', 'TP_COR_RACA', 'TP_SEXO', 'FK_COD_MUNICIPIO_END', 'FK_COD_ETAPA_ENSINO', 'PK_COD_TURMA', 'PK_COD_ENTIDADE']
newDF = pd.DataFrame(columns = col_names)
c = 0

student_ids = [118477336627, 116454441392, 121501283064, 122224207840, 
    122224327403, 116477980869, 122224667840, 121503544892, 121505126966]

print("step 3")
for file in dirs:
    print(dirs[0])
    df = pd.read_csv(file, error_bad_lines=False, engine = 'python', usecols=col_names)

    for index, row in df.iterrows():
        if(row['FK_COD_ALUNO'] in student_ids):
            newDF.loc[c] = index
            c+=1

print("step 6")
newDF.to_csv('output.csv')
#print(c)

