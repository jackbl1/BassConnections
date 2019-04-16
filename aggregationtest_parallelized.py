import pandas as pd
import os, sys
from pathlib import Path


#This chunk of code loads every file from the data folder
#To select only certain data files, alter the 'dirs' variable
dirs = ["MATRICULA_CO2012_TRIM.csv", "MATRICULA_CO2013_TRIM.csv", "MATRICULA_CO2014_TRIM.csv"]

col_names = ['FK_COD_ALUNO', 'TP_COR_RACA', 'TP_SEXO', 'FK_COD_MUNICIPIO_END', 'FK_COD_ETAPA_ENSINO', 'PK_COD_TURMA', 'PK_COD_ENTIDADE']
df_cols = ['FK_COD_ALUNO']
newDF = pd.DataFrame(columns = df_cols)
c = 0
paths = ["path1"]
student_ids = {}

num_cols = 1
year = 2012
current_row = 0

#Parallelizes the directory loop into interations
i = int(os.environ['SLURM_ARRAY_TASK_ID'])
file = dirs[i-1]
df = pd.read_csv(file, error_bad_lines=False, engine = 'python', usecols=col_names)

for col in col_names:
    newDF.insert(num_cols, str(col) + '_' + str(year), None)
    num_cols+=1

for index, row in df.iterrows():
    if(row['FK_COD_ALUNO'] in student_ids):
        current_row = student_ids[row['FK_COD_ALUNO']]
        #TODO: replace 'column1' etc. with the correct column names
        newDF.at[current_row, 'TP_COR_RACA' + "_" + str(year)] = row['TP_COR_RACA']
        newDF.at[current_row, 'TP_SEXO' + "_" + str(year)] = row['TP_SEXO']
        newDF.at[current_row, 'FK_COD_MUNICIPIO_END' + "_" + str(year)] = row['FK_COD_MUNICIPIO_END']
        newDF.at[current_row, 'FK_COD_ETAPA_ENSINO' + "_" + str(year)] = row['FK_COD_ETAPA_ENSINO']
        newDF.at[current_row, 'PK_COD_TURMA' + "_" + str(year)] = row['PK_COD_TURMA']
        newDF.at[current_row, 'PK_COD_ENTIDADE' + "_" + str(year)] = row['PK_COD_ENTIDADE']
    else:
        current_row = c
        student_ids[row['FK_COD_ALUNO']] = current_row
        newDF.at[current_row, 'TP_COR_RACA' + "_" + str(year)] = row['TP_COR_RACA']
        newDF.at[current_row, 'TP_SEXO' + "_" + str(year)] = row['TP_SEXO']
        newDF.at[current_row, 'FK_COD_MUNICIPIO_END' + "_" + str(year)] = row['FK_COD_MUNICIPIO_END']
        newDF.at[current_row, 'FK_COD_ETAPA_ENSINO' + "_" + str(year)] = row['FK_COD_ETAPA_ENSINO']
        newDF.at[current_row, 'PK_COD_TURMA' + "_" + str(year)] = row['PK_COD_TURMA']
        newDF.at[current_row, 'PK_COD_ENTIDADE' + "_" + str(year)] = row['PK_COD_ENTIDADE']
        c+=1
year+=1

print(newDF)

