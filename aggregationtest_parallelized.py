import pandas as pd
import os, sys
from pathlib import Path
import time

start = time.time()

#This chunk of code loads every file from the data folder
#To select only certain data files, alter the 'dirs' variable
print("step 1")

dirs = ["MATRICULA_CO2012.csv", "MATRICULA_CO2013.csv", "MATRICULA_CO2014.csv"]
#dirs = ["C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2012_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2013_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2014_TRIM.csv"]
    #"C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2015_TRIM.csv", "C:\\Users\\Jack\\bassconnections\\trimmed_data\\MATRICULA_CO2016_TRIM.csv"]
#dirs = ["C:\\Users\\Jack\\bassconnections\\data\\MATRICULA_CO2012s.csv", "C:\\Users\\Jack\\bassconnections\\data\\MATRICULA_CO2013s.csv"]

print("step 2")
col_names = ['FK_COD_ALUNO', 'TP_COR_RACA', 'TP_SEXO', 'FK_COD_MUNICIPIO_END', 'FK_COD_ETAPA_ENSINO', 'PK_COD_TURMA', 'PK_COD_ENTIDADE']
#df_cols = ['FK_COD_ALUNO', 'TP_COR_RACA_2012', 'TP_SEXO_2012', 'FK_COD_MUNICIPIO_END_2012', 'FK_COD_ETAPA_ENSINO_2012', 'PK_COD_TURMA_2012', 'PK_COD_ENTIDADE_2012', 'TP_COR_RACA_2013', 'TP_SEXO_2013', 'FK_COD_MUNICIPIO_END_2013', 'FK_COD_ETAPA_ENSINO_2013', 'PK_COD_TURMA_2013', 'PK_COD_ENTIDADE_2013']
df_cols = ['FK_COD_ALUNO']
newDF = pd.DataFrame(columns = df_cols)
c = 0

student_ids = {}

print("step 3")
num_cols = 1
year = 2012
current_row = 0
#Parallelizes the directory loop into interations
i = int(os.environ['SLURM_ARRAY_TASK_ID'])
#for file in dirs:
    print(dirs[i])
    df = pd.read_csv(dirs[i], error_bad_lines=False, delimiter='|', engine = 'python', usecols=col_names)

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

print("step 6")
newDF.to_csv('output.csv')
#print(c)

