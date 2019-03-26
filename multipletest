import pandas as pd
import os, sys
#test
#This chunk of code loads every file from the data folder
#To select only certain data files, alter the 'dirs' variable
print("step 1")
#path = "./data/"
#dirs = os.listdir(path)
#for file in dirs:
    #print(file)
dirs = ["C:\\Users\\Jack\\bassconnections\\data\\MATRICULA_CO2012", "C:\\Users\\Jack\\bassconnections\\data\\MATRICULA_CO2013"]

print("step 2")
#df = pd.read_csv('MATRICULA_CO.csv')
col_names = ['FK_COD_ALUNO', 'NUM_IDADE', 'ID_ZONA_RESIDENCIAL']
newDF = pd.DataFrame(columns = col_names)
c = 0

print("step 3")
for file in dirs:
    print(dirs[0])
    df = pd.read_csv(file, delimiter = '|', error_bad_lines=False, engine = 'python')
    #df = pd.read_csv('./data/' + file, error_bad_lines=False)

    for index, row in df.iterrows():
        if(row['FK_COD_ALUNO'] == 118477336627):
            newDF.loc[c] = [row['FK_COD_ALUNO'], row['NUM_IDADE'], row['ID_ZONA_RESIDENCIAL']]
            c+=1

print("step 4")
newDF.to_csv('output.csv')
print(c)

