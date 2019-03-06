import pandas as pd

#df = pd.read_csv('MATRICULA_CO.csv')
df = pd.read_csv('testbook.csv', delimiter = '|')
col_names = ['Student ID', 'PK_COD_MATRICULA']
newDF = pd.DataFrame(columns = col_names)
c = 0
for index, row in df.iterrows():
    print(row['NUM_IDADE'])
    if(row['NUM_IDADE'] == 8):
        newDF.loc[c] = [row['NUM_IDADE'], row['PK_COD_MATRICULA']]
        c+=1


newDF.to_csv('output.csv')
print(c)
