import pandas as pd

filename='2002_CENSOESC.csv'
print("Loading file", filename+"...")
df = pd.read_csv('/Users/alexisangel/Desktop/'+filename, \
                 usecols=['UF','CODMUNIC', 'MUNIC', 'DEP', 'LOC', 'CODFUNC'],\
                 delimiter = ',', encoding='latin1')
print("Done loading file", filename)

print("Loading regions into dictionary...")
regions=dict()
for index, row in df.iterrows():
    val = row['UF']
    if val not in regions:
        regions[val]=0
    regions[val]+=1
print("Done loading dictionary")

## Printing output
print("Number of regions in", filename, "is", len(regions))
print("Total rows in dataset is", len(df))
for key in sorted(regions.keys()):
    print(key, ":", regions[key], "["+str(int((int(regions[key])/len(df))*100))+"%]")

#filtering for Sao Paolo region only
  
sp=df[df.UF=='São Paulo']
sp=sp[sp.CODFUNC=='Ativo']
sp=sp[sp.MUNIC=='SAO PAULO']
#print(sp.head(10))
