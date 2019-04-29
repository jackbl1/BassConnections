import pandas as pd
import os

dirs = ["MATRICULA_NORDESTE2012.csv", "MATRICULA_NORDESTE2013.csv", "MATRICULA_NORDESTE2014.csv"]

col_names = ['FK_COD_ALUNO', 'TP_COR_RACA', 'TP_SEXO', 'FK_COD_MUNICIPIO_END', 'FK_COD_ETAPA_ENSINO', 'PK_COD_TURMA', 'PK_COD_ENTIDADE',
    'ID_POSSUI_NEC_ESPECIAL', 'ID_N_T_E_P', 'ID_INGRESSO_FEDERAIS', 'FK_COD_CURSO_PROF', 'COD_MINICIPIO_ESCOLA', 'FK_CODIGO_DISTRITO', 'ID_DEPENDENCIA_ADM_ESC', 'ID_MANT_ESCOLA_PRIVADA_SIST_S']
df_cols = ['FK_COD_ALUNO']
newDF = pd.DataFrame(columns = df_cols)
c = 0

student_ids = {}

num_cols = 1
year = 2012
current_row = 0
for file in dirs:
    df = pd.read_csv(file, error_bad_lines=False, sep='|', engine = 'python', usecols=col_names)

    for col in col_names:
        newDF.insert(num_cols, str(col) + '_' + str(year), None)
        num_cols+=1

    for index, row in df.iterrows():
        if(row['FK_COD_ALUNO'] in student_ids):
            current_row = student_ids[row['FK_COD_ALUNO']]
            newDF.at[current_row, 'TP_COR_RACA' + "_" + str(year)] = row['TP_COR_RACA']
            newDF.at[current_row, 'TP_SEXO' + "_" + str(year)] = row['TP_SEXO']
            newDF.at[current_row, 'FK_COD_MUNICIPIO_END' + "_" + str(year)] = row['FK_COD_MUNICIPIO_END']
            newDF.at[current_row, 'FK_COD_ETAPA_ENSINO' + "_" + str(year)] = row['FK_COD_ETAPA_ENSINO']
            newDF.at[current_row, 'PK_COD_TURMA' + "_" + str(year)] = row['PK_COD_TURMA']
            newDF.at[current_row, 'PK_COD_ENTIDADE' + "_" + str(year)] = row['PK_COD_ENTIDADE']
            newDF.at[current_row, 'ID_POSSUI_NEC_ESPECIAL' + "_" + str(year)] = row['ID_POSSUI_NEC_ESPECIAL']
            newDF.at[current_row, 'ID_N_T_E_P' + "_" + str(year)] = row['ID_N_T_E_P']
            newDF.at[current_row, 'ID_INGRESSO_FEDERAIS' + "_" + str(year)] = row['ID_INGRESSO_FEDERAIS']
            newDF.at[current_row, 'FK_COD_CURSO_PROF' + "_" + str(year)] = row['FK_COD_CURSO_PROF']
            newDF.at[current_row, 'COD_MUNICIPIO_ESCOLA' + "_" + str(year)] = row['COD_MUNICIPIO_ESCOLA']
            newDF.at[current_row, 'FK_CODIGO_DISTRITO' + "_" + str(year)] = row['FK_CODIGO_DISTRITO']
            newDF.at[current_row, 'ID_DEPENDENCIA_ADM_ESC' + "_" + str(year)] = row['ID_DEPENDENCIA_ADM_ESC']
            newDF.at[current_row, 'ID_MANT_ESCOLA_PRIVADA_SIST_S' + "_" + str(year)] = row['ID_MANT_ESCOLA_PRIVADA_SIST_S']
        else:
            current_row = c
            student_ids[row['FK_COD_ALUNO']] = current_row
            newDF.at[current_row, 'FK_COD_ALUNO'] = row['FK_COD_ALUNO']
            newDF.at[current_row, 'TP_COR_RACA' + "_" + str(year)] = row['TP_COR_RACA']
            newDF.at[current_row, 'TP_SEXO' + "_" + str(year)] = row['TP_SEXO']
            newDF.at[current_row, 'FK_COD_MUNICIPIO_END' + "_" + str(year)] = row['FK_COD_MUNICIPIO_END']
            newDF.at[current_row, 'FK_COD_ETAPA_ENSINO' + "_" + str(year)] = row['FK_COD_ETAPA_ENSINO']
            newDF.at[current_row, 'PK_COD_TURMA' + "_" + str(year)] = row['PK_COD_TURMA']
            newDF.at[current_row, 'PK_COD_ENTIDADE' + "_" + str(year)] = row['PK_COD_ENTIDADE']
            newDF.at[current_row, 'ID_POSSUI_NEC_ESPECIAL' + "_" + str(year)] = row['ID_POSSUI_NEC_ESPECIAL']
            newDF.at[current_row, 'ID_N_T_E_P' + "_" + str(year)] = row['ID_N_T_E_P']
            newDF.at[current_row, 'ID_INGRESSO_FEDERAIS' + "_" + str(year)] = row['ID_INGRESSO_FEDERAIS']
            newDF.at[current_row, 'FK_COD_CURSO_PROF' + "_" + str(year)] = row['FK_COD_CURSO_PROF']
            newDF.at[current_row, 'COD_MUNICIPIO_ESCOLA' + "_" + str(year)] = row['COD_MUNICIPIO_ESCOLA']
            newDF.at[current_row, 'FK_CODIGO_DISTRITO' + "_" + str(year)] = row['FK_CODIGO_DISTRITO']
            newDF.at[current_row, 'ID_DEPENDENCIA_ADM_ESC' + "_" + str(year)] = row['ID_DEPENDENCIA_ADM_ESC']
            newDF.at[current_row, 'ID_MANT_ESCOLA_PRIVADA_SIST_S' + "_" + str(year)] = row['ID_MANT_ESCOLA_PRIVADA_SIST_S']
            c+=1
    year+=1

newDF.to_csv('NordesteAggregated.csv')

