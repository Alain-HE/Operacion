# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 23:05:14 2023

@author: luisn
"""

import pandas as pd
import numpy as np


df = pd.read_excel("Base_telefonos.xlsx")

"""
Preprocesado para telefonos
"""
dfTelefonos = df[["telefono","TEL1","TEL2","TEL3","TEL4"]].values
dfTelefonos = dfTelefonos[:,0:4]

#En el caso de TEL4 contiene valores 0 por eso se hace esto antes
dfTEL4 = df["TEL4"].values.reshape(-1,1)
dfTEL4 = dfTEL4.astype(float)

df["TEL4"] = df["TEL4"].replace(0, np.nan)
        
dfTelefonos = np.concatenate([dfTelefonos, dfTEL4],axis=1)

mascaraNulos = np.isnan(dfTelefonos).astype(int)

# Sumar las filas de la matriz de NaN para contar NaN por columna
mascaraNulos = np.sum(mascaraNulos, axis=1)

boolTelefonos = np.where(mascaraNulos < 5, 1, mascaraNulos)

boolTelefonos = np.where(mascaraNulos == 5, 0, boolTelefonos)

"""
Preprocesado para mail
"""
dfMail = df["MAIL"].values.reshape(-1,1).copy()
df["MAIL"] = df["MAIL"].apply(lambda x: np.nan if x == " " else x)

boolMail = np.array([0 if x!=x else 1 for x in dfMail])


#Se juntan ambos preprocesados
boolTotal = boolTelefonos + boolMail

boolTotal = np.where(boolTotal >= 1, 1, boolTotal)

dfConBool = pd.concat([df, pd.Series(boolTotal, name='C')], axis=1)

#Df de valores que se pueden contactar y valores que no se pueden contactar
"""
Guardar esta tabla
Datos que se regresan "No tienen como contactarse"
"""
dfNoContactables = dfConBool[dfConBool['C'] == 0]
#Borramos la ultima columna porque no nos sirve
dfNoContactables = dfNoContactables.drop(dfNoContactables.columns[-1], axis=1)

dfNoContactables.to_excel("Base_telefonos_no_contactables.xlsx",index=False)

dfContactables = dfConBool[dfConBool['C'] == 1]
#Borramos la ultima columna porque no nos sirve
dfContactables = dfContactables.drop(dfContactables.columns[-1], axis=1)

#Valores unicos del data que si podemos contactar
"""
Guardar esta tabla
Datos que nos quedamos "Si tienen como contactarse y son unicos"
"""
dfContactablesUnicos = dfContactables[~dfContactables.duplicated(subset="Cliente")]

dfContactablesUnicos.to_excel("Base_telefonos__contactables.xlsx",index=False)

#Valores duplicados
"""
Guardar esta tabla 
Datos que si tienen como contactarse pero estan duplicados
"""
dfDuplicados = dfContactables[dfContactables.duplicated(subset="Cliente", keep="first")]

dfDuplicados.to_excel("Base_telefonos_contactables_duplicados.xlsx",index=False)

Porcentaje=100*(len(dfContactablesUnicos)/len(df))


"""
Cuanto podemos recuperar del total

"""


dfSaldos = pd.read_excel("Saldos.xlsx")

dfMerge = pd.merge(dfSaldos, dfContactablesUnicos, on='Cliente') 

dfMatch = dfMerge[dfMerge['Cliente'].isin(dfSaldos['Cliente']) & 
              dfMerge['Cliente'].isin(dfContactablesUnicos['Cliente'])]

#Ver cuantos no hacen match
dfSaldosNoMatch = dfSaldos[~dfSaldos['Cliente'].isin(dfMatch['Cliente'])]

#Hacemos un dataframe con el cliente id y saldo vencido
dfSaldoVencido = dfMerge[['Cliente','Saldo_Vencido']]

#Sacamos la suma total del saldo vencido
Suma_total_a_recuperar = dfSaldoVencido['Saldo_Vencido'].sum()













