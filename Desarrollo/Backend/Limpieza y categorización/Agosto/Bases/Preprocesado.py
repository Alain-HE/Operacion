# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 23:05:14 2023

@author: luisn
"""

import pandas as pd
import numpy as np


df = pd.read_excel("Base_Telefonos_Agosto23s.xlsx")

"""
Preprocesado para telefonos
"""
dfTelefonos = df[["telefono","TEL1","TEL2","TEL3","TEL4"]].values

#Se separan aquellos con espacios en blanco
dftelefono = df["telefono"].values.reshape(-1,1)
dfTEL1 = df["TEL1"].values.reshape(-1,1)
dfTEL4 = df["TEL4"].values.reshape(-1,1)
dfTelefonos = dfTelefonos[:,2:4]

dfTEL4 = dfTEL4.astype(float)


for i in range(len(dfTEL4)):
    if dfTEL4[i]==0:
        dfTEL4[i]=np.nan  
del i

dftelefono[dftelefono == ' '] = np.nan
dftelefono = dftelefono.astype(float)

dfTEL1[dfTEL1 == ' '] = np.nan
dfTEL1 = dfTEL1.astype(float)

#Se vuelven a juntar
dfTelefonos = np.concatenate([dftelefono,dfTEL1, dfTelefonos,dfTEL4],axis=1).astype(float)
      

#Se realiza una mascara
mascaraNulos = np.isnan(dfTelefonos).astype(int)

# Sumar las filas de la matriz de NaN para contar NaN por columna
mascaraNulos = np.sum(mascaraNulos, axis=1)

#Si tiene menos de 5 indica que tiene al menos un medio de contacto
boolTelefonos = np.where(mascaraNulos < 5, 1, mascaraNulos)

#Si tiene 5 no tiene medio de contacto es decir 5 nans
boolTelefonos = np.where(mascaraNulos == 5, 0, boolTelefonos)

"""
Preprocesado para mail
"""
dfMail = df["MAIL"].values.reshape(-1,1).copy()

for i in range(len(dfMail)):
    if dfMail[i] == " ":
        dfMail[i] = np.nan
del i

boolMail = np.array([0 if x!=x else 1 for x in dfMail])


#Se juntan ambos preprocesados
boolTotal = boolTelefonos + boolMail

boolTotal = np.where(boolTotal >= 1, 1, boolTotal)

dfConBool = pd.concat([df, pd.Series(boolTotal, name='C')], axis=1)


"""
Guardar esta tabla
Datos que se regresan "No tienen como contactarse"
"""
dfNoContactables = dfConBool[dfConBool['C'] == 0]
#Borramos la ultima columna porque no nos sirve
dfNoContactables = dfNoContactables.drop(dfNoContactables.columns[-1], axis=1)


#dfNoContactables.to_excel("NO_CONTACTABLES_AGO_2023.xlsx",index=False)

dfContactables = dfConBool[dfConBool['C'] == 1]
#Borramos la ultima columna porque no nos sirve
dfContactables = dfContactables.drop(dfContactables.columns[-1], axis=1)

#Valores unicos del data que si podemos contactar
"""
Guardar esta tabla
Datos que nos quedamos "Si tienen como contactarse y son unicos"
"""
dfContactablesUnicos = dfContactables[~dfContactables.duplicated(subset="Cliente")]

#dfContactablesUnicos.to_excel("Base_telefonos__contactables.xlsx",index=False)

#Valores duplicados
"""
Guardar esta tabla 
Datos que si tienen como contactarse pero estan duplicados
"""
dfDuplicados = dfContactables[dfContactables.duplicated(subset="Cliente", keep="first")]

#dfDuplicados.to_excel("Reporte_Duplicados_AGO_2023.xlsx",index=False)

Porcentaje=100*(len(dfContactablesUnicos)/len(df))



"""
Cuanto podemos recuperar del total

"""

#Se carga el nuevo archivo los archivos saldos
dfSaldos = pd.read_excel("Saldos.xlsx")


#Se hace un merge de los que podemos juntar a partir de cliente
dfMerge = pd.merge(dfSaldos, dfContactablesUnicos, on='Cliente') 

#Si cliente esta en ambas tablas se indexa
dfMatch = dfMerge[dfMerge['Cliente'].isin(dfSaldos['Cliente']) & 
              dfMerge['Cliente'].isin(dfContactablesUnicos['Cliente'])]

#Ver cuantos no hacen match
dfSaldosNoMatch = dfSaldos[~dfSaldos['Cliente'].isin(dfMatch['Cliente'])]


#Hacemos un dataframe con el cliente id y saldo vencido
dfSaldoVencido = dfMerge[['Cliente','Saldo_Vencido']]


#Sacamos la suma total del saldo vencido
Suma_total_a_recuperar = dfSaldoVencido['Saldo_Vencido'].sum()


dfDemo = pd.read_excel("Asignación_DIRSA_Agosto23d.xlsx")




#Se vuelven a hacer merge de aquellos datos que si se pueden juntar
dfCombinado_todo = pd.merge(dfDemo, dfMatch, on='Cliente') 
dfCombinado_todo = dfCombinado_todo[["Credito","Cliente","NOMBRE1","NOMBRE2","APELLIDO1","APELLIDO2","fec_apertura","pagos_vencidos","Saldo_Vencido","Saldo_Actual","Saldo_Intereses","Saldo_Para_Liquidar","telefono","TEL1","TEL2","TEL3","TEL4","MAIL","ESTADO","CP"]]


#Se les asigna según su tipo
dfPredictivas = dfCombinado_todo[dfCombinado_todo['pagos_vencidos'] == 1]
#Se exporta
#dfPredictivas.to_excel("listado_predictivos_AGO_2023.xlsx",index=False)
Suma_total_a_recuperar_predictivo = dfPredictivas['Saldo_Vencido'].sum()



dfProgresiva = dfCombinado_todo[dfCombinado_todo['pagos_vencidos'] >=  2]
dfProgresiva = dfProgresiva[dfProgresiva['pagos_vencidos'] <=  6]

Suma_total_a_recuperar_progresivo = dfProgresiva['Saldo_Vencido'].sum()
#Se exporta
#dfProgresiva.to_excel("listado_progresivos_AGO_2023.xlsx",index=False)



dfManual = dfCombinado_todo[dfCombinado_todo['pagos_vencidos'] >  6]
#Se exporta
#.to_excel("listado_manual_AGO_2023.xlsx",index=False)
Suma_total_a_recuperar_manual = dfManual['Saldo_Vencido'].sum()






