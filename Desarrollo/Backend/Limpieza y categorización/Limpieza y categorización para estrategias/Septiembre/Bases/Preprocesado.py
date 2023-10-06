# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 22:57:21 2023

@author: luisn
"""


import pandas as pd
import numpy as np
import datetime



df = pd.read_excel("Base_Telefonos_Agosto23s.xlsx").replace(" ",np.nan).replace(0,np.nan)

"""
Preprocesado para telefonos
TEL_VENTA	Telefono_Cel	telefono	TEL1	TEL2	TEL3	TEL_CARTERA

"""

      

#Se realiza una mascara
mascaraNulos = np.isnan(df).astype(int)

# Sumar las filas de la matriz de NaN para contar NaN por columna
mascaraNulos = np.sum(mascaraNulos, axis=1)

#Si tiene menos de 7 indica que tiene al menos un medio de contacto
mascaraNulos = np.where(mascaraNulos < 7, 1, mascaraNulos)

#Si tiene 7 no tiene medio de contacto es decir 5 nans
mascaraNulos = np.where(mascaraNulos == 7, 0, mascaraNulos)


#Se agrega al DF
df = pd.concat([df, pd.Series(mascaraNulos, name='T')], axis=1)


#Datos demograficos
dfDemo = pd.read_excel("Asignación_DIRSA_Agosto23d.xlsx")

#Datos saldo
dfSaldos = pd.read_excel("Saldos.xlsx")


df_Demo_df = pd.merge(dfDemo, df, on=['Cliente', 'Credito']) 
df_Demo_Comibinado = pd.merge(df_Demo_df, dfSaldos, on=['Cliente', 'Credito']) 

"""
Aqui se agrega el mes a cada fila
"""

df_Demo_Comibinado['mes'] = 'Octubre'

"""
Crédito	Cliente	Nombre	Nombre	Apellido	Apellido	Fecha Apertura	Días vencidos	
Saldo capital	Saldo Intereses	Saldo TOTAL	TEL1	TEL2	TEL3	Correo	Colonia	Delegación	Estado	CP
"""
df_Demo_Comibinado = df_Demo_Comibinado[["Credito","Cliente","nombre1","nombre2","apellido1","apellido2","Fecha_Apertura",
                                     "Meses_vencidos","Saldo_Vencido","Saldo_Actual","Saldo_Intereses","Saldo_Para_Liquidar",
                                     "telefono","TEL1","TEL2","TEL3","TEL_CARTERA","Correo","Dir_calle","CP","T","mes"]]

#Preprocesado para mail

dfMail = df_Demo_Comibinado["Correo"].values.reshape(-1,1)

for i in range(len(dfMail)):
    if dfMail[i] == " ":
        dfMail[i] = np.nan
del i

boolMail = np.array([0 if x!=x else 1 for x in dfMail])

df_Demo_Comibinado = pd.concat([df_Demo_Comibinado, pd.Series(boolMail, name='C')], axis=1)

df_Demo_Comibinado['TC'] = df_Demo_Comibinado['T'] + df_Demo_Comibinado['C']
df_Demo_Comibinado = df_Demo_Comibinado.drop('T',axis=1)
df_Demo_Comibinado = df_Demo_Comibinado.drop('C',axis=1)



"""
Guardar esta tabla
Datos que se regresan "No tienen como contactarse"
"""
dfNoContactables = df_Demo_Comibinado[df_Demo_Comibinado['TC'] == 0]
#Borramos la ultima columna porque no nos sirve
dfNoContactables = dfNoContactables.drop(dfNoContactables.columns[-1], axis=1)


#dfNoContactables.to_excel("NO_CONTACTABLES_AGO_2023.xlsx",index=False)

dfContactables = df_Demo_Comibinado[df_Demo_Comibinado['TC'] > 0]
#Borramos la ultima columna porque no nos sirve
dfContactables = dfContactables.drop(dfContactables.columns[-1], axis=1)


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

Porcentaje=100*(len(dfContactablesUnicos)/len(df))

Suma_total_a_recuperar = dfContactablesUnicos['Saldo_Vencido'].sum()

#Se les asigna según su tipo
dfPredictivas = dfContactablesUnicos[dfContactablesUnicos['Meses_vencidos'] == 1]
#Se exporta
#dfPredictivas.to_excel("listado_predictivos_SEP_2023.xlsx",index=False)
Suma_total_a_recuperar_predictivo = dfPredictivas['Saldo_Vencido'].sum()



dfProgresiva = dfContactablesUnicos[dfContactablesUnicos['Meses_vencidos'] >=  2]
dfProgresiva = dfProgresiva[dfProgresiva['Meses_vencidos'] <=  6]

Suma_total_a_recuperar_progresivo = dfProgresiva['Saldo_Vencido'].sum()
#Se exporta
#dfProgresiva.to_excel("listado_progresivos_SEP_2023.xlsx",index=False)



dfManual = dfContactablesUnicos[dfContactablesUnicos['Meses_vencidos'] >  6]
#Se exporta
#dfManual.to_excel("listado_manual_SEP_2023.xlsx",index=False)
Suma_total_a_recuperar_manual = dfManual['Saldo_Vencido'].sum()



