# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 20:42:22 2023


            host='pernexium-db.cfioetbrvik6.us-east-2.rds.amazonaws.com', #ip
            user='admin',
            password='4FtfUO8s2CLMYhtrYErm',
            db='crm_interaccion'


@author: luisn
"""
import pymysql
import pandas as pd
import numpy as np

df = pd.read_csv("C:/Users/luisn/OneDrive/Escritorio/Chamba/One_Conection/Blaster/file.csv")


class DataBase:
    def __init__(self):
        self.connection = pymysql.connect(
            host='pernexium-db.cfioetbrvik6.us-east-2.rds.amazonaws.com', #ip
            user='admin',
            password='4FtfUO8s2CLMYhtrYErm',
            db='crm_interaccion'
        )

        self.cursor = self.connection.cursor()
        print("Conexión establecida!!")

    def cargar(self, Call_ID, CREDITO, ValorNumerico, Answer_DateTime, Hangup_DateTime, Duration, Notes, Disposition):
        values = [Call_ID, CREDITO, ValorNumerico, Answer_DateTime, Hangup_DateTime, Duration, Notes, Disposition]
    
        non_empty_values = []
        
        for value in values:
            if not pd.isna(value) and value != "":
                non_empty_values.append(value)
        
        if non_empty_values:
            placeholders = ', '.join(['%s'] * len(non_empty_values))
            sql = f"INSERT INTO reporteinteraccion (id, idCredito, idAgente, horaInicio, horaFin, tiempoLlamada, outputLlamada, detalleOutput) VALUES ({placeholders})"
            try:
                self.cursor.execute(sql, non_empty_values)
                self.connection.commit()
                print("Registro insertado con éxito!!")
            except Exception as e:
                print(e)
        else:
            print("No se pudo insertar el registro porque todos los campos están vacíos o contienen 'nan'.")
            
    def mostrar_repetidos(self):
        sql="SELECT cliente, COUNT(*) AS cantidad FROM asignacion GROUP BY cliente HAVING COUNT(*) > 1;"
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            registros = self.cursor.fetchall()
            for registro in registros:
                print('Cliente: ', registro[0])
                print('Repeticiones: ',registro[1])
        except Exception as e:
            print(e)
            
    #Reporte de Interacción - Pasa a ser un Inner Join
    def ReporteInteraccion(self):
        sql = """
        SELECT
            asignacion.tipoCampana,
            asignacion.cliente,
            reporteinteraccion.idAgente,
            reporteinteraccion.horaInicio,
            reporteinteraccion.horaFin,
            reporteinteraccion.tiempoLlamada,
            reporteinteraccion.outputLlamada,
            reporteinteraccion.detalleOutput,
            asignacion.saldoParaLiquidar,
            asignacion.dirCalle
        FROM reporteinteraccion
        INNER JOIN asignacion ON reporteinteraccion.idCredito = asignacion.credito;
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            registros = self.cursor.fetchall()
            for registro in registros:
                print("TipoCampana:", registro[0])
                print("Cliente:", registro[1])
                print("IDAgente:", registro[2])
                print("HoraInicio:", registro[3])
                print("HoraFin:", registro[4])
                print("TiempoLlamada:", registro[5])
                print("OutputLlamada:", registro[6])
                print("DetalleOutput:", registro[7])
                print("SaldoParaLiquidar:", registro[8])
                print("DirCalle:", registro[9])
                print()
        except Exception as e:
            print(e)
            
    def ProductoxCliente(self,cliente):
        sql = """
                SELECT cliente, TipoServicio, saldoActual, 
                       (SELECT SUM(saldoActual) FROM asignacion WHERE cliente = a.cliente) AS SaldoTotal
                FROM asignacion AS a
                WHERE cliente = {};
        """.format(cliente)
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            registros = self.cursor.fetchall()
            for registro in registros:
                print('Cliente: ',registro[0])
                print('Tipo Servicio: ',registro[1])
                print('Saldo Actual: ',registro[2])
                print('Saldo Total: ',registro[3])
        except Exception as e:
            print(e)    
    
    
    def mostrar(self):
        sql = "SELECT * FROM reporteinteraccion"
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            registros = self.cursor.fetchall()
            for registro in registros:
                print("ID:", registro[0])
                print("IDCredito:", registro[1])
                print("IDAgente:", registro[2])
                print("HoraInicio:", registro[3])
                print("HoraFin:", registro[4])
                print("TiempoLlamada:", registro[5])
                print("OutputLlamada:", registro[6])
                print("DetalleOutput:", registro[7])
                print()
        except Exception as e:
            print(e)


    def close(self):
        print("Conexión terminada")
        self.connection.close()
        
def asignar_valor_numerico(df):
    df['ValorNumerico'] = 0
    for col_idx, col_name in enumerate(df.columns):
        if df[col_name].any():
            df.loc[df[col_name] == True, 'ValorNumerico'] = col_idx + 1
    return df
from datetime import datetime


# Función para convertir el formato

def convertir_a_mysql_format(fecha_original):
    if isinstance(fecha_original, str):
        try:
            return datetime.strptime(fecha_original, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return fecha_original  # Si no es una fecha válida, mantener el valor original
    else:
        return fecha_original  # Si no es una cadena, mantener el valor original
        
def llamadas_contestadas(df):
    df.fillna(0, inplace=True)
    
    
    df_contestadas = df[df['Answer State'] == 'ANSWER']
    
    df_contestadas = df_contestadas[df_contestadas['Agent'] != 0]
    
    dummies = pd.get_dummies(df_contestadas['Agent'], prefix='Agente', drop_first=False)
    
    dummies = asignar_valor_numerico(dummies)
    
    dummies.drop(dummies.iloc[:, 0:-1], inplace=True, axis=1)
    
    df_contestadas=pd.merge(dummies,df_contestadas, left_index=True, right_index=True)
    
    TIME_AVG=np.mean(df_contestadas["Duration (Seconds)"])
    
    
    promedio_por_indice = df_contestadas.groupby('ValorNumerico')['Duration (Seconds)'].mean()
    
    
    columnas_seleccionadas = [
        'Call ID', 'CREDITO', 'ValorNumerico', 'Answer DateTime',
        'Hangup DateTime', 'Duration (Seconds)', 'Notes','Disposition - BANCOPPEL'
    ]
    
    df_subir=df_contestadas[columnas_seleccionadas]
    
    
    
    # Aplicar la función a todo el DataFrame
    df_subir = df_subir.applymap(convertir_a_mysql_format)
    
    df_subir=df_subir.to_numpy()
    # Imprimir el DataFrame resultante
    return df_subir

def tiempo_llamada(df,valor):
    df_contestadas = df[df['Answer State'] == 'ANSWER']
    
    df_contestadas = df_contestadas[df_contestadas['Agent'] != 0]
    
    dummies = pd.get_dummies(df_contestadas['Agent'], prefix='Agente', drop_first=False)
    
    dummies = asignar_valor_numerico(dummies)
    
    dummies.drop(dummies.iloc[:, 0:-1], inplace=True, axis=1)
    
    df_contestadas=pd.merge(dummies,df_contestadas, left_index=True, right_index=True)
    
    TIME_AVG=np.mean(df_contestadas["Duration (Seconds)"])
    
    
    promedio_por_indice = df_contestadas.groupby('ValorNumerico')['Duration (Seconds)'].mean().to_numpy()
    
    if valor=="*":
        return TIME_AVG
    elif valor==int(valor) and valor<=len(promedio_por_indice):
        return promedio_por_indice[valor]
    else:
        return (-1)


"""
Funcion llamadas promedio por agente

"*"   - Todos los agentes
1,2,3 - Agente en especifico
"""
valor=1

llamadas = tiempo_llamada(df, valor)

database = DataBase()


"""
Funcion de df_subir
"""

#df_subir=llamadas_contestadas(df)

"""
Cargar datos SOLO UNA VEZ
for i in range(len(df_subir)):
   database.cargar(df_subir[i][0], int(df_subir[i][1]), df_subir[i][2], df_subir[i][3], df_subir[i][4], int(df_subir[i][5]), df_subir[i][6], df_subir[i][7])
"""


database.close()