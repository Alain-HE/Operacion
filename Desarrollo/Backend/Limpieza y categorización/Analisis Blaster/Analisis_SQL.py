# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 21:57:16 2023

@author: luisn
"""
import pymysql
import pandas as pd
import numpy as np

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

database = DataBase()

"""
Cargar datos SOLO UNA VEZ
for i in range(len(df_subir)):
   database.cargar(df_subir[i][0], int(df_subir[i][1]), df_subir[i][2], df_subir[i][3], df_subir[i][4], int(df_subir[i][5]), df_subir[i][6], df_subir[i][7])
"""


"""
Primero ejecutar mostrar repetidos y luego se elije uno para la siguiente funcion
"""

database.mostrar_repetidos()

"""
Productos por cada cliente
"""
database.ProductoxCliente(90717594)

"""
Reporte Interaccion de todos los clientes
"""

database.ReporteInteraccion()

database.close()