from logging import raiseExceptions

import pandas as pd 
import requests
import sqlite3
from sqlite3 import OperationalError
import os
import json
import datetime
from datetime import datetime
from datetime import timedelta

# Modulo de funciones
from funciones_stocks import*

# Inicio del programa con un while para seleccion en menu principal
menuprincipal = int(input("TRABAJO FINAL PYTHON\n\nIngrese el numero de opcion deseada: \n 1 - Actualizacion de datos \n 2 - Visualizacion de datos guardados \n 0 - Salir \n"))

while menuprincipal !=0:
    
    # Conexion con la base de datos
    os.chdir(r"C:\Users\jmaxi\Trabajo final python\env\src")
    db_conn = sqlite3.connect("database/stocks")
    
    #Actualizacion de datos
    if menuprincipal==1:
        
        #Validacion de fechas, descarga desde y hasta comparando fecha minima o fecha maxima
        try:
            cursor=db_conn.cursor()
            ticker, fecha_desde, fecha_hasta = ingresar_fechas()
            
            date_min = cursor.execute(f"select min(Dates) from tbl_stocks where Ticker='{ticker}'")
            date_min = cursor.fetchone()
            date_min = str(date_min[0])
            date_max = cursor.execute(f"select max(Dates) from tbl_stocks where Ticker='{ticker}'")
            date_max = cursor.fetchone()
            date_max = str(date_max[0])
            

            if date_min == 'None' and date_max == 'None':
                fecha_hasta = fecha_hasta
                fecha_desde = fecha_desde
            else:
                date_min = datetime.strptime(date_min, '%Y-%m-%d')
                date_max = datetime.strptime(date_max, '%Y-%m-%d')
                fecha_desde = datetime.strptime(fecha_desde, '%Y-%m-%d')
                fecha_hasta = datetime.strptime(fecha_hasta, '%Y-%m-%d')
                
                if fecha_desde > date_min and fecha_hasta < date_max:
                    raise ValueError("Las fechas solicitadas ya estan guardadas en la base de datos, muchas gracias. \n")
                
                if fecha_desde > date_min:
                    fecha_desde = date_min
                
                if fecha_hasta < date_max:
                    fecha_hasta = date_max
            
                fecha_desde = fecha_desde.strftime('%Y-%m-%d')
                fecha_hasta = fecha_hasta.strftime('%Y-%m-%d')
            

            #Request a la api
        
            data = api_request(ticker, fecha_desde, fecha_hasta)

            dict = json.loads(data)
            df2 = pd.DataFrame.from_dict(dict, orient="index")

            list_items = []

            #Parseo de json
            for item in dict['results']:
                a = item
                list_items.append(a)
                
            df_results = pd.DataFrame(list_items)
            df_results.reset_index(inplace=True,drop=True)

            #Preparado de los datos para guardar en la base
            df_stock = data_wrangling(df_results, ticker)


            #Guardado en la base, con validacion de datos duplicados
            df_stock.to_sql(name='tbl_stocks', con=db_conn, if_exists='append',index=False)
            
            df_no_duplicates = pd.read_sql_query("select distinct * from tbl_stocks", db_conn)
            
            df_no_duplicates.to_sql(name='tbl_stocks', con=db_conn, if_exists='replace',index=False)
            
            print("Datos actualizados, muchas gracias. \n")
            
            cursor.close()
            
        except ValueError as err:
            print(err.args)
    
    #Visualizacion de datos    
    elif menuprincipal==2:
        
        menu_viz = int(input("Elija el tipo de visualizacion: \n 1 - Resumen \n 2 - Grafico \n \n"))
        
        #Resumen de datos 
        if menu_viz==1:
        
            print("Los tickers guardados son: ")
            
            #Creo un df con los distintos tickers guardados
            tickers_res = pd.read_sql_query("select distinct ticker from tbl_stocks", db_conn)

            date_minres = []
            date_maxres = []
            
            #Iteracion por tickers para consultar fechas minimas y maximas
            for x in tickers_res['Ticker']:
                
                minres_query = pd.read_sql_query(f"select min(Dates) as FechaDesde from tbl_stocks where Ticker='{x}'", db_conn)
                date_minres.append(minres_query)
                maxres_query = pd.read_sql_query(f"select max(Dates) as FechaHasta from tbl_stocks where Ticker='{x}'", db_conn)
                date_maxres.append(maxres_query)
                
            tickers_res['FechaDesde'] = date_minres
            tickers_res['FechaHasta'] = date_maxres
            
            for x in range(len(tickers_res['Ticker'])):
                ticker = str(tickers_res.iloc[x]['Ticker'])
                fechasdesde =  str(tickers_res.iloc[x]['FechaDesde'])
                fechahasta =  str(tickers_res.iloc[x]['FechaHasta'])
                
                print(f"{ticker} {fechasdesde} {fechahasta} \n")
        
        #Grafico segun ticker a demanda    
        elif menu_viz==2:
            
            ticker_plot = str(input("Elija el ticker a graficar: \n"))
            df_plot = pd.read_sql_query(f"select * from tbl_stocks where Ticker='{ticker_plot}'", db_conn)
            
            graficar(df_plot,ticker_plot)
        else:
            print("Opcion invalida, vuelva a ingresar la opcion deseada.")
                     
    elif menuprincipal==0:
        print('Muchas gracias, vuelva prontos!')

    else:
        print("Opcion invalida, vuelva a ingresar la opcion deseada.")
        
    menuprincipal = int(input("Ingrese el numero de opcion deseada: \n 1 - Actualizacion de datos \n 2 - Visualizacion de datos \n 3 - Graficos en tiempo real \n 0 - Salir \n" ))