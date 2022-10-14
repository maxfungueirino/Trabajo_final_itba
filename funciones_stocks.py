# En este modulo se declaran todas las fuciones utilizadas en main_stocks

import pandas as pd 
import numpy as np
import requests
import sqlite3
from sqlite3 import OperationalError
import urllib
import time
import csv
import json
import datetime
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import plotly.graph_objects as go
from funciones_stocks import*

# Funcion para ingresar fechas con validacion de datos
def ingresar_fechas():
    while True:
        try:
            ticker= str(input("Ingrese el ticker a consultar: "))
            fecha_desde = str(input("Ingrese fecha inicio (formato %Y-%m-%d): "))
            fecha_hasta = str(input("Ingrese fecha fin  (formato %Y-%m-%d): "))
        except ValueError:
            print("Ingreso de datos incorrecto.")
            continue

        if len(fecha_desde) < 10 or len(fecha_desde) > 10 or len(fecha_hasta) < 10 or len(fecha_hasta) > 10:
            print("Ingreso de fecha incorrecto, por favor vuelva a ingresar los datos correctamente.")
            continue
        
        if len(ticker) > 10:
            print("Ingreso de fecha incorrecto, por favor vuelva a ingresar los datos correctamente.")
        else:
            break
        
    return(ticker, fecha_desde, fecha_hasta)

# Funcion para request a la api de stocks
def api_request (ticker, fecha_desde, fecha_hasta):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{fecha_desde}/{fecha_hasta}?adjusted=true&sort=desc&limit=5000&apiKey=6sXhLR8h0V1oqNzAgef37YroK7AJKmYE'

    access_token = '6sXhLR8h0V1oqNzAgef37YroK7AJKmYE'
    headers_auth= {"Authorization" : "Bearer {token}".format(token=access_token)}
    try:
        response = requests.get(url, headers=headers_auth)
        response_stat = response.status_code
        if response_stat == 400:
            raise Exception('Error en conexion, verifique la informacion y vuelva a intentar')
        
        data = response.content
        return(data)
    except:
        print(Exception)
        
# Funcion para request a la api de stocks, parametros para tiempo real
def api_request_plot (ticker, fecha_desde, fecha_hasta):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{fecha_desde}/{fecha_hasta}?adjusted=true&sort=desc&limit=5000&apiKey=6sXhLR8h0V1oqNzAgef37YroK7AJKmYE'

    access_token = '6sXhLR8h0V1oqNzAgef37YroK7AJKmYE'
    headers_auth= {"Authorization" : "Bearer {token}".format(token=access_token)}
    
    try:
        response = requests.get(url, headers=headers_auth)

        print(response.status_code)
        data = response.content
        return(data)
    except:
        print("Error de conexion, vuelva a intentar.")    

# Funcion que prepara los datos para cargar en la base de datos
def data_wrangling (df_results, ticker):
    df_results.rename(columns={'v':'Volume',
                            'vw':'volume_weighted',
                            'o':'Open',
                            'c':'Close',
                            'h':'High',
                            'l':'Low',
                            't':'timestamp',
                            'n':'n_transactions'},
                             inplace=True)

    df_results['timestamp']=pd.to_datetime(df_results['timestamp'], unit='ms')
    df_results["Dates"] = pd.to_datetime(df_results['timestamp']).dt.date
    df_results['Time'] = pd.to_datetime(df_results['timestamp']).dt.time
    df_results['Ticker'] = ticker
    return(df_results)

def data_wrangling_plot (df_results, ticker):
    df_results.rename(columns={'v':'Volume',
                            'vw':'volume_weighted',
                            'o':'Open',
                            'c':'Close',
                            'h':'High',
                            'l':'Low',
                            't':'timestamp',
                            'n':'n_transactions'},
                             inplace=True)

    df_results['timestamp']=pd.to_datetime(df_results['timestamp'], unit='ms')
    df_results["Dates"] = pd.to_datetime(df_results['timestamp'])
    df_results['Time'] = pd.to_datetime(df_results['timestamp'])
    df_results['Ticker'] = ticker
    df_results.sort_values(by=['Dates'], inplace=True, ascending=False)
    return(df_results)

def graficar (df_plot, ticker):
    fig = go.Figure(data=[go.Candlestick(x=df_plot['Dates'],
                open=df_plot['Open'],
                high=df_plot['High'],
                low=df_plot['Low'],
                close=df_plot['Close'])])
    
    fig.update_layout(
        title=f'{ticker} historico candlesticks',
        yaxis_title=f'{ticker} Stock en USD')

    fig.show()
    