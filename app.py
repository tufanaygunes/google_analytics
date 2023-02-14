#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 14:00:02 2023

@author: tufanaygunes
"""
from googleapiclient.discovery import build
from google.oauth2 import service_account
import numpy as np
import pandas as pd
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import requests
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
from dotenv import load_dotenv
import os 
import dash_auth
import time
from dash import dash_table


load_dotenv()

def initialize_analyticsreporting(id):
    VIEW_ID=id
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = '/etc/secrets/ga_keys.json'
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics,id,start='7daysAgo'):
    START_DATE=start
    END_DATE='today'
    return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': id,
          'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}],
          'metrics': [{'expression': 'ga:pageviews'}],
          'dimensions': [{'name': 'ga:date'},{'name': 'ga:pagePath'}]
        }]
      }
  ).execute()

def print_response(response):
    list = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        for row in rows:
            dict = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])
            for header, dimension in zip(dimensionHeaders, dimensions):
                dict[header] = dimension
            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
                    if ',' in value or '.' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                        dict[metric.get('name')] = int(value)
            list.append(dict)
        df = pd.DataFrame(list)
        return df

# df cleaning and preparation
def cleaning(df):
    df.rename(columns={'ga:pagePath':"path",'ga:pageviews':"Pageviews",'ga:date':"date"},inplace=True)
    nn=df[df['path'].isnull()].index
    df.drop(nn,inplace=True)
    df['local']=df.path.str.extract("locale=([a-z]*)", expand=True)
    df['keyword']=df.path.str.extract("surface_detail=([a-zA-Z+ -]*)", expand=True)
    df['keyword']=df['keyword'].str.lower()
    df['keyword']=df['keyword'].str.replace('-',' ',regex=True)
    df['keyword']=df['keyword'].str.replace('+',' ',regex=True)
    df['keyword']=df['keyword'].str.strip()
    df['page']=df.path.str.extract("surface_inter_position=(\d*)", expand=True)
    df['rank']=df.path.str.extract("surface_intra_position=(\d*)", expand=True)
    df['type']=df.path.str.extract("&surface_type=(\w*)", expand=True)
    nnn=df[df['path'].isnull()].index
    df.drop(nnn,inplace=True)
    nn=df[df['page'].isnull()].index
    df.drop(nn,inplace=True)
    xxx=df[df['keyword'].isnull()].index
    df.drop(xxx,inplace=True)
    df['page']=df['page'].astype(int)
    df['rank']=df['rank'].astype(int)
    df['sort']=((df['page']-1)*24)+df['rank']
    return df

def float_n(df,column):
    df[column]=df[column].apply(lambda x:round(x,2))
    return df

def run_once():
    VIEW_ID = '252616533'
    response3 = get_report(initialize_analyticsreporting(VIEW_ID),VIEW_ID)
    df3=print_response(response3)
    cleaning(df3)
    dff3=df3.groupby(['keyword','type']).agg({'sort':'mean','Pageviews':'sum'}).rename(columns={"sort":"avg_position"}).reset_index()
    float_n(dff3,'avg_position')
    return dff3.to_dict('records')

# dash setup and visuzalization

VALID_USERNAME_PASSWORD_PAIRS = {
    os.getenv("USER_NAME"): os.getenv("PASSWORD")
}

app = Dash(__name__,prevent_initial_callbacks=True, suppress_callback_exceptions=True)
server = app.server
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
) 

app.layout = html.Div(children=[
    html.Div(children=[dcc.Dropdown(id='data-set-chosen', multi=False, value='252616533',
                                    options=[{'label':'Product Page Banner','value':'277875383'},
                                            {'label':'Low Stock Counter','value':'252616533'}])],style={'margin-bottom': '20px', 'width': '20%'}),
    html.Div(id='time2', children=[dcc.Dropdown(id='time', multi=False, value='7daysAgo',
                                    options=[{'label':'7daysAgo','value':'7daysAgo'},
                                            {'label':'30daysAgo','value':'30daysAgo'}])],style={'margin-bottom': '20px', 'width': '20%'}),                                        
    html.Div(id='datatable-interactivity-data', children=[]),
    html.Div(id='datatable-initial-show', children=[dash_table.DataTable(
        id='datatable-initial',
        columns=[
            {"name": i, "id": i} for i in run_once()[0]
        ],
        data=run_once(),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 20,
    )]),
    html.Div(id='datatable-interactivity-container'),
    dcc.Store(id='store-data',data=[],storage_type='memory')
],style={'width': '60vw', 'margin': 'auto','margin-top':'100px'})

@app.callback(
    Output('store-data', 'data'),
    Input('data-set-chosen', 'value'),
    Input('time', 'value')  
)
def store_data(value,id):
    if value == '252616533':
        VIEW_ID = '252616533'
        response = get_report(initialize_analyticsreporting(value),value,id)
        df=print_response(response)
        cleaning(df)
        dff=df.groupby(['keyword','type']).agg({'sort':'mean','Pageviews':'sum'}).rename(columns={"sort":"avg_position"}).reset_index()
        float_n(dff,'avg_position')
        return dff.to_dict('records')
    else:
        VIEW_ID = '277875383'
        response2 = get_report(initialize_analyticsreporting(value),value,id)
        df2=print_response(response2)
        cleaning(df2)
        dff2=df2.groupby(['keyword','type']).agg({'sort':'mean','Pageviews':'sum'}).rename(columns={"sort":"avg_position"}).reset_index()
        float_n(dff2,'avg_position')
        return dff2.to_dict('records')

@app.callback(
    Output('datatable-initial-show', 'style'),
    Input('data-set-chosen', 'value'),
    Input('time', 'value') 
)
def hide_initial(time,value):
    if value or time:
        return {'display':'none'}

@app.callback(
    Output('datatable-interactivity-data', 'children'),
    Input('store-data', 'data')
)
def store_data(data):
    return dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i} for i in data[0]
        ],
        data=data,
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 20)

#this is mine 


if __name__ == '__main__':
    app.run_server(debug=True)
