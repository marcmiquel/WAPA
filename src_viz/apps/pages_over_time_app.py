# -*- coding: utf-8 -*-
import sys
import dash_apps_wapa
sys.path.insert(0, '/srv/wapa/src_viz')
from dash_apps_wapa import *


"""

# flash dash
import flask
from flask import Flask, request, render_template
from flask import send_from_directory
from dash import Dash
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
# viz
import plotly
import chart_studio.plotly as py
import plotly.figure_factory as ff
# data
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import pandas as pd
import sqlite3
import xlsxwriter
# other
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time

# script
import wikilanguages_utils


import requests



##### METHODS #####
# parse
def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    print (state)
    return state

# layout
def apply_default_value(params):
    def wrapper(func):
        def apply_value(*args, **kwargs):
            if 'id' in kwargs and kwargs['id'] in params:
                kwargs['value'] = params[kwargs['id']]
            return func(*args, **kwargs)
        return apply_value
    return wrapper



def save_dict_to_file(dic):
    f = open('databases/'+'dict.txt','w')
    f.write(str(dic))
    f.close()

def load_dict_from_file():
    f = open('databases/'+'dict.txt','r')
    data=f.read()
    f.close()
    return eval(data)




# DASH APPS #
#########################################################
#########################################################
#########################################################

databases_path = 'databases/'

territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
languages = wikilanguages_utils.load_wiki_projects_information();

wikilanguagecodes = languages.index.tolist()

# wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'production')
# save_dict_to_file(wikipedialanguage_numberarticles) # using this is faster than querying the database for the number of articles in each table.





wikipedialanguage_numberarticles = load_dict_from_file()
for languagecode in wikilanguagecodes:
   if languagecode not in wikipedialanguage_numberarticles: wikilanguagecodes.remove(languagecode)


language_names = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    language_names[lang_name] = languagecode


closest_langs = wikilanguages_utils.obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, 4)

country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()

country_names_inv = {v: k for k, v in country_names.items()}

ISO31662_subdivisions_dict, subdivisions_ISO31662_dict = wikilanguages_utils.load_iso_31662_to_subdivisions()

for i in (set(languages.index.tolist()) - set(list(wikipedialanguage_numberarticles.keys()))):
    try: languages.drop(i, inplace=True); territories.drop(i, inplace=True)
    except: pass
print (wikilanguagecodes)


footbar = html.Div('')
navbar = html.Div('')


# # web
title_addenda = ' - Wikipedia Administrative Pages Analytics'
external_stylesheets = [dbc.themes.BOOTSTRAP, 'https://wdo.wmcloud.org/assets/bWLwgP.css']



#########################################################
#########################################################
#########################################################
"""







### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

dash_app_viz_pot = Dash(__name__, server = app_wapa, url_base_pathname= webtype + '/pages_over_time/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app_viz_pot = Dash(url_base_pathname = '/pages_over_time/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_viz_pot.config['suppress_callback_exceptions']=True

title = "Pages Over Time"

dash_app_viz_pot.title = title+title_addenda

print (title)

dash_app_viz_pot.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain the creation of different types of content over time. They depict both the accumulated articles and the new articles created on a monthly basis. The different types of content used for the analysis are: geographical entities (countries, subregions and regions), languages CCC, Top CCC Lists, and gender.
       '''),

    # dcc.Markdown('''
    #     The graphs answer the following questions:
    #     * What is the extent of the different types of content created and accumulated in Wikipedia language editions over time?
    #     * What Wikipedia language editions have created and accumulated more content of the different types over time?
    #     * What is the extent of the different types of content created and accumulated in the Wikipedia language editions in a specific month?
    #     * What Wikipedia language editions have created more content of the different types in a specific month?
    #    '''),

    html.Br(),

# ###


    dcc.Tabs([


        dcc.Tab(label='Wikipedia Over Time (Barchart)', children=[

        # 3 OVER TIME BARCHART
            html.Br(),

            # html.H5('Created and Accumulated Articles by Content Type Over Time in Wikipedia Language Editions'),
            dcc.Markdown('''* **What is the extent of the different types of content created and accumulated in Wikipedia language editions over time?**
             '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select a Wikipedia'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Content type'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Select the time aggregation'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Show absolute or relative values'),
            style={'display': 'inline-block','width': '300px'}),


            html.Br(),
            html.Div(
            dcc.Dropdown(
                id='lang_dropdown_barchart',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'French (fr)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='content_types_barchart',
                options = [{'label': k, 'value': k} for k in admin_page_types.values()],
                value = 'Regions',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='time_aggregation_barchart',
                options = [{'label': k, 'value': k} for k in ['Monthly','Quarterly','Yearly']],
                value = 'Yearly',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.RadioItems(
                id='show_absolute_relative_radio_barchart',
                options=[{'label':'Absolute','value':'Absolute'},{'label':'Relative','value':'Relative'}],
                value='Absolute',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            dcc.Graph(id = 'createdaccumulatedmonthly_barchart1'),
            dcc.Graph(id = 'createdaccumulatedmonthly_barchart2'),
            #html.Hr(),

            dcc.Markdown('''The barchart graphs show for a single Wikipedia language edition and for a selected type of content, the amount of articles and the percentage of for each of its entities that is both accumulated and created over time. Time is presented in the x-axis and it is possible to select the periods in which articles are aggregated (Yearly, Quarterly and Monthly). The stacked bars can take the whole y-axis or be proportional to the number of aggregated articles for that period of time. 

              The graph contains a range-slider on the bottom to select a specific period of time especially useful when the time aggregation is set to quarterly. It is possible to use predefined specific time selections by clicking on the labels 6M, 1Y, 5Y, 10Y and ALL (last six months, last year, last five years, last ten years and all the time). The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend.
             '''.replace('  ', '')),

        ]),


        dcc.Tab(label='Wikipedia Comparison (Time Series)', children=[

            # 4 OVER TIME TIME SERIES
            html.Br(),

            # html.H5('Wikipedia Language Editions By Monthly Created Articles On Any Content Type Over Time'),
            dcc.Markdown('''* 
               **What Wikipedia language editions have created and accumulated more content of the different types over time?**
             '''.replace('  ', '')),

            html.Br(),
            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('You can add or remove languages:'),
            style={'display': 'inline-block','width': '500px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='langgroup_dropdown_timeseries',
                options = [{'label': k, 'value': k} for k in lang_groups],
                disabled =False,
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='langgroup_box_timeseries',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                multi=False,
                style={'width': '790px'}
             ), style={'display': 'inline-block','width': '800px'}),

            html.Br(),

            html.Div(
            html.P('Select a content type'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('You can add or remove entities:'),
            style={'display': 'inline-block','width': '500px'}),
            html.Br(),



            html.Div(
            dcc.Dropdown(
                id='content_types_timeseries',
                options = [{'label': k, 'value': k} for k in ['Entire Wikipedia']],
                value = 'Regions',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='entities_box_timeseries',
                options = [{'label': k, 'value': j} for k,j in {}], #entities_list
                multi=True,
                style={'width': '790px'}
             ), style={'display': 'inline-block','width': '800px'}),
            html.Br(),

            html.Div(
            html.P('Show absolute or relative values'),
            style={'display': 'inline-block','width': '210px'}),

            html.Div(
            html.P('Compare entities in language / entities by language'),
            style={'display': 'inline-block','width': '400px'}),

            html.Br(),

            html.Div(
            dcc.RadioItems(
                id='show_absolute_relative_radio_timeseries',
                options=[{'label':'Absolute','value':'Absolute'},{'label':'Relative','value':'Relative'}],
                value='Absolute',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '200px'}
             ), style={'display': 'inline-block','width': '210px'}),

            html.Div(
            dcc.RadioItems(
                id='show_compare_timeseries',
                options=[{'label':'Limit 1 language','value':'1Language'},{'label':'Limit 1 entity','value':'1Entity'}],
                value='1Language',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),

            html.Br(),

            dcc.Graph(id = 'createdaccumulatedmonthly_timeseries1'),
            dcc.Graph(id = 'createdaccumulatedmonthly_timeseries2'),

            dcc.Markdown('''The time series / line chart graphs show for a group of selected Wikipedia language editions and for specific entities of a type of content, the amount of articles and the percentage of each entity that has been both accumulated and created over time. The graphs allow selecting either one Wikipedia language edition and more than one entity from a content type or one single entity from a content type and more than one Wikipedia language edition in order to compare them over time.

            Time is presented in the x-axis and it is possible to select the periods in which articles are aggregated (Yearly, Quarterly and Monthly). The lines can be presented in the y-axis as a result of the number of aggregated articles for that period of time or the extent they take according to the total created or accumulated articles for that content type. The graph contains a range-slider on the bottom to select a specific period of time especially useful when the time aggregation is set to quarterly. It is possible to use predefined specific time selections by clicking on the labels 6M, 1Y, 5Y, 10Y and ALL (last six months, last year, last five years, last ten years and all the time). The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend.
             '''.replace('  ', '')),
            ]),


    ]),

    footbar,

], className="container")





#### FUNCTIONS AND CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


def dataframe_periods(df, order, entity, units):

    # print (order, entity, units)
    # print ('oh')
    # print (df.head(10))
    # print ('eh')


    if order == 'get_last_month_of_year':
        # df.to_csv('inici.csv')
        df = df.sort_values(by=['Wiki', entity, 'period'])

        old_year = 0
        index_to_delete = []
        index_list = []
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            cur_year = row['period'].year
            cur_entity = row[entity]

            if (cur_year != old_year and old_year!=0) or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                df.at[old_index, 'period_formatted'] = str(old_year)
                index_to_delete+= index_list
                index_list = []

            cur_index = i
            index_list.append(cur_index)
            old_year = cur_year
            old_index = cur_index
            old_entity = cur_entity

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, 'period_formatted'] = str(old_year)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=[entity,'period'])
        # df.to_csv('final.csv')


    if order == 'get_last_month_of_quarter':
        df = df.sort_values(by=['Wiki', entity, 'period'])

        index_to_delete = []
        index_list = []
        old_quarter = 0
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            pe = row['period']
            cur_month = pe.month
            cur_index = i
            cur_entity = row[entity]
            cur_quarter = pe.quarter

            if (cur_quarter != old_quarter and old_quarter!=0) or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
#                print (row)

            old_quarter = pe.quarter
            old_year = pe.year
            old_entity = cur_entity

            index_list.append(cur_index)
            old_index = cur_index

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])


    if order == 'add_articles_of_year':
        df = df.sort_values(by=[entity, 'period'])

        old_year = 0
        index_to_delete = []
        index_list = []
        units_val = 0
        old_entity = 0
        for i, row in df.iterrows():
            cur_year = row['period'].year
            cur_index = i
            cur_entity = row[entity]

            if (cur_year != old_year and old_year!=0)  or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                df.at[old_index, units] = units_val
                units_val = 0
                df.at[old_index, 'period_formatted'] = str(old_year)

            index_list.append(cur_index)
            units_val += row[units]
            old_year = cur_year
            old_index = cur_index
            old_entity = cur_entity

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, units] = units_val
        df.at[old_index, 'period_formatted'] = str(old_year)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])


    if order == 'add_articles_of_quarter':
        df = df.sort_values(by=[entity, 'period'])

        index_to_delete = []
        index_list = []
        units_val = 0
        old_quarter = 0
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            pe = row['period']
            cur_month = pe.month
            cur_index = i
            cur_entity = row[entity]
            cur_quarter = pe.quarter

            if (cur_quarter != old_quarter and old_quarter!=0)  or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                # print (df.loc[old_index])
                df.at[old_index, units] = units_val
                df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
                # print (df.loc[old_index])
                units_val = 0

            old_quarter = cur_quarter
            old_year = pe.year
            old_entity = cur_entity

            index_list.append(cur_index)
            units_val += row[units]
            old_index = cur_index

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, units] = units_val
        df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])

    df['period']=df['period_formatted']
    return df



def create_fig_barchart(created_accumulated, df, time_aggregation, content_type, absolute_relative, entities_list):

    fig = go.Figure()

    if absolute_relative == 'Absolute':
        customdata = 'Extent Articles (%)'
        y = 'Articles'
        hovertemplate_extent = '<br>Extent Articles: %{customdata}%'
        hovertemplate_articles = '<br>Articles: %{y}'
        yaxis = content_type+' Entities by Number of Articles'
    else:
        customdata = 'Articles'
        y = 'Extent Articles (%)'
        hovertemplate_extent = '<br>Extent Articles: %{y}%'
        hovertemplate_articles = '<br>Articles: %{customdata}'
        yaxis = content_type+' Entities by Percentage of Articles'


    if time_aggregation != '':
        titletext = time_aggregation+' '+created_accumulated+' on '+content_type
        title = 'Periods of Time ('+time_aggregation+')'


        for entity_name in entities_list:
            d = df.loc[(df['Entity'] == entity_name)]

            fig.add_trace(go.Bar(
                customdata=d[customdata],
                y = d[y],
                x=d['period'],
                name=entity_name,
                texttemplate='%{y}',
                hovertemplate=str(entity_name)+hovertemplate_articles+hovertemplate_extent+'<br>Period: %{x}<br><extra></extra>',
            ))

    else:
        if content_type != '':
            titletext = created_accumulated+' on '+content_type
        else:
            titletext = created_accumulated            
        title = 'Periods of Time'
#        df = df.drop(df.index, inplace=True)

    fig.update_layout(
        xaxis=dict(
            title=title,
            titlefont_size=12,
            tickfont_size=10,

            rangeselector=dict(
                buttons=list([
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(count=5,
                         label="5y",
                         step="year",
                         stepmode="backward"),
                    dict(count=10,
                         label="10y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
            visible = True
            ),
            type="date"
        ),
        yaxis=dict(
            title=yaxis,
            titlefont_size=12,
            tickfont_size=10,
        ),
        title_font_size=12,
        title_text=titletext,
        legend = dict(
            font=dict(
#                family="sans-serif",
                size=12
#                color="black"
            ),
            traceorder="normal"
            ),
        autosize=True,
        height = 500,
        width=1200,
        barmode='stack')

    return fig



# MONTHLY BARCHART
@dash_app_viz_pot.callback(
    [Output('createdaccumulatedmonthly_barchart1', 'figure'), Output('createdaccumulatedmonthly_barchart2', 'figure')], [Input('lang_dropdown_barchart', 'value'), Input('content_types_barchart', 'value'), Input('time_aggregation_barchart', 'value'), Input('show_absolute_relative_radio_barchart','value')])
def update_monthly_barchart(language, content_type, time_aggregation, absolute_relative):

#    print (language, content_type, time_aggregation, absolute_relative)

    if time_aggregation == None: time_aggregation = ''
    if language == None: language = ''
    if content_type == None: content_type = ''

    functionstartTime = time.time()

    if content_type != '':
        ct = content_type_dict[content_type]
        conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() #

#        print (content_type)

        query, params = params_to_df(language_names[language], ct, None, 'monthly')
#        print (query, params)
        df_created = pd.read_sql_query(query, conn, params = params).round(1)
        df_created = df_extended(df_created, ct)

        query, params = params_to_df(language_names[language], ct, None, 'accumulated')
#        print (query)
        df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
        df_accumulated = df_extended(df_accumulated, ct)

        # print('abans de periods'+str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        # print (df_created.head(10))
        # print (df_accumulated.head(10))

        # df_created.to_csv('cr.csv')
        # df_accumulated.to_csv('acc.csv')


        if time_aggregation == 'Quarterly': df_created = dataframe_periods(df_created, 'add_articles_of_quarter','Entity','Articles') # CREATED QUARTER

        if time_aggregation == 'Quarterly': df_accumulated = dataframe_periods(df_accumulated, 'get_last_month_of_quarter','Entity','Articles') # ACCUMULATED QUARTER

        if time_aggregation == 'Yearly': df_created = dataframe_periods(df_created, 'add_articles_of_year','Entity','Articles') # CREATED YEAR

        if time_aggregation == 'Yearly': df_accumulated = dataframe_periods(df_accumulated, 'get_last_month_of_year','Entity','Articles') # ACCUMULATED YEAR

        # print (df_created.head(10))
        # print (df_accumulated.head(10))


        df_accumulated = df_accumulated.sort_values(by=['Articles','Entity','period'], ascending = False)
    
        entities_list = list(df_accumulated.Entity.unique())

        df_accumulated['Period Articles'] = df_accumulated['period'].map(df_accumulated.groupby('period').Articles.sum())
        df_accumulated['extent'] = 100*df_accumulated['Articles']/df_accumulated['Period Articles']
        df_accumulated['Extent Articles (%)'] = df_accumulated['extent'].round(1)
    
        df_accumulated = df_accumulated.loc[(df_accumulated['Entity'].isin(entities_list[:15]))]

        df_created = df_created.sort_values(by=['Articles','Entity','period'], ascending = False)#, reverse=True)
        df_created['Period Articles'] = df_created['period'].map(df_created.groupby('period').Articles.sum())
        df_created['Extent Articles (%)'] = 100*df_created['Articles']/df_created['Period Articles']
        df_created = df_created.round(1)


        df_created['period'] = pd.to_datetime(df_created['period'])
        df_accumulated['period'] = pd.to_datetime(df_accumulated['period'])


    fig = create_fig_barchart('Accumulated Articles', df_accumulated, time_aggregation, content_type, absolute_relative, entities_list)
    fig2 = create_fig_barchart('Created Articles', df_created, time_aggregation, content_type, absolute_relative, entities_list)

#    print('després de generar figs'+str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    return (fig,fig2)



########################

def create_fig_timeseries(created_accumulated, df, content_type, absolute_relative):
    fig = go.Figure()

    if absolute_relative == 'Absolute':
        customdata = 'Extent Articles (%)'
        y = 'Articles'
        hovertemplate_extent = '<br>Extent Articles: %{customdata}%'
        hovertemplate_articles = '<br>Articles: %{y}'
        yaxis = 'Number of Articles ' + created_accumulated
    else:
        customdata = 'Articles'
        y = 'Extent Articles (%)'
        hovertemplate_extent = '<br>Extent Articles: %{y}%'
        hovertemplate_articles = '<br>Articles: %{customdata}'
        yaxis = 'Percentage of Articles ' + created_accumulated

    fig = go.Figure()
    for entity_name in list(df["Entity (Wiki)"].unique()):
#        print (entity_name)
        d = df.loc[(df["Entity (Wiki)"] == entity_name)]

        fig.add_trace(go.Scatter(
            customdata=d[customdata],
            y = d[y],
            x=d['period'],
            name=entity_name,
            hovertemplate=str(entity_name)+hovertemplate_articles+hovertemplate_extent+'<br>Period: %{x}<br><extra></extra>'
        ))

    fig.update_layout(
        xaxis=dict(
            title='Time (Monthly)',
            titlefont_size=12,
            tickfont_size=10,

            rangeselector=dict(
                buttons=list([
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(count=5,
                         label="5y",
                         step="year",
                         stepmode="backward"),
                    dict(count=10,
                         label="10y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                        ])
                    ),
                    rangeslider=dict(
                        visible=False
                    ),
                    type="date"

        ),
        yaxis=dict(
            title=yaxis,
            titlefont_size=12,
            tickfont_size=10,
        ),

        title_font_size=12,
        title_text=created_accumulated+' Articles on '+content_type,
        legend = dict(
            font=dict(
#                family="sans-serif",
                size=12
#                color="black"
            ),
            traceorder="normal"
            ),
        autosize=True,
        height = 500,
        width=1200)

    return fig


# MONTHLY TIME SERIES
@dash_app_viz_pot.callback(
    [Output('createdaccumulatedmonthly_timeseries1', 'figure'), Output('createdaccumulatedmonthly_timeseries2', 'figure')],
    [Input('langgroup_box_timeseries', 'value'),
    Input('content_types_timeseries', 'value'), 
    Input('entities_box_timeseries', 'value'), 
    Input('show_absolute_relative_radio_timeseries','value')])
def update_monthly_time_series(languages, content_type, entities, absolute_relative):

    if entities == None:
        entities = []
    if languages == None:
        languages = []

#    print (languages, content_type, entities, absolute_relative, 'update_monthly_time_series')
    langs = []
    if type(languages) != str:
        for x in languages: langs.append(language_names[x])
    else:
        langs.append(language_names[languages])

    ents = []
    if type(entities) != str:
        try:
            for x in entities: ents.append(language_name_wiki[x])
        except:
            ents = entities
    else:
        try:
            ents = [language_name_wiki[entities]]
        except:
            ents = [entities]



    ct = content_type_dict[content_type]
    conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() #

    query, params = params_to_df(langs, ct, None, 'monthly')
    df_created = pd.read_sql_query(query, conn, params = params).round(1)
    df_created = df_extended(df_created, ct)

    query, params = params_to_df(langs, ct, None, 'accumulated')
    df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
    df_accumulated = df_extended(df_accumulated, ct)

    # # ACCUMULATED
    # df_accumulated = pd.DataFrame()
    # df_accumulated = df_accumulated_dict[content_type]
    # df_accumulated = df_accumulated.loc[(df_accumulated['Wiki'].isin(langs)) & (df_accumulated['Entity'].isin(ents))]
    df_accumulated = df_accumulated.loc[(df_accumulated['Entity'].isin(ents))]

    # # CREATED
    # df_created = pd.DataFrame()
    # df_created = df_created_dict[content_type]
    # df_created = df_created.loc[(df_created['Wiki'].isin(langs)) & (df_created['Entity'].isin(ents))]
    df_created = df_created.loc[(df_created['Entity'].isin(ents))]

    df_accumulated["Entity (Wiki)"] = df_accumulated["Entity"]+" ("+df_accumulated["Wiki"]+")"
    df_created["Entity (Wiki)"] = df_created["Entity"]+" ("+df_created["Wiki"]+")"

    fig = create_fig_timeseries('Accumulated', df_accumulated, content_type, absolute_relative)
    fig2 = create_fig_timeseries('Created', df_created, content_type, absolute_relative)

    return fig, fig2


########################


# Dropdown languages
@dash_app_viz_pot.callback(
    Output('langgroup_box_timeseries', 'value'),
    [Input('langgroup_dropdown_timeseries', 'value'), 
    Input('show_compare_timeseries','value')])
def set_langs_group_options_time_series(selected_group, compare):

#    print (selected_group, compare, 'set_langs_group_options_time_series')
    if compare == '1Language':
        return []

    if compare == '1Entity' and selected_group != None and len(selected_group)!=0:
        langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
        available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
        list_options = []
        for item in available_options: list_options.append(item['label'])
        return sorted(list_options,reverse=False)
#        return ['Catalan (ca)','French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)']




    
# if __name__ == '__main__':
#     dash_app_viz_pot.run_server(debug=True)#,dev_tools_ui=False)
