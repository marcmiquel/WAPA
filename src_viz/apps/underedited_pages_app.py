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
def dash_apps_wapa.apply_default_value(params):
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


####### -------------------- This is the beginning of the App.


### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_tool_rfg = Dash(__name__, server = app_wapa, url_base_pathname = webtype + '/underedited_pages/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
#dash_app_tool_rfg = Dash(url_base_pathname = '/red_flags/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_tool_rfg.config['suppress_callback_exceptions']=True

app_title = 'Underedited Pages'
dash_app_tool_rfg.title = app_title+title_addenda

print (app_title)

dash_app_tool_rfg.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])





features_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Inlinks from CCC':'num_inlinks_from_CCC','Outlinks to CCC':'num_outlinks_to_CCC', 'Creation Date':'date_created', 'Start Year':'start_time', 'End Year':'end_time'}

features_dict_inv = {v: k for k, v in features_dict.items()}



topic_dict={'All':'all','Keywords':'keywords','Geolocated':'geolocated','People':'people','Women':'women','Men':'men','Folk':'folk','Earth':'earth','Monuments and Buildings':'monuments_and_buildings','Music Creations and Organizations':'music_creations_and_organizations','Sports and Teams':'sport_and_teams','Food':'food','Paintings':'paintings','GLAM':'glam','Books':'books','Clothing and Fashion':'clothing_and_fashion','Industry':'industry', 'CCC':'ccc', 'Time interval':'time_interval'}

topic_dict_inv = {v: k for k, v in topic_dict.items()}


target_langs_dict = language_names

show_gaps_dict = {'No language gaps':'no-gaps','At least one gap':'one-gap-min','Only gaps':'only-gaps'}






text_results = '''
The following table shows the resulting list of administrative pages with potential "Red Flags". The Qitem column provides the id and a link to the Wikidata corresponding page when it exists. The column Title provides the title in the selected language. The next columns (creation date, Bytes, pageviews, interwiki, inlinks, and edits) show the value for some features. The following columns show the metrics selected in the dropdown menus (max. of 3) and the ratios (max. of 3).
'''


text_default = '''On this page, you can screen and find specific administrative pages in Wikipedia language editions based on page characteristics related to their development, inclusion, and participation. 

Use different metrics and ratios in order to find “Red Flags” that may indicate that a page is valuable but needs attention. We define a red flag in an admin page when a page has a value for a ratio that is much higher or lower than the other pages and thus calls for editor attention. We compute ratios between the different dimensions and their metrics. For example: completeness-popularity (number of Bytes/number of pageviews in the last month), activity-relevance (number of edits last month/number of inlinks), activity-disagreement (number of edits/number of reverts), or recency-popularity (number of days since the last 5 edits were made/number of pageviews in the last month).'''


interface_row1 = html.Div([
    html.Div(
    [
    html.P(
        [
            "Source ",
            html.Span(
                "language",
                id="tooltip-target-sourcelanguage",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a source language to retrieve a list of articles from its 'local content' or CCC.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-sourcelanguage",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])



interface_row2 = html.Div([

    html.Div(
    [
    html.P(
        [
            html.Span(
                "Topic",
                id="tooltip-target-topic",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '200px'},
    ),

    dbc.Tooltip(
        html.P("Select a Topic to filter the resulting articles to CCC, general topics, people, etc.",
        style={"width": "47rem", 'font-size': 12, 'text-align':'left','backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-topic",
        placement="bottom",
        style={'color':'black','backgroundColor':'transparent'},
    ),

    html.Div(
    [
    html.P(
        [
            html.Span(
                "Start year",
                id="tooltip-target-startyear",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "The articles/wikidata qitems in the results will have a property with a value at least equal or higher than this year of start.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-startyear",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '140px'},
    ),


    html.Div(
    [
    html.P(
        [
            html.Span(
                "End year",
                id="tooltip-target-endyear",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "The articles/wikidata qitems in the results will have a property with a value at least equal or lower than this year of finish.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-endyear",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '140px'},
    ),

    ])



interface_row3 = html.Div([

    html.Div([
    html.P(
        [
            "Order by ",
            html.Span(
                "feature",
                id="tooltip-target-order_by",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature to sort the results.",
        style={"width": "22rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-order_by",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),





    html.Div(
    [
    html.P(
        [   
            "Show the ",
            html.Span(
                "gaps",
                id="tooltip-target-show_gaps",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Show the articles which already exist or do not exist in the target language.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-show_gaps",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),




    html.Div(
    [
    html.P(
        [
            "Limit the ",
            html.Span(
                "results",
                id="tooltip-target-limit",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Choose a number of results (by default 100)",
        style={"width": "24rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    )

])




def dash_app_tool_rfg_build_layout(params):

    #  ['source_lang','target_langs','topic','time_presets','start_year','end_year','order_by','limit','show_gaps']

    if len(params)!=0 and params['target_langs'].lower()!='none' and params['source_lang'].lower()!='none':
   
        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cur = conn.cursor()

        # SOURCE lANGUAGE
        source_lang = params['source_lang'].lower()
        source_language = languages.loc[source_lang]['languagename']

        # TARGET LANGUAGES
        target_langs = params['target_langs'].lower()
        target_langs = target_langs.split(',')
        target_language = languages.loc[target_langs[0]]['languagename']


        # CONTENT
        if 'topic' in params:
            topic = params['topic']
        else:
            topic = 'none'

        # FILTER
        if 'order_by' in params:
            order_by = params['order_by']
        else:
            order_by = 'none'


        if 'start_year' in params:
            start_year = int(params['start_year'])
        else:
            start_year = 'none'

        if 'end_year' in params:
            end_year = int(params['end_year'])
        else:
            end_year = 'none'

        if 'time_presets' in params:
            time_presets = params['time_presets']
        else:
            time_presets = 'none'


        if 'limit' in params:
            try:
                limit = int(params['limit'])
            except:
                limit = 100
        else:
            limit = 100

        try:
            show_gaps = params['show_gaps']
        except:
            show_gaps = 'none'


        # CREATING THE QUERY FROM THE PARAMS
        query = 'SELECT '
        query += 'r.qitem, '

#        query += 'REPLACE(r.page_title,"_"," ") as r.page_title, '
        query += 'r.page_id as page_id, r.page_title as page_title, '

        query += 'r.num_editors, r.num_edits, r.num_pageviews, r.num_interwiki, r.num_bytes, r.start_time, r.end_time, '

        columns = ['num','qitem','page_title','num_editors','num_edits','num_pageviews','num_interwiki','num_bytes','date_created','start_time','end_time']

        if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references']: 
            query += 'r.'+order_by+' '
            columns = columns + [order_by]

        query += 'r.date_created '
        query += 'FROM '+source_lang+'wiki r '

        query += 'WHERE (r.start_time IS NOT NULL OR r.end_time IS NOT NULL) '


        if start_year != 'none':
            query+= 'AND r.start_time >= '+str(start_year)+' '

        if end_year != 'none':
            query+= 'AND r.end_time <= '+str(end_year)+' '


        if topic != "none" and topic != "None" and topic != "all":
            if topic == 'ccc':
                query += 'AND r.ccc_binary = 1 '
            elif topic == 'geolocated':
                query += 'AND (r.geocoordinates IS NOT NULL OR r.location_wd IS NOT NULL) '
            elif topic == 'men': # male
                query += 'AND r.gender = "Q6581097" '
            elif topic == 'women': # female
                query += 'AND r.gender = "Q6581072" '
            elif topic == 'people':
                query += 'AND r.gender IS NOT NULL '
            elif topic == 'not_people':
                query += 'AND r.gender IS NULL '
            elif topic == 'time_interval':
                query += 'AND r.time_interval IS NOT NULL '               
            else:
                query += 'AND r.'+topic+' IS NOT NULL '


        if order_by == "none" or order_by == "None":
#            pass
            query += 'ORDER BY r.num_pageviews DESC '

        elif order_by in ['num_outlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references','num_pageviews']: 
            query += 'ORDER BY r.'+order_by+' DESC '

        if limit == "none":
            query += 'LIMIT 500;'
        else:
            query += 'LIMIT 500;'
            # query += 'LIMIT '+str(limit)+';'

#        columns = columns + ['target_langs']
        print (query)
 

        df = pd.read_sql_query(query, conn)#, parameters)
        print (df.head(10))


        """
        # for the target languages
        df = df.head(limit)
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(source_lang); mysql_cur_read = mysql_con_read.cursor()
        df = wikilanguages_utils.get_interwikilinks_articles(sourcelang, target_langs, df, mysql_con_read)
        """

        df = df.fillna(0)
        if order_by == "none" or order_by == "None":
            df = df.sort_values(by='start_time', ascending=False)
        else:
            df = df.sort_values(by=order_by, ascending=False)



        columns_dict = {'num':'Nº','page_title':source_language+' Title','target_langs':'Target Langs.','qitem':'Qitem'}
        columns_dict.update(features_dict_inv)



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([
                navbar,

                html.H3(title, style={'textAlign':'center'}),
                html.Br(),

                dcc.Markdown(
                    text_default.replace('  ', '')),


                # HERE GOES THE INTERFACE
                # LINE 
                html.Br(),
                html.H5('Select the source'),

                interface_row1,
                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='ca',
                    placeholder="Select a source language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
        #        dcc.Link('Query',href=""),



                html.Br(),
                html.H5('Select the topic and time'),
                interface_row2,
                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                    id='start_year',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='',
                    style={'width': '120px'}
                 ), style={'display': 'inline-block','width': '130px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                    id='end_year',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='',
                    style={'width': '120px'}
                 ), style={'display': 'inline-block','width': '130px'}),


                # LINE 
                html.Br(),
                html.H5('Filter the results'),
                interface_row3,
                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='show_gaps',
                    options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                    value='none',
                    placeholder="Show the gaps (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '90px'}
                 ), style={'display': 'inline-block','width': '100px'}),

                ###            

                html.Div(
                html.A(html.Button('Query Results!'),
                    href=''),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Br(),

                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                # footbar,

                footbar,     

            ], className="container")

            return layout




        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
        print (df.columns)
#        print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')

        # # PREPARE THE DATA
        df=df.rename(columns=columns_dict)
        order_by = order_by.lower()
        if order_by != 'none':
            order = features_dict_inv[order_by]
        else:
            order = features_dict_inv['start_time']
        df = df.sort_values(order,ascending=False)


        print (columns)
        print (df.head(100))

        # input('')
        columns_ = []
        for x in columns:
            columns_.append(columns_dict[x])
        columns = columns_

#        columns.append(target_language+' Title')

        df_list = list()
        k = 0
        z = 0
        for index, rows in df.iterrows():
            df_row = list()

            for col in columns:
                title = rows[source_language+' Title']
                if not isinstance(title, str):
                    title = title.iloc[0]

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == source_language+' Title':
                    title = rows[source_language+' Title']
                    if not isinstance(title, str):
                        title = title.iloc[0]
                    df_row.append(html.A(title.replace('_',' '), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == target_language+' Title':

                    t_title = rows[target_language+' Title']

                    if isinstance(t_title, int): 
                        df_row.append('')

                    elif not isinstance(t_title, str):
                        t_title = t_title.iloc[0]

                    if isinstance(t_title, str):  
                        df_row.append(html.A(t_title.replace('_',' '), href='https://'+target_langs[0]+'.wikipedia.org/wiki/'+t_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Outlinks' or col == 'References' or col == 'Images':
                    title = rows[source_language+' Title']
                    df_row.append(html.A( rows[col], href='https://'+target_langs[0]+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows[source_language+' Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks from CCC':
                    df_row.append(html.A( rows['Inlinks from CCC'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows[source_language+' Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Outlinks from CCC':
                    df_row.append(html.A( rows['Outlinks from CCC'], href='https://'+source_lang+'.wikipedia.org/wiki/'+rows[source_language+' Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows[source_language+' Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows[source_language+' Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    df_row.append(html.A( rows['Discussions'], href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows[source_language+' Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikirank':
                    df_row.append(html.A( rows['Wikirank'], href='https://wikirank.net/'+source_lang+'/'+rows[source_language+' Title'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+source_lang+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows[source_language+' Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    title = rows[source_language+' Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Creation Date':
                    date = rows['Creation Date']
                    if date == 0: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)

                elif col == 'Start Year':
                    date = rows['Start Year']
                    df_row.append(date)

                elif col == 'End Year':
                    date = rows['End Year']
                    df_row.append(date)


                elif col == 'Target Langs.':

                    z=0
                    target_langs_titles = []
                    for i in range(1,len(target_langs)+1):
                        if rows['page_title_'+str(i)] == '':
                            z+=1
                        target_langs_titles.append(rows['page_title_'+str(i)])
#                    print (target_langs_titles)

                    text = ''

                    for x in range(0,len(target_langs)):
                        cur_title = target_langs_titles[x]
                        if cur_title!= None and cur_title != '' and cur_title != 0:
                            if text!='': text+=', '

                            text+= '['+target_langs[x]+']'+'('+'http://'+target_langs[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'
    
                    df_row.append(dcc.Markdown(text))

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))
                else:
                    df_row.append(rows[col])

            # print (rows)
            # print (len(rows))
            # print (len(df_row))

            if show_gaps == 'one-gap-min' and z == 0: continue
            elif show_gaps == 'only-gaps' and z < len(target_langs_titles): continue
            elif show_gaps == 'no-gaps' and z > 0: continue
            
            if k <= limit:
                df_list.append(df_row)




#        print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')
#        print (df.head())

        # RESULTS

        df1 = pd.DataFrame(df_list)
        dash_app_tool_rfg.title = app_title+title_addenda

        # LAYOUT
        layout = html.Div([
            navbar,          
            html.H3(app_title, style={'textAlign':'center'}),
#            html.Br(),

            dcc.Markdown(
                text_default.replace('  ', '')),

            html.Br(),



            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the source'),

            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='ca',
                placeholder="Select a source language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
            #        dcc.Link('Query',href=""),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': target_langs_dict[i]} for i in sorted(target_langs_dict)],
                value=['es','ca'],
                multi=True,
                placeholder="Select languages",           
                style={'width': '470px'}
             ), style={'display': 'inline-block','width': '490px'}),


            html.Br(),
            html.H5('Select the topic and time'),
            interface_row2,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='topic',
                options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                value='none',
                placeholder="Select a topic",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='start_year',                    
                placeholder='Enter a value...',
                type='text',
                value='',
                style={'width': '120px'}
             ), style={'display': 'inline-block','width': '130px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='end_year',                    
                placeholder='Enter a value...',
                type='text',
                value='',
                style={'width': '120px'}
             ), style={'display': 'inline-block','width': '130px'}),


            # LINE 
            html.Br(),
            html.H5('Filter the results'),
            interface_row3,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show the gaps (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='100',
                style={'width': '90px'}
             ), style={'display': 'inline-block','width': '100px'}),

            html.A(html.Button('Query Results!'),
                href=''),
           


            # here there is the table            
            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(text_results.replace('  ', '')),
            html.Br(),

            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(df_row[x]) for x in range(len(columns))
            ]) for df_row in df_list]),

            footbar,     

        ], className="container")
#        print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' before printing')



    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            navbar,
            
            html.H3(app_title, style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the source'),

            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='it',
                placeholder="Select a source language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': target_langs_dict[i]} for i in sorted(target_langs_dict)],
                value=['es','oc','fr'],
                multi=True,
                placeholder="Select languages",           
                style={'width': '470px'}
             ), style={'display': 'inline-block','width': '490px'}),


            html.Br(),
            html.H5('Select the topic and time'),
            interface_row2,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='topic',
                options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                value='none',
                placeholder="Select a topic",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='start_year',                    
                placeholder='Enter a value...',
                type='text',
                value='',
                style={'width': '120px'}
             ), style={'display': 'inline-block','width': '130px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='end_year',                    
                placeholder='Enter a value...',
                type='text',
                value='',
                style={'width': '120px'}
             ), style={'display': 'inline-block','width': '130px'}),


            # LINE 
            html.Br(),
            html.H5('Filter the results'),
            interface_row3,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show the gaps (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='100',
                style={'width': '90px'}
             ), style={'display': 'inline-block','width': '100px'}),


            html.A(html.Button('Query Results!'),
                href=''),

            html.Br(),
            html.Br(),

            footbar,           

        ], className="container")

    return layout



# callback update URL
component_ids_rfg = ['source_lang','target_langs','topic','start_year','end_year','order_by','limit','show_gaps']
@dash_app_tool_rfg.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_rfg])
def update_url_state(*values):

    if not isinstance(values[1], str):
        values = values[0],','.join(values[1]),values[2],values[3],values[4],values[5],values[6]

    state = urlencode(dict(zip(component_ids_rfg, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app_tool_rfg.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps_wapa.parse_state(href)
    return dash_app_tool_rfg_build_layout(state)


    
# if __name__ == '__main__':
#     dash_app_tool_rfg.run_server(debug=True)#,dev_tools_ui=False)
