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

dash_app_viz_pc = Dash(__name__, server = app_wapa, url_base_pathname= webtype + '/page_characteristics/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app_viz_pc = Dash(url_base_pathname = '/page_characteristics/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_viz_pc.config['suppress_callback_exceptions']=True

title = "Page Characteristics"
dash_app_viz_pc.title = title+title_addenda

print (title)

dash_app_viz_pc.layout = html.Div([

    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain how well each Wikipedia language edition covers 
        the [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) articles (also known as local content) from the other language editions.
        They illustrate the content culture gap between language editions, that is the imbalances across languages editions in content representing each language cultural context. 
        '''),

    # dcc.Markdown('''They answer the following questions:
    #     * How well does this group of Wikipedia language editions cover each others’ CCC?
    #     * How well does this Wikipedia language edition cover other languages CCC?
    #     * What is the extent of all language editions CCC in this Wikipedia language edition?
    #     * What Wikipedia language editions cover best the sum of all languages CCC articles?
    #     '''),

    html.Br(),


##----
    dcc.Tabs([

        dcc.Tab(label="Pages' Distribution (Treemap)", children=[
            # html.Br(),
            # html.H5("Wikipedia Language Coverage of Other Languages CCC Treemap", style={'textAlign':'left'}),
            html.Br(),

            dcc.Markdown('''
                * **What is the extent of all language editions CCC in this Wikipedia language edition?**
                '''.replace('  ', '')),

            dcc.Markdown('''
                In the following dropdown menus you can select the two Wikipedia language editions to compare how well they cover the other languages CCC and the extent they occupy.
                You can also select whether to show or hide the selected Wikipedia associated CCC.
                '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select two Wikipedias'),
            style={'display': 'inline-block','width': '400px'}),
            html.Br(),


            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_treemapcoverage',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'Spanish (es)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            html.P('Show the selected language CCC extent in the graph'),
            style={'display': 'inline-block','width': '400px'}),
            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_treemapcoverage2',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'Catalan (ca)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

#            html.Br(),

            html.Div(
            dcc.RadioItems(id='radio_exclude_ownccc',
                options=[{'label':'Yes','value':'Yes'},{'label':'No','value':'No'}],
                value='No',
                labelStyle={'display': 'inline-block', "margin": "0px 10px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            dcc.Graph(id = 'treemap_ccc_coverage'),
#            html.Hr(),
            dcc.Markdown('''
                The treemap graphs show for two selected Wikipedia language editions both the extent and the coverage of other languages CCC. The size of the tiles and the colorscale (light-dark blue) is according to the extent the other languages CCC take in the selected Wikipedia language edition. When you hover on a tile you can read the same information regarding the coverage and extent plus the number of articles. 
                '''.replace('  ', '')),

        ]),


        dcc.Tab(label='Wikipedia Comparison (Scatterplot)', children=[
            html.Br(),

            # html.H5('Wikipedia Language Coverage of Other Language CCC Scatterplot', style={'textAlign':'left'}),

            dcc.Markdown('''
                * **How well does this Wikipedia language edition cover other languages CCC?**
             '''.replace('  ', '')),

            dcc.Markdown('''
                In the following menu you can choose a Wikipedia language edition to see the degree of coverage of other language editions CCC both in percentage and number of articles.
                '''.replace('  ', '')),


            html.Br(),

            html.Div(html.P('Select a Wikipedia'), style={'display': 'inline-block','width': '200px'}),

            dcc.Dropdown(
                id='sourcelangdropdown_coverage',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '190px'}
             ),

            dcc.Graph(id = 'scatterplot_coverage'),
        #    html.Br(),
            dcc.Markdown('''
                The scatterplot graph shows how well each Wikipedia language edition covers other languages CCC. While the Y-axis (log-scale) shows the perentage of a language CCC it covers, the X-axis shows the number of articles this equals. Wikipedia language editions are colored according to their world region (continent).
             '''.replace('  ', '')),
        ]),

    ]),

    footbar,


], className="container")




### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 



# SCATTER LANGUAGES CCC COVERAGE
@dash_app_viz_pc.callback(
    Output('scatterplot_coverage', 'figure'),
    [Input('sourcelangdropdown_coverage', 'value')])
def update_scatterplot(value):
    source_lang = value

    df_langs_map_coverage2 = scatterplotccccoverage_values(source_lang, df_langs_map_coverage)

    fig = px.scatter(df_langs_map_coverage2, x="CCC Coverage Percentage", y="Covered CCC Articles", color="Region", log_x=False, log_y=True,hover_data=['Language (Wiki)'],text="Wiki") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",
    fig.update_traces(
        textposition='top center')

    return fig


# TREEMAP CCC COVERAGE
@dash_app_viz_pc.callback(
    Output('treemap_ccc_coverage', 'figure'),
    [Input('sourcelangdropdown_treemapcoverage', 'value'),Input('sourcelangdropdown_treemapcoverage2', 'value'),dash.dependencies.Input('radio_exclude_ownccc', 'value')])
def update_treemap_coverage(value,value2,exclude):

#    print (df_langs_map_coverage2.head(10))
    df_langs_map_coverage2 = treemapcoverage_values(value, df_langs_map_coverage)
    df_langs_map_coverage3 = treemapcoverage_values(value2, df_langs_map_coverage)
#    print (exclude)

    if exclude=='No':
        df_langs_map_coverage2.drop(df_langs_map_coverage2.tail(1).index,inplace=True)
        df_langs_map_coverage3.drop(df_langs_map_coverage3.tail(1).index,inplace=True)


    parents = list()
    for x in df_langs_map_coverage2.index:
        parents.append('')

#    fig = make_subplots(1, 2, subplot_titles=['Size Coverage', 'Size Extent'])
    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        # subplot_titles = (value+' Wikipedia<br />&nbsp;<br />', value2+' Wikipedia<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_langs_map_coverage2['languagename_full'],
        customdata = df_langs_map_coverage2['abs_value'],
        values = df_langs_map_coverage2['self_rel_value'],
        text = df_langs_map_coverage2['rel_value'],
#        textinfo = "label+value+text",
        texttemplate = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%",
        hovertemplate='<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>',
        marker_colorscale = 'Blues',
        ),
            row=1, col=1)


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_langs_map_coverage3['languagename_full'],
        customdata = df_langs_map_coverage3['abs_value'],
        values = df_langs_map_coverage3['self_rel_value'],
        text = df_langs_map_coverage3['rel_value'],
#        textinfo = "label+value+text",
        texttemplate = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%",
        hovertemplate='<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>',
        marker_colorscale = 'Blues',
        ),
            row=1, col=2)


    fig.update_layout(
        autosize=True,
#        width=700,
        height=900,
        title_font_size=12,
#        paper_bgcolor="White",
        title_text=value+' Wikipedia (Left) and '+value2+' Wikipedia (Right)',
        title_x=0.5,

    )

    return fig




    
# if __name__ == '__main__':
#     dash_app_viz_pc.run_server(debug=True)#,dev_tools_ui=False)
