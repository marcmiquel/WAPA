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






### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# Summary table




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

dash_app_viz_top = Dash(__name__, server = app_wapa, url_base_pathname= webtype + '/types_of_page/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app_viz_top = Dash(url_base_pathname = '/types_of_page/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_viz_top.config['suppress_callback_exceptions']=True

title = "Types of Page"
dash_app_viz_top.title = title+title_addenda

print (title)

types_of_page_metrics_dropwdown = {'Admin Pages Types by Num. Articles':'content_articles', 'Admin Pages Types by Sum. Pageviews':'content_pageviews', 'Admin Pages Types by Sum. Edits':'content_edits', 'Admin Pages Types by Sum. Reverts':'content_reverts', 'Admin Page Types by Sum. Edits Talk Pages':'content_discussion_edits', 'Admin Pages with zero Inlinks':'set2descriptor_zero_inlinks', 'Admin Pages with zero Interwiki links':'set2descriptor_zero_ill', 'Admin Pages with zero Pageviews':'set2descriptor_zero_pageviews', 'Admin Pages with zero Edits last month':'set2descriptor_zero_edits_last_month', 'Admin Pages Categories with zero subcategories':'set2descriptor_zero_subcats', 'Admin Pages Categories with zero subcategories and pages':'set2descriptor_zero_subcats_pages'}










dash_app_viz_top.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain the different types of administrative pages in Wikipedia language editions. 

        Administrative pages are the
        '''),

    html.Br(),


    dcc.Tabs([


        dcc.Tab(label='Wikipedia Comparison (Stacked Bars)', children=[
            html.Br(),

            html.H5('aaa'),
            dcc.Markdown('''aaaa'''),
#           html.Hr(),
            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='grouplangdropdown',
                options=[{'label': k, 'value': k} for k in lang_groups],
                value='Top 10',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Div(
            html.P('You can add or remove languages:'),
            style={'display': 'inline-block','width': '500px'}),

            dcc.Dropdown(id='sourcelangdropdown',
                options = [{'label': k, 'value': k} for k in language_names_list],
                multi=True),

            html.Br(),
            html.Div(
            html.P('Metric'),
            style={'display': 'inline-block','width': '200px'}),

            dcc.Dropdown(id='sourcemetricdropdown',
                options = [{'label': k, 'value': j} for k,j in types_of_page_metrics_dropwdown.items()],),

            dcc.Graph(id = 'language_barchart'),



        ]),


        dcc.Tab(label='Summary Stats (Table)', children=[
            html.Br(),

            html.H5('aaa'),
            dcc.Markdown('''aaaa'''),
#           html.Hr(),



























        ]),






    ]),

    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# Dropdown languages
@dash_app_viz_top.callback(
    dash.dependencies.Output('sourcelangdropdown', 'value'),
    [dash.dependencies.Input('grouplangdropdown', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    return re

#    return ['Cebuano (ceb)', 'Dutch (nl)', 'English (en)', 'French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)', 'Russian (ru)', 'Spanish (es)', 'Swedish (sv)']


# BARCHART
@dash_app_viz_top.callback(
    Output('language_barchart', 'figure'),
    [Input('sourcelangdropdown', 'value')])
def update_barchart(langs):

    languagecodes = []
    for l in langs:
        try:
            languagecodes.append(language_names[l])
        except:
            pass

    df = df_gender_articles_male.loc[df_gender_articles_male['Wiki'].isin(languagecodes)].set_index('Wiki').sort_values(by ='Extent Articles (%)', ascending=False)
    df2 = df_gender_articles_female.loc[df_gender_articles_female['Wiki'].isin(languagecodes)].set_index('Wiki').sort_values(by ='Extent Articles (%)', ascending=True)

    height = len(df2)*25
    if len(languagecodes)==10: height = 500

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df['Language'],
        x=df['Extent Articles (%)'],
        name='Men Articles',
        marker_color='blue',
#        values = df2['Extent Articles (%)'],
        customdata = df['Articles'],
        texttemplate='%{y}',
        orientation='h',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{x}%<br><extra></extra>',

    ))
    fig.add_trace(go.Bar(
        y=df2['Language'],
        x=df2['Extent Articles (%)'],
        name='Women Articles',
        marker_color='red',
#        values = df2['Extent Articles (%)'],
        customdata = df2['Articles'],
        texttemplate='%{y}',
        orientation='h',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{x}%<br><extra></extra>',
    ))

    fig.update_layout(
#        autosize=True,
        title_font_size=12,
        height = height,
        titlefont_size=12,
        width=700,
        barmode='stack')



    return fig


# if __name__ == '__main__':
#     dash_app_viz_top.run_server(debug=True)#,dev_tools_ui=False)
