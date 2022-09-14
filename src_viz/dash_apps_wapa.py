# -*- coding: utf-8 -*-

# flash dash
import flask
from flask import Flask, request, render_template
from flask import send_from_directory
from dash import Dash
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
# dash bootstrap components
import dash_bootstrap_components as dbc

# viz
import plotly
import chart_studio.plotly as py
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# data
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import pandas as pd
import sqlite3
import xlsxwriter

# other
import numpy as np
import random
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time
import requests
import subprocess


# script
sys.path.insert(0, '/srv/wapa/src_data')
import wikilanguages_utils


setting_up_time = time.time()

##### DATA RESOURCES FOR ALL APPS #####



#last_period = '2019-05' # CHANGE WHEN THE NEXT stats.db IS COMPUTED.


territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
languages = wikilanguages_utils.load_wiki_projects_information();


wikilanguagecodes = sorted(languages.index.tolist())


# Verify/Remove all languages without a replica database
for a in wikilanguagecodes:
    if wikilanguages_utils.establish_mysql_connection_read(a)==None:
        wikilanguagecodes.remove(a)
print (wikilanguagecodes)


# Add the 'wiki' for each Wikipedia language edition
wikilanguagecodeswiki = []
for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')






##### START / LISTS OF VARIABLES FOR THE DROPDOWN MENUS AND #####
# This we get them from here and we only make a copy there in the apps.


# For the source and target language menus
language_names_list = []
language_names = {}
language_names_full = {}
language_name_wiki = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    language_name_wiki[lang_name]=languages.loc[languagecode]['languagename']
    language_names_full[languagecode]=languages.loc[languagecode]['languagename']
    language_names[lang_name] = languagecode
    language_names_list.append(lang_name)


langs_dict = {} 
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    langs_dict[lang_name] = languagecode



language_names_list = sorted(language_names_list)
language_names_inv = {v: k for k, v in language_names.items()}

lang_groups = list()
lang_groups += ['Top 5','Top 10', 'Top 20', 'Top 30', 'Top 40']#, 'Top 50']
lang_groups += territories['region'].unique().tolist()
lang_groups += territories['subregion'].unique().tolist()
try: lang_groups.remove(''); 
except: pass




# APP VARIABLES

# APP: Pages Over Time
time_aggregation = ['Monthly','Quarterly','Yearly']

content_over_time1_dropdown = {'Wikipedia Pages as a whole':'wp_whole', 'Admin Pages as a whole':'admin_page_types_whole', 'Admin and Non-Admin Pages':'admin_non_admin', 'Admin Pages-Types':'admin_page_types', 'Type1':'type1', 'Namespace 1':'ns1', 'etc.':'etc.'}

# NUMBER OF DIMENSIONS: 2!
# selecting "admin and non-admin pages" you see the distribution between admin and non-admin pages. You CANNOT choose editor_type, it is disabled.
# selecting "admin pages-types" you see the distribution of all admin pages types. You CANNOT choose editor_type, it is disabled.
# selecting "admin and non-admin pages" o

# NUMBER OF DIMENSIONS: 1!
# selecting "Wikipedia pages as a whole", you see them as a whole. You can choose editor_type.
# selecting "Admin Pages as a whole", you see them as a whole. You can choose editor_type.
# selecting any type or namespace, you can choose editor_type.


metrics_dropdown = {'Edits':'edits','Articles Created':'create_article_edits','Reverts':'reverts','Discussion Edits':'discussion_edits','Editors':'editors'}

type_of_editor_dropdown = {'Registered Editors':'registered', 'Newcomers 90 Days':'newcomer_90d', 'Newcomers 1 Year':'newcomer_1y', 'Newcomers 5 Years':'newcomer_5y', 'Lustrum of First Edit 2000-2005':'lustrum_2000_2005','Lustrum of First Edit 2006-2010':'lustrum_2006_2010', 'Lustrum of First Edit 2011-2015':'lustrum_2011_2015', 'Lustrum of First Edit 2016-2020':'2016_2020', 'Lustrum of First Edit 2021-2025':'lustrum_2021_2025', 'Editor with Admin Flag':'admin'}


# IF NUMBER OF DIMENSIONS 1 in content.
# choosing registered, you have registered and anonymous.
# choosing any newcomer, you have that newcomer group and the remaining.
# choosing any lustrum lustrum, you have all the lustrums.
# choosing admin, you have registered editors admin and non-admin.

# IF NUMBER OF DIMENSIONS 2 in content.
# choosing registered, you have ONLY registered.
# choosing newcomer, you only have that newcomer group.
# choosing any lustrum lustrum, you only have that lustrum.
# choosing admin, you only have the admin group.

content_over_time2_dropdown = {'Wikipedia Pages':'wp_whole', 'Admin Pages':'admin_page_types_whole', 'Type1':'type1', 'Namespace 1':'ns1', 'etc.':'etc.'}

# same metrics, same type of editor.
# behaviour is like in the previous case (if number of dimensions 2 in content for. tab 1)

lustrums_dict = {'Lustrum of First Edit 2000-2005':'lustrum_2000_2005','Lustrum of First Edit 2006-2010':'lustrum_2006_2010', 'Lustrum of First Edit 2011-2015':'lustrum_2011_2015', 'Lustrum of First Edit 2016-2020':'lustrum_2016_2020', 'Lustrum of First Edit 2021-2025':'lustrum_2021_2025'}


# APP: Pages' Characteristics
content_dropdown = {'Admin Pages as a whole':'admin_page_types_whole','Type1':'type1', 'Namespace 1':'ns1', 'etc.':'etc.'}
page_metrics_dropdown = {'num_bytes':'Number of Bytes', 'num_references':'Number of References', 'num_images':'Number of Images', 'num_multilingual_sisterprojects':'Number of Pages in Multilingual Sister Projects', 'num_wdproperty':'Number WD Properties', 'num_wdidentifiers':'Number of WD Identifiers', 'num_outlinks':'Number of Outlinks', 'num_outlinks_to_admin_pages':'Number of Outlinks to Admin Pages', 'percent_outlinks_to_admin_pages':'Percent of Outlinks to Admin Pages', 'num_inlinks':'Number of Inlinks', 'num_interwiki':'Number of Interwiki', 'num_inlinks_from_admin_pages':'Number of Inlinks From Admin Pages', 'percent_inlinks_from_admin_pages':'Percent of Inlinks From Admin Pages', 'num_categories_contains':'Number of Categories Contained', 'num_pages_contains':'Number of Pages Contained', 'num_pages_admin_contains':'Number of Admin Pages Contained', 'num_pageviews':'Number of Pageviews', 'num_edits':'Number of Edits', 'num_edits_last_month':'Number of Edits Last Month', 'active_months':'Number of Months With Edits', 'editing_days':'Number of Days With Edits', 'num_discussions':'Number of Discussion Edits', 'num_reverts':'Number of Reverts', 'num_anonymous_edits':'Number of Anon. Edits', 'num_bot_edits':'Number of Bot. Edits', 'num_editors':'Number of Editors', 'num_admin_editors':'Number of Admin. Editors', 'median_year_first_edit':"Median of Editors' Year of First Edit", 'median_editors_edits':"Median of Editors' Edits", 'num_edits_last_month_by_admin':'Number of Edits Last Month by Admin', 'num_edits_last_month_by_anonymous':'Number of Edits Last Month by Anonyomous', 'num_edits_last_month_by_newcomer_90d':'Number of Edits Last Month by a Newcomer of 90 Days', 'num_edits_last_month_by_newcomer_1y':'Number of Edits Last Month by a Newcomer of 1 Year', 'num_edits_last_month_by_newcomer_5y':'Number of Edits Last Month by a Newcomer of 5 Years', 'date_last_edit':'Date of the Last Edit', 'date_last_discussion':'Date of the Last Edit in the Discussion Page', 'days_last_50_edits':'Days Since The Last 50 Edits', 'days_last_5_edits':'Days Since The Last 5 Edits', 'days_last_edit':'Days Since The Last 5 Edit', 'date_created':'Date of Creation', 'first_timestamp_lang':'Wikipedia where First Created', 'total_months':'Number of Months Since Creation', 'max_active_months_row':'Number of Months in a Row Editing Activity', 'max_inactive_months_row':'Number of Months in a Row with No Editing Activity', 'percent_active_months':'Percent of Months With Editing Activity', 'percent_editing_days':'Percent of Editing Days'} 
page_metrics_dropdown_inv = {v: k for k, v in page_metrics_dropdown.items()}


# APP: Red Flags
admin_page_types = {'wikipedia_policies_and_guidelines':'Wikipedia policies and guidelines', 'wikipedia_help':'Wikipedia help', 'wikipedia_maintenance':'Wikipedia maintenance', 'wikipedia_tools':'Wikipedia tools', 'wikipedia_essays':'Wikipedia essays', 'wikipedia_disclaimers':'Wikipedia disclaimers', 'wikipedia_copyright':'Wikipedia copyright', 'wikipedia_village_pump':'Wikipedia village pump', 'wikipedia_deletion':'Wikipedia deletion', 'wikiprojects':'WikiProjects', 'stubs':'Stubs'}
admin_page_types_categories = {'wikipedia_policies_and_guidelines':'Wikipedia policies and guidelines', 'wikipedia_help':'Wikipedia help', 'wikipedia_maintenance':'Wikipedia maintenance', 'wikipedia_tools':'Wikipedia tools', 'wikipedia_essays':'Wikipedia essays', 'wikipedia_disclaimers':'Wikipedia disclaimers', 'wikipedia_copyright':'Wikipedia copyright', 'wikipedia_village_pump':'Wikipedia village pump', 'wikipedia_deletion':'Wikipedia deletion', 'wikiprojects':'WikiProjects', 'stubs':'Stubs'}

page_namespaces = {'ns4':'Wikipedia (ns4)','ns12':'Help (ns12)','ns14':'Category (ns14)','ns100':'Portal (ns100)'}
wikidata_instance_of = {'Q14204246':'instance_of_Wikimedia_project_page', 'Q17442446':'instance_of_Wikimedia_internal_item', 'Q4656150':'instance_of_Wikimedia_project_policies_guidelines', 'Q56005592': 'instance_of_Wikimedia_help_page', 'Q16695773': 'instance_of_Wikimedia_wikiproject', 'Q4663903': 'instance_of_Wikimedia_wikimedia_portal'}

# page_metrics_dropwdown. the same.
ratios_dropdown = {}
base_metrics = ['qitem','date_created','num_bytes','num_pageviews','num_interwiki','num_inlinks','num_edits']
# order_by_metrics_ratios_dropdown = base_metrics + selected_metrics_ratios # this will vary according to the previous choices.
#metrics_ratios_dict = metric_dict.copy().update(ratios_dict)

# APP: Admin. Pages Gaps
show_gaps_dict = {'No language gaps':'no-gaps','At least one gap':'one-gap-min','Only gaps':'only-gaps', 'All':'all'}


# APP: Incomplete Pages
# same from previous.

# APP: Page Across Languages
# same from previous.

# APP: Recent Changes
edittypes_dict = {'New articles':'new_articles','All edits':'all_edits','Wikidata Edits':'wikidata_edits'}
editortypes_dict = {'Only registered editors':'editors_edits','Only anonymous':'anonymous_edits','Only bots':'bots_edits','No bots':'no_bots','All editors':'all_editors'}

edittypes_dict_inv = {v: k for k, v in edittypes_dict.items()}
editortypes_dict_inv = {v: k for k, v in editortypes_dict.items()}


####






##### END / LISTS OF VARIABLES FOR THE DROPDOWN MENUS #####






##### WEB RESOURCES #####

title_addenda = ' - Wikipedia Administrative Pages Analytics (WAPA)'
#external_stylesheets = ['https://wapa.wmcloud.org/assets/bWLwgP.css'] 
external_stylesheets = [dbc.themes.BOOTSTRAP,'https://wapa.wmcloud.org/assets/bWLwgP.css']
external_scripts = ['https://wapa.wmcloud.org/assets/gtag.js']
webtype = ''


##### NAVBAR #####
# LOGO = "./assets/logo.png"
# LOGO_foot = "./assets/wikimedia-logo.png"

LOGO = "https://wapa.wmcloud.org/assets/logo.png"
LOGO_foot = "https://wapa.wmcloud.org/assets/wikimedia-logo.png"


# LOGO = app.get_asset_url('logo.png') # this would have worked. 




navbar = html.Div([
    html.Br(),
    dbc.Navbar(
        [ dbc.Collapse(
                dbc.Nav(
                    [
                    html.A(
                    # Use row and col to control vertical alignment of logo / brand
                    dbc.Row(
                        [
                            dbc.Col(html.Img(src=LOGO, height="35px")),
    #                        dbc.Col(dbc.NavbarBrand("WAPA", className="mb-5")),
                        ],
                        align="center",
                    ),
                    href="https://meta.wikimedia.org/wiki/Wikipedia_Administrative_Pages_Analytics", target= "_blank",
                    style = {'margin-right':"5px"}),
                    dbc.DropdownMenu(
                        [
                        dbc.DropdownMenuItem("Types of Page", href="https://wapa.wmcloud.org/types_of_page/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Page Characteristics", href="https://wapa.wmcloud.org/page_characteristics/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Pages Over Time", href="https://wapa.wmcloud.org/pages_over_time/", style = {'font-size':"12px"}),
                        ],
                        label="Visualizations",
                        nav=True,
                        style = {'font-size':"14px"},
                    ),
                    dbc.DropdownMenu(
                        [
                        dbc.DropdownMenuItem("Red Flags", href="https://wapa.wmcloud.org/red_flags/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Incomplete Pages", href="https://wapa.wmcloud.org/incomplete_pages/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Page Gaps", href="https://wapa.wmcloud.org/page_gaps/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Page Across Languages", href="https://wapa.wmcloud.org/page_across_languages/", style = {'font-size':"12px"}),
                        dbc.DropdownMenuItem("Recent Changes", href="https://wapa.wmcloud.org/recent_changes/", style = {'font-size':"12px"}),
                        ],
                        label="Tools",
                        nav=True,
                        style = {'font-size':"14px"},
                    ),
                ], className="ml-auto", navbar=True),
                id="navbar-collapse2",
                navbar=True,
            ),
        ],
        color="white",
        dark=False,
        className="ml-2",
    ),
    ])


##### FOOTBAR #####
footbar = html.Div([
        html.Br(),
        html.Br(),
        html.Hr(),

        html.Div(
            dbc.Nav(
                [
                    dbc.NavLink("Meta-Wiki Project Page", href="https://meta.wikimedia.org/wiki/Wikipedia_Administrative_Pages_Analytics", target="_blank", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("View Source", href="https://github.com/marcmiquel/wapa", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("Datasets/Databases", href="https://meta.wikimedia.org/wiki/Wikipedia_Administrative_Pages_Analytics/Database", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("Research", href="https://meta.wikimedia.org/wiki/Wikipedia_Administrative_Pages_Analytics#Research", style = {'color': '#8C8C8C'}),
                ], className="ml-2"), style = {'textAlign': 'center', 'display':'inline-block' , 'width':'60%'}),

        html.Div(id = 'current_data', children=[        
            'Updated with data generated in: ',
            html.B("2022-07")],
#            html.B(current_dataset_period_stats)],
            style = {'textAlign':'right','display': 'inline-block', 'width':'40%'}),
        html.Br(),
        html.Div([
            html.P('Hosted with ♥ on ',style = {'display':'inline-block'}),
            html.A('Wikimedia Cloud VPS',href='https://wikitech.wikimedia.org/wiki/Portal:Cloud_VPS', target="_blank", style = {'display':'inline-block'}),
            html.P('.',style = {'display':'inline-block', 'margin-right':"5px"}),
            html.A(html.Img(src=LOGO_foot, height="35px"),href='https://wikitech.wikimedia.org/wiki/Help:Cloud_Services_Introduction', target="_blank", style = {'display':'inline-block'}),

            ], style = {'textAlign':'right'}
            ),
        html.Br(),
    ])




##### FLASK APP #####
app_wapa = flask.Flask(__name__)

if __name__ == '__main__':
    app_wapa.run_server(host='0.0.0.0', threaded=True)


@app_wapa.errorhandler(404)
def handling_page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404





##### DASH APPS #####

print ('* importing apps.')

# others
from apps.home_app import *



# visualizations
# from apps.types_of_page_app import *
# from apps.pages_over_time_app import *
# from apps.page_characteristics_app import *
print ('viz loaded')

# tools
# from apps.red_flags_app import *
# from apps.incomplete_pages_app import *
# from apps.page_across_languages_app import *
# from apps.page_gaps_app import *
# from apps.recent_changes_app import *
print ('tools loaded')


##### FUNCTIONS #####
# parse
def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
#    print (state)
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


##### EXECUTING FUNCTIONS #####

print ('* dash_apps_wapa loaded after: '+str(datetime.timedelta(seconds=time.time() - setting_up_time)))
print ('\n\n\n*** START WAPA APP:'+str(datetime.datetime.now()))


