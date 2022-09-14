

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


####### -------------------- This is the beginning of the App.

##### METHODS SPECIFIC #####

# Get me the recent created articles or recent edits (Filter: Bot, New)
def get_recent_articles_recent_edits(languagecode, edittypes, editortypess, periodhours, resultslimit):
    functionstartTime = time.time()
    print (languagecode, edittypes, editortypess, periodhours, resultslimit)

    def conditions(s):
        if (s['rc_bot'] == 1):
            return 'bot'
        elif (s['rc_actor'] == 'NULL'):
            return 'anonymous'
        else:
            return 'editor'


    query = 'SELECT CONVERT(rc_title USING utf8mb4) as page_title, rc_cur_id as page_id, CONVERT(rc_timestamp USING utf8mb4) as rc_timestamp, rc_bot, rc_actor, rc_new_len as Bytes, CONVERT(actor_name USING utf8mb4) as actor_name, rc_namespace as page_namespace, rc_new, rc_type FROM recentchanges rc LEFT JOIN actor a ON rc.rc_actor = a.actor_id WHERE rc_namespace IN (4, 10, 12, 14, 100, 118, -1, -2) '
#    query = 'SELECT CONVERT(rc_title USING utf8mb4) as page_title, rc_cur_id as page_id, rc_new, rc_type, rc_deleted, rc_bot, CONVERT(rc_timestamp USING utf8mb4) as rc_timestamp FROM recentchanges WHERE rc_namespace = 0 '

    if edittypes == 'new_pages': 
        query += 'AND rc_new = 1 '

    if edittypes == 'wikidata_edits': 
        query += 'AND rc_type = 5 '

    if editortypess == 'no_bots':
        query+= 'AND rc_bot = 0 '

    if editortypess == 'bots_edits':
        query+= 'AND rc_bot = 1 '

    if editortypess == 'anonymous_edits':
        query+= 'AND actor_user IS NULL '

    if editortypess == 'editors_edits':
        query+= 'AND rc_bot = 0 '
        query+= 'AND actor_user IS NOT NULL '


    timelimit = datetime.datetime.now() - datetime.timedelta(hours=int(periodhours))
    timelimit_string = datetime.datetime.strftime(timelimit,'%Y%m%d%H%M%S') 
    query+= 'AND rc_timestamp > "'+timelimit_string+'" '

    query+= 'ORDER BY rc_timestamp DESC'

    query+= ' LIMIT '+str(resultslimit)

    print (query)
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    df = pd.read_sql(query, mysql_con_read);

    df['Editor Edit Type'] = df.apply(conditions, axis=1)
    df=df.drop(columns=['rc_actor','rc_bot'])

    # print (df.head(100))
    # print (len(df))
    # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')
    return df




# Get me the articles that are also in the wikipedia_administrative_pages_analytics_production.db and the admin_pages categories it belongs to.
def get_articles_admin_page_types_wikipedia_db(languagecode, df_rc, admin_page_types):

    conn = sqlite3.connect(databases_path + 'wikipedia_administrative_pages_analytics_production.db'); cursor = conn.cursor()
    df_rc = df_rc.set_index('page_id')

    print (admin_page_types)
    if admin_page_types != 'all_topics':
        conditions = ' AND '+ dash_apps_wapa.admin_page_type_conditions([admin_page_types])
    else:
        conditions = ''


    page_ids = df_rc.index.tolist()
    page_asstring = ','.join( ['?'] * len(page_ids) )
    df_categories = pd.read_sql_query('SELECT page_id, page_namespace, page_title, date_created, num_bytes, num_pageviews, num_interwiki, num_editors, days_last_5_edits, instance_of_Wikimedia_help_page, instance_of_Wikimedia_project_policies_guidelines, instance_of_Wikimedia_wikiproject, Wikipedia_help_level, Wikipedia_policies_and_guidelines_level, Wikipedia_essays_level, WikiProjects_level, Wikipedia_village_pump_level, Wikipedia_copyright_level, Wikipedia_disclaimers_level, Wikipedia_tools_level, Wikipedia_maintenance_level, Wikipedia_deletion_level from '+languagecode+'wiki_pages WHERE page_id IN ('+page_asstring+') '+conditions, conn, params = page_ids)

    df_categories = df_categories.set_index('page_id')

    df_categories['help'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['instance_of_Wikimedia_help_page'] == 1) |
        ((df_categories['Wikipedia_help_level'] < 6) & (df_categories['page_namespace'] == 4)) |
        (df_categories['page_namespace'] == 12),
        'help'] = 1 # then set class to 1


    df_categories['policies_and_guidelines'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['instance_of_Wikimedia_project_policies_guidelines'] == 1) |
        ((df_categories['Wikipedia_policies_and_guidelines_level'] < 6) & (df_categories['page_namespace'] == 4)),
        'policies_and_guidelines'] = 1 # then set class to 1


    df_categories['essays'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_essays_level'] < 6) & (df_categories['page_namespace'] == 4),
        'essays'] = 1 # then set class to 1


    df_categories['wikiprojects'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['instance_of_Wikimedia_wikiproject'] == 1) |
        ((df_categories['WikiProjects_level'] < 6) & (df_categories['page_namespace'] == 4)),
        'wikiprojects'] = 1 # then set class to 1


    df_categories['village_pump'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_village_pump_level'] < 6) & (df_categories['page_namespace'] == 4),
        'village_pump'] = 1 # then set class to 1


    df_categories['copyright'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_copyright_level'] < 6) & (df_categories['page_namespace'] == 4),
        'copyright'] = 1 # then set class to 1


    df_categories['disclaimers'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_disclaimers_level'] < 6) & (df_categories['page_namespace'] == 4),
        'disclaimers'] = 1 # then set class to 1


    df_categories['tools'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_tools_level'] < 6) & (df_categories['page_namespace'] == 4),
        'tools'] = 1 # then set class to 1


    df_categories['maintenance'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_maintenance_level'] < 6) & (df_categories['page_namespace'] == 4),
        'maintenance'] = 1 # then set class to 1


    df_categories['deletion'] = 0 # add a class column with 0 as default value
    df_categories.loc[
        (df_categories['Wikipedia_deletion_level'] < 6) & (df_categories['page_namespace'] == 4),
        'deletion'] = 1 # then set class to 1

    # df_categories.to_csv('categories_prova.csv')

    df_rc_types = df_rc.merge(df_categories, how='left', on='page_id')
    print (df_rc_types.head(10))

    # df_rc_types.to_csv('prova2.csv')
    # print ('caca')
    # input('')

    return df_rc_types






### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_tool_rcm = Dash(__name__, server = app_wapa, url_base_pathname = webtype + '/recent_changes/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
#dash_app_tool_rcm = Dash(url_base_pathname = '/recent_changes/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)


dash_app_tool_rcm.config['suppress_callback_exceptions']=True
title = 'Recent Changes'

dash_app_tool_rcm.title = title+title_addenda

print (title)

dash_app_tool_rcm.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])





edittypes_dict = {'New pages':'new_pages','All edits':'all_edits','Wikidata Edits':'wikidata_edits'}
editortypes_dict = {'Only registered editors':'editors_edits','Only anonymous':'anonymous_edits','Only bots':'bots_edits','No bots':'no_bots','All editors':'all_editors'}

edittypes_dict_inv = {v: k for k, v in edittypes_dict.items()}

editortypes_dict_inv = {v: k for k, v in editortypes_dict.items()}




admin_page_types_inv = {'policies_and_guidelines':'Policies and guidelines', 'help':'Help', 'maintenance':'Maintenance', 'tools':'Tools', 'essays':'Essays', 'disclaimers':'Disclaimers', 'copyright':'Copyright', 'village_pump':'Village pump', 'deletion':'Deletion', 'wikiprojects':'WikiProjects', 'stubs':'Stubs'}
admin_page_types = {v: k for k, v in admin_page_types_inv.items()}
admin_page_types_inv.update({'all_topics':'All Topics'})



content_dropdown = {'Admin Pages as a whole':'admin_page_types_whole','Type1':'type1', 'Namespace 1':'ns1', 'etc.':'etc.'}
page_metrics_dropdown = {'num_bytes':'Number of Bytes', 'num_references':'Number of References', 'num_images':'Number of Images', 'num_multilingual_sisterprojects':'Number of Pages in Multilingual Sister Projects', 'num_wdproperty':'Number WD Properties', 'num_wdidentifiers':'Number of WD Identifiers', 'num_outlinks':'Number of Outlinks', 'num_outlinks_to_admin_pages':'Number of Outlinks to Admin Pages', 'percent_outlinks_to_admin_pages':'Percent of Outlinks to Admin Pages', 'num_inlinks':'Number of Inlinks', 'num_interwiki':'Number of Interwiki', 'num_inlinks_from_admin_pages':'Number of Inlinks From Admin Pages', 'percent_inlinks_from_admin_pages':'Percent of Inlinks From Admin Pages', 'num_categories_contains':'Number of Categories Contained', 'num_pages_contains':'Number of Pages Contained', 'num_pages_admin_contains':'Number of Admin Pages Contained', 'num_pageviews':'Pageviews', 'num_edits':'Number of Edits', 'num_edits_last_month':'Number of Edits Last Month', 'active_months':'Number of Months With Edits', 'editing_days':'Number of Days With Edits', 'num_discussions':'Number of Discussion Edits', 'num_reverts':'Number of Reverts', 'num_anonymous_edits':'Number of Anon. Edits', 'num_bot_edits':'Number of Bot. Edits', 'num_editors':'Number of Editors', 'num_admin_editors':'Number of Admin. Editors', 'median_year_first_edit':"Median of Editors' Year of First Edit", 'median_editors_edits':"Median of Editors' Edits", 'num_edits_last_month_by_admin':'Number of Edits Last Month by Admin', 'num_edits_last_month_by_anonymous':'Number of Edits Last Month by Anonyomous', 'num_edits_last_month_by_newcomer_90d':'Number of Edits Last Month by a Newcomer of 90 Days', 'num_edits_last_month_by_newcomer_1y':'Number of Edits Last Month by a Newcomer of 1 Year', 'num_edits_last_month_by_newcomer_5y':'Number of Edits Last Month by a Newcomer of 5 Years', 'date_last_edit':'Date of the Last Edit', 'date_last_discussion':'Date of the Last Edit in the Discussion Page', 'days_last_50_edits':'Days Since The Last 50 Edits', 'days_last_5_edits':'Days Since The Last 5 Edits', 'days_last_edit':'Days Since The Last 5 Edit', 'date_created':'Date of Creation', 'timestamp':'Edit Timestamp', 'first_timestamp_lang':'Wikipedia where First Created', 'total_months':'Number of Months Since Creation', 'max_active_months_row':'Number of Months in a Row Editing Activity', 'max_inactive_months_row':'Number of Months in a Row with No Editing Activity', 'percent_active_months':'Percent of Months With Editing Activity', 'percent_editing_days':'Percent of Editing Days'} 
page_metrics_dropdown_inv = {v: k for k, v in page_metrics_dropdown.items()}



## ----------------------------------------------------------------------------------------------------- ##



text_default = '''In this page you can retrieve the list of Recent Changes in a Wikipedia language edition according with different categories relevant to admin pages (e.g., Policies and Guidelines, Help Pages, etc.).'''    



text_results = '''
The following graph shows bars with the number of edits and the percentage for each of the categories relevant to admin_pages that were detected using the project's database. The colors represent each type of admin page.

The table shows the list of requested Recent changes edits. The columns present the article title, timestamp, editor, article creation date, current length after the edit, number of pageviews and number of Interwiki links. When a featured is selected to sort the results (order by), it is added as a column. The remaining columns are the mentioned types of admin pages.

Note:  Since the categorization is based on the last database created, some features may not be up to date. This dashboard is in alpha phase.
'''    

text_results2 = '''
The following table shows the list of requested Recent changes edits. The columns present the article title, timestamp, editor, article creation date, current length after the edit, number of pageviews and number of Interwiki links. When a featured is selected to sort the results (order by), it is added as a column. The remaining columns are the mentioned admin_pages-related categories. 

Note: Since the categorization is based on the last database created, some features may not be up to date. This dashboard is in alpha phase.
'''    


## ----------------------------------------------------------------------------------------------------- ##


interface_row1 = html.Div([

    html.Div([
    html.P(
        [
            "Source ",
            html.Span(
                "language",
                id="tooltip-target-lang",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a language to retrieve a list of recent changes.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-lang",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div([
    html.P(
        [
            "Types of ",
            html.Span(
                "edits",
                id="tooltip-target-content",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select all the edits, edits that resulted in new articles, or external edits made in Wikidata.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-content",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div([
    html.P(
        [
            "Types of ",
            html.Span(
                "editors",
                id="tooltip-target-editortypes",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select or filter the edits by a specific type of editor.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-editortypes",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),




    html.Div(
    [
    html.P(
        [
            "Filter by ",
            html.Span(
                "type of admin page",
                id="tooltip-target-category",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a Topic to filter the results to show only articles about certain topic (geolocated with a ISO code, gender, CCC, etc.)",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-category",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])


interface_row2 = html.Div([

    html.Div([
    html.P(
        [
            "Order by ",
            html.Span(
                "metric",
                id="tooltip-target-orderby",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a relevant metric to sort the results).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-orderby",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div(
    [
    html.P(
        [
            "Timeframe in ",
            html.Span(
                "hours",
                id="tooltip-target-timeframe",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Specify the number of hours from now you to retrieve the recent changes (by default 24).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-timeframe",
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
                "number of results",
                id="tooltip-target-limit",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Limit the number of results (by default 300, maximum of 5000)",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    )

])




def dash_app_tool_rcm_build_layout(params):

    if len(params)!=0 and params['lang']!='none' and params['lang']!= None:
        functionstartTime = time.time() 

        if 'lang' in params:
            languagecode = params['lang']
            if languagecode != 'none': language = languages.loc[languagecode]['languagename']
        else:
            languagecode = 'none'

        if 'edittypes' in params:
            edittypes = params['edittypes']
            if edittypes not in edittypes_dict_inv: edittypes = 'all_edits'
        else:
            edittypes = 'all_edits'

        if 'editortypes' in params:
            editortypes=params['editortypes']
            if editortypes not in editortypes_dict_inv: editortypes = 'all_editors'
        else:
            editortypes='all_editors'

        if 'page_type' in params:
            page_type = params['page_type']
            if page_type not in admin_page_types_inv: page_type = 'all_topics'
        else:
            page_type = 'all_topics'

        if 'orderby' in params:
            orderby=params['orderby'].lower()
            if orderby not in page_metrics_dropdown_inv: orderby = 'timestamp'
        else:
            orderby='timestamp'

        if 'timeframe' in params:
            timeframe = params['timeframe']
            try:
                timeframe = int(timeframe)
                if timeframe == 0: timeframe = 24
                elif timeframe > 168: timeframe = 168
            except:
                timeframe = 24
        else:
            timeframe = 24

        if 'limit' in params:
            limit = int(params['limit'])
            try:
                limit = int(limit)
                if limit == 0: limit = 5000
                elif limit > 5000: limit = 5000
            except:
                limit = 5000
        else:
            limit = 5000
        

        print (languagecode, edittypes, editortypes, page_type, orderby, timeframe, limit)

        # df = pd.read_csv(databases_path+'df_rc_types_sample.csv')
        # df = df.rename_axis('position')
        # df = df.reset_index()

        # if limit != 'none':
        #     df = df.head(limit)


        df = get_recent_articles_recent_edits(languagecode, edittypes, editortypes, timeframe, limit)
        df = get_articles_admin_page_types_wikipedia_db(languagecode, df, page_type)
        df = df.reset_index()


        df = df.rename_axis('position')
        df = df.reset_index()



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([
                navbar,          
                html.H3('Recent Changes', style={'textAlign':'center'}),
                html.Br(),

                dcc.Markdown(
                    text_default.replace('  ', '')),


                # HERE GOES THE INTERFACE
                # LINE 
                html.Br(),
                html.H5('Select the Wikipedia language edition'),
                interface_row1,
                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='lang',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='ca',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='edittypes',
                    options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                    value='none',
                    placeholder="Type of edits (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='editortypes',
                    options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                    value='none',
                    placeholder="Type of editors (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='page_type',
                    options=[{'label': i, 'value': admin_page_types[i]} for i in admin_page_types],
                    value='none',
                    placeholder="Type of admin page (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),



                # LINE 
                html.Br(),
                interface_row2,
                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                    id='orderby',
                    options=[{'label': i, 'value': page_metrics_dropdown[i]} for i in sorted(page_metrics_dropdown)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                    id='timeframe',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='24',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='500',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                ###            

                html.Div(
                html.A(html.Button('Query Results!'),
                    href=''),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Br(),

                html.Hr(),
                # html.H5('Results'),
                # dcc.Markdown(text_results.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                footbar,
            ], className="container")

            return layout


        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
#        print (df.columns)
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')

        dict_values = {"Help":df["help"].sum(), "Policies and Guidelines":df["policies_and_guidelines"].sum(), "Essays":df["essays"].sum(), "Wikiprojects":df["wikiprojects"].sum(),  "Village Pump":df["village_pump"].sum(),  "Copyright":df["copyright"].sum(),  "Disclaimers":df["disclaimers"].sum(),  "Tools":df["tools"].sum(), "Maintenance":df["maintenance"].sum(), "Deletion":df["deletion"].sum()}

        dfy = pd.DataFrame(list(dict_values.items()))
        if edittypes != 'new_pages':
            dfy.rename(columns = {0:'Admin Pages',1:'Number of edits'}, inplace = True)
        else:
            dfy.rename(columns = {0:'Admin Pages',1:'Number of new pages'}, inplace = True)


        a = df['Page Namespace'].value_counts(ascending=False)

        print (df)
        print (df.columns)


        
        if edittypes != 'new_pages':
            fig = px.bar(dfy, x="Admin Pages", y="Number of edits", color="Admin Pages", title="Categories Summary",text=dfy['Number of edits'])
        else:
            fig = px.bar(dfy, x="Admin Pages", y="Number of new pages", color="Admin Pages", title="Categories Summary",text=dfy['Number of new pages'])


        # PREPARE THE DATATABLE
        columns_dict = {'position':'Nº','page_title_x':'Title','rc_timestamp':'Edit Timestamp', 'page_namespace_x':'Namespace', 'date_created':'Creation Date', 'num_bytes':'Current Length', 'num_pageviews':'Pageviews','num_interwiki':'Interwiki', 'actor_name':'Editor', 'help':'Help', 'policies_and_guidelines':'Policies', 'essays':'Essays','wikiprojects':'WikiProjects','village_pump':'Village pump', 'copyright':'Copyright', 'disclaimers':'Disclaimers', 'tools':'Tools'}

        columns_dict.update(page_metrics_dropdown_inv)

        df=df.rename(columns=columns_dict)
        print (df.columns)

        # {'New pages':'new_pages','All edits':'all_edits','Wikidata Edits':'wikidata_edits'}



        '''
        ['position', 'page_id', 'page_title_x', 'rc_timestamp', 'Bytes',
       'actor_name', 'page_namespace_x', 'rc_new', 'rc_type',
       'Editor Edit Type', 'page_namespace_y', 'page_title_y', 'date_created',
       'num_bytes', 'num_pageviews', 'num_interwiki', 'num_editors',
       'days_last_5_edits', 'instance_of_Wikimedia_help_page',
       'instance_of_Wikimedia_project_policies_guidelines',
       'instance_of_Wikimedia_wikiproject', 'Wikipedia_help_level',
       'Wikipedia_policies_and_guidelines_level', 'Wikipedia_essays_level',
       'WikiProjects_level', 'Wikipedia_village_pump_level',
       'Wikipedia_copyright_level', 'Wikipedia_disclaimers_level',
       'Wikipedia_tools_level', 'Wikipedia_maintenance_level',
       'Wikipedia_deletion_level', 'help', 'policies_and_guidelines', 'essays',
       'wikiprojects', 'village_pump', 'copyright', 'disclaimers', 'tools',
       'maintenance', 'deletion']

       '''

        final_columns = ['Nº','Title','Namespace','Edit Timestamp','Editor']+['Creation Date','Current Length','Pageviews','Interwiki']
        admin_pages = ['Help', 'Policies', 'Essays', 'WikiProjects', 'Tools', 'Village pump', 'Disclaimers', 'Copyright']


        if orderby!='none' and page_metrics_dropdown[orderby] not in admin_pages and page_metrics_dropdown[orderby] not in final_columns:
            final_columns+= [page_metrics_dropdown[orderby]]

        final_columns = final_columns + admin_pages
        if edittypes == 'wikidata_edits': 
            final_columns.remove('Editor')

        columns = final_columns
        df = df[columns]
        # print ('ih')
        # print (columns)
        # print (df.columns)
        # print ('ah')

        # df1=df1.drop(columns=todelete)
        orderby = orderby.lower()
        if orderby != 'none':
            order = page_metrics_dropdown[orderby]
        else:
            order = page_metrics_dropdown['timestamp']
        df = df.sort_values(order,ascending=False)
        df = df.fillna('')
        df_list = list()

        namespaces_dict = lang_namespaces_dict[languagecode]
        namespaces_dict_inv = {v: k for k, v in namespaces_dict.items()}
        # print (namespaces_dict)




        k = 0
        for index, rows in df.iterrows():
            if k > limit: break

            df_row = list()

            for col in columns:
                title = rows['Title']
                if not isinstance(title, str):
                    title = title.iloc[0]

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == 'Title':
                    title = rows['Title']
                    if not isinstance(title, str):
                        title = title.iloc[0]

                    namespace = namespaces_dict_inv[rows['Namespace']]
                    df_row.append(html.A(title.replace('_',' '), href='https://'+languagecode+'.wikipedia.org/wiki/'+namespace+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Namespace':
                    ns = rows['Namespace']
                    ns_plus = str(namespaces_dict_inv[ns])+' ('+str(ns)+')'
                    df_row.append(html.Div(ns_plus, style={'text-decoration':'none'}))

                elif col == 'Editor':
                    editor = rows['Editor']
                    df_row.append(html.Div(editor, style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    try:
                        df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    except:
                        df_row.append('')

                elif col == 'Current Length':
                    try:
                        value = round(float(int(rows[col])/1000),1)
                        df_row.append(str(value)+'k')
                    except:
                        df_row.append('0')


                elif col == 'Outlinks' or col == 'References' or col == 'Images':
                    title = rows['Title']
                    df_row.append(html.A( rows[col], href='https://'+languagecode+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+languagecode+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+languagecode+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+languagecode+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    df_row.append(html.A( rows['Discussions'], href='https://'+languagecode+'.wikipedia.org/wiki/Talk:'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+languagecode+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    title = rows['Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+languagecode+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edit Timestamp':
                    timestamp = str(int(rows[col]))
                    df_row.append(datetime.datetime.strftime(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S'),'%H:%M:%S %d-%m-%Y'))

                elif col == 'Creation Date':
                    try:
                        timestamp = str(int(rows[col]))
                        df_row.append(datetime.datetime.strftime(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S'),'%Y-%m-%d'))
                    except:
                        df_row.append('')

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))


                elif col == 'Sexual Orientation' or col == 'Ethnic Group' or col == 'Gender':
                    if pd.isna(rows[col]): 
                        df_row.append('')
                        continue
                    qit = str(rows[col])

                    if ';' in qit:
                        qlist = qit.split(';')
                    else:
                        qlist = [qit]

                    c = len(qlist)

                    text = ' '

                    i = 0
                    for ql in qlist:
                        i+= 1
                        try:
                            try:
                                label = qitem_labels_lang.loc[ql]['page_title']
                                if label == '': label = qitem_labels_lang.loc[ql]['label']
                            except:
                                label = qitem_labels_lang.loc[ql]['label']

                            text+= '['+label+']'+'('+'http://'+languagecode+'.wikipedia.org/wiki/'+ label.replace(' ','_')+')'
                        except: 
                            try:
                                try:
                                    label = qitem_labels_en.loc[ql]['page_title']
                                    if label == '': label = qitem_labels_en.loc[ql]['label']
                                except:
                                    label = qitem_labels_en.loc[ql]['label']

                                text+= '['+label+' (en)'+']'+'('+'http://en.wikipedia.org/wiki/'+ label.replace(' ','_')+')'

                            except:
                                label = ql
                                text+= '['+label+']'+'('+'https://www.wikidata.org/wiki/'+ label+')'

                        if i<c:
                            text+=', '

                    df_row.append(dcc.Markdown(text))


                elif col == language+' CCC':
#                    print (rows['Title'],rows['CCC'])
                    if rows['CCC'] == 1:
                        df_row.append('yes')
                    else:
                        df_row.append('')

                else:
                    df_row.append(rows[col])

            if k <= limit:
                df_list.append(df_row)


        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')
        # print (len(df_list))


        # RESULTS PAGE
        title = 'Recent Changes'
        df1 = pd.DataFrame(df_list)
        dash_app_tool_rcm.title = title+title_addenda

        # LAYOUT
        layout = html.Div([
            navbar,           
            html.H3(title, style={'textAlign':'center'}),
#            html.Br(),

            dcc.Markdown(
                text_default.replace('  ', '')),
            html.Br(),


            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the Wikipedia language edition'),

            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='ca',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='edittypes',
                options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                value='none',
                placeholder="Type of edits (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='editortypes',
                options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                value='none',
                placeholder="Type of editors (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='page_type',
                options=[{'label': i, 'value': admin_page_types[i]} for i in admin_page_types],
                value='none',
                placeholder="Type of admin page (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),



            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='orderby',
                options=[{'label': i, 'value': page_metrics_dropdown[i]} for i in sorted(page_metrics_dropdown)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='timeframe',                    
                placeholder='Enter a value...',
                type='text',
                value='24',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='500',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.A(html.Button('Query Results!', style={"marginLeft": "60%"}),
                href=''),
           

            # here there is the table            
            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            html.Div(html.I('Recent Changes for '+language+' Wikipedia [limited by '+str(limit)+' results, last '+str(timeframe)+' hours, '+edittypes_dict_inv[edittypes]+', '+edittypes_dict_inv[edittypes]+', '+editortypes_dict_inv[editortypes]+', '+admin_page_types_inv[page_type]+', and ordered by '+page_metrics_dropdown[orderby]+']')),
            html.Br(),


            dcc.Markdown(text_results.replace('  ', '')),
            dcc.Graph(
                id='example-graph',
                figure=fig
            ),

            # if edittypes != 'new_pages':
            # else:
            #     dcc.Markdown(text_results2.replace('  ', '')),
    
            html.Br(),
            html.H6('Table'),


            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(
                    (df_row[x]),
                    style={'font-size':"12px"} # 'background-color':"lightblue"}
                    )
                for x in range(len(columns))
            ]) for df_row in df_list]),

            footbar,

        ], className="container")

        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' before printing')
    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            navbar,           
            html.H3('Recent Changes', style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the Wikipedia language edition'),

            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='ca',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='edittypes',
                options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                value='none',
                placeholder="Type of edits (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='editortypes',
                options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                value='none',
                placeholder="Type of editors (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='page_type',
                options=[{'label': i, 'value': admin_page_types[i]} for i in admin_page_types],
                value='none',
                placeholder="Type of admin page (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),



            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='orderby',
                options=[{'label': i, 'value': page_metrics_dropdown[i]} for i in sorted(page_metrics_dropdown)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='timeframe',                    
                placeholder='Enter a value...',
                type='text',
                value='24',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='500',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.A(html.Button('Query Results!', style={"marginLeft": "60%"}),
                href=''),
           
            footbar,

        ], className="container")

    return layout



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# callback update page layout
@dash_app_tool_rcm.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps_wapa.parse_state(href)
    return dash_app_tool_rcm_build_layout(state)

# callback update URL
component_ids_rcm = ['lang','edittypes', 'editortypes','page_type','orderby','timeframe','limit']
@dash_app_tool_rcm.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_rcm])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids_rcm, values)))
    return '?'+state
#    return f'?{state}'




if __name__ == '__main__':
    dash_app_tool_rcm.run_server(debug=True)#,dev_tools_ui=False)
