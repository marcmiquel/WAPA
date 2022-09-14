import sys
import dash_apps_wapa
sys.path.insert(0, '/srv/wapa/src_viz')
from dash_apps_wapa import *





### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_tool_pg = Dash(__name__, server = app_wapa, url_base_pathname = webtype + '/page_gaps/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
#dash_app_tool_pg = Dash(url_base_pathname = '/page_gaps/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_tool_pg.config['suppress_callback_exceptions']=True

general_title = 'Page Gaps'
dash_app_tool_pg.title = general_title+title_addenda
print (general_title)


dash_app_tool_pg.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])






source_lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    source_lang_dict[lang_name] = languagecode

topic_dict={'All':'all','Keywords':'keywords','Geolocated':'geolocated','People':'people','Women':'women','Men':'men','Folk':'folk','Earth':'earth','Monuments and Buildings':'monuments_and_buildings','Music Creations and Organizations':'music_creations_and_organizations','Sports and Teams':'sport_and_teams','Food':'food','Paintings':'paintings','GLAM':'glam','Books':'books','Clothing and Fashion':'clothing_and_fashion','Industry':'industry','Not people':'not_people','CCC':'ccc', 'CCC Not People':'ccc_not_people'}

target_lang_dict = language_names

metrics_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Creation Date':'date_created','Inlinks from CCC':'num_inlinks_from_CCC','Outlinks to CCC':'num_outlinks_to_CCC','LGBT Indicator':'lgbt_topic'}

metrics_dict_inv= {v: k for k, v in metrics_dict.items()}
show_gaps_dict = {'No language gaps':'no-gaps','At least one gap':'one-gap-min','Only gaps':'only-gaps'}





admin_page_types_inv = {'policies_and_guidelines':'Policies and guidelines', 'help':'Help', 'maintenance':'Maintenance', 'tools':'Tools', 'essays':'Essays', 'disclaimers':'Disclaimers', 'copyright':'Copyright', 'village_pump':'Village pump', 'deletion':'Deletion', 'wikiprojects':'WikiProjects', 'stubs':'Stubs'}
admin_page_types = {v: k for k, v in admin_page_types_inv.items()}
admin_page_types_inv.update({'all_topics':'All Topics'})


page_namespaces = {'ns4':'Wikipedia (ns4)','ns12':'Help (ns12)','ns14':'Category (ns14)','ns100':'Portal (ns100)'}
page_namespaces_inv = {v: k for k, v in page_namespaces.items()}

source_lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    source_lang_dict[lang_name] = languagecode

query_lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    query_lang_dict[lang_name] = languagecode

target_lang_dict = {} 
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    target_lang_dict[lang_name] = languagecode



content_dropdown = {'Admin Pages as a whole':'admin_page_types_whole','Type1':'type1', 'Namespace 1':'ns1', 'etc.':'etc.'}
page_metrics_dropdown = {'num_bytes':'Number of Bytes', 'num_references':'Number of References', 'num_images':'Number of Images', 'num_multilingual_sisterprojects':'Number of Pages in Multilingual Sister Projects', 'num_wdproperty':'Number WD Properties', 'num_wdidentifiers':'Number of WD Identifiers', 'num_outlinks':'Number of Outlinks', 'num_outlinks_to_admin_pages':'Number of Outlinks to Admin Pages', 'percent_outlinks_to_admin_pages':'Percent of Outlinks to Admin Pages', 'num_inlinks':'Number of Inlinks', 'num_interwiki':'Number of Interwiki', 'num_inlinks_from_admin_pages':'Number of Inlinks From Admin Pages', 'percent_inlinks_from_admin_pages':'Percent of Inlinks From Admin Pages', 'num_categories_contains':'Number of Categories Contained', 'num_pages_contains':'Number of Pages Contained', 'num_pages_admin_contains':'Number of Admin Pages Contained', 'num_pageviews':'Pageviews', 'num_edits':'Number of Edits', 'num_edits_last_month':'Number of Edits Last Month', 'active_months':'Number of Months With Edits', 'editing_days':'Number of Days With Edits', 'num_discussions':'Number of Discussion Edits', 'num_reverts':'Number of Reverts', 'num_anonymous_edits':'Number of Anon. Edits', 'num_bot_edits':'Number of Bot. Edits', 'num_editors':'Number of Editors', 'num_admin_editors':'Number of Admin. Editors', 'median_year_first_edit':"Median of Editors' Year of First Edit", 'median_editors_edits':"Median of Editors' Edits", 'num_edits_last_month_by_admin':'Number of Edits Last Month by Admin', 'num_edits_last_month_by_anonymous':'Number of Edits Last Month by Anonyomous', 'num_edits_last_month_by_newcomer_90d':'Number of Edits Last Month by a Newcomer of 90 Days', 'num_edits_last_month_by_newcomer_1y':'Number of Edits Last Month by a Newcomer of 1 Year', 'num_edits_last_month_by_newcomer_5y':'Number of Edits Last Month by a Newcomer of 5 Years', 'date_last_edit':'Date of the Last Edit', 'date_last_discussion':'Date of the Last Edit in the Discussion Page', 'days_last_50_edits':'Days Since The Last 50 Edits', 'days_last_5_edits':'Days Since The Last 5 Edits', 'days_last_edit':'Days Since The Last 5 Edit', 'date_created':'Date of Creation', 'timestamp':'Edit Timestamp', 'first_timestamp_lang':'Wikipedia where First Created', 'total_months':'Number of Months Since Creation', 'max_active_months_row':'Number of Months in a Row Editing Activity', 'max_inactive_months_row':'Number of Months in a Row with No Editing Activity', 'percent_active_months':'Percent of Months With Editing Activity', 'percent_editing_days':'Percent of Editing Days'} 
page_metrics_dropdown_inv = {v: k for k, v in page_metrics_dropdown.items()}



query_type_dict = {'Wikipedia Article Search':'search', 'Wikidata SPARQL Query':'sparql', 'List of articles':'alist', "List of categories' articles":'clist'}


type_query = '''You must choose the *Type of query*: Wikipedia Article Search, Wikidata SPARQL Query, List of articles, and List of categories articles. 

    * The *Wikipedia Article Search* allows you to introduce a query to the same search engine of Wikipedia has (CirrusSearch) and retrieve some articles. For example, if you introduce the Source Language English and the query "Japanese Cuisine", you will obtain the articles from English Wikipedia along with their main stats on relevance metrics (number of editors, edits, discussion edits, pageviews, etc.). When using the search option, you can introduce the *Language of the query* and specify which language you are using to query (e.g. You can query "cuisine du Japon" if you are using French).
    * The *Wikidata SPARQL Query* allows you to introduce a query in the textbox and retrieve the articles related to the Qitems that appear in them (if the query does not contain any Qitem and only labels, there will be no results).
    * The *List of articles* query simply allows you to introduce a list of articles (their titles or their URLs separated by a comma, semicolon or a line break) in the textbox in order to see the main stats and their availability in the *Target Languages*. 
    * The *List of categories' articles* allows you to introduce a list of categories and retrieve the articles contained in them. '''


text_results = '''
The following table shows the results from the query. The columns present the title in the source language, a set of metrics (number of inlinks, number of inlinks from the source language CCC, number of Outlinks, number of Bytes, number of References, number of Images, number of Editors, number of Edits, number of Discussions, number of Pageviews, numer of Interwiki links and number of Wikidata properties) from the article in the source language, the title in the first target language (in case it exists), and the languages in which it is available from the target languages.
'''


text_results2 = '''
The following table shows the resulting list of articles and their availability.

The Qitem column provides the id and a link to the Wikidata corresponding page. The column Title provides the title in the source language. The next columns (editors, edits, pageviews, interwiki, creation date) show the value for some metrics in the first source language. The column LGBT indicator tells the number of languages in which the algorithm identified this article as part of the LGBT culture. If the content is ordered by another metric, this is added as an extra column. The column Target Langs. provides links to the article version in each of the selected target languages. The last column shows the title in the first target language.
'''



## ----------------------------------------------------------------------------------------------------- ##

text_default = '''On this page, you can search for content gaps related to administrative pages in Wikipedia language editions. Search for valuable articles from a Wikipedia language edition that are missing in a target Wikipedia.'''



## ----------------------------------------------------------------------------------------------------- ##


interface_row1 = html.Div([



    html.Div(
    [
    html.P(
        [
            "Target ",
            html.Span(
                "language",
                id="tooltip-target-targetlanguages",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
            " where to find gaps",
        ]
    ),
    dbc.Tooltip(
        html.P("Select the target Wikipedia language editions in which you want to check whether the resulting list of articles exist or not.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-targetlanguages",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '400px'},
    ),

    ])




interface_row2 = html.Div([


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
        html.P("Select the language editions from which you want to retrieve the Admin Page Gaps they have in common. You can retrieve a single language if you want. The relevance metrics will be according to the first language.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-sourcelanguage",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    ])



interface_row3 = html.Div([

    html.Div(
    [
    html.P(
        [
            html.Span(
                "Admin page type",
                id="tooltip-admin-type",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '400px'},
    ),

    dbc.Tooltip(
        html.P('Select a Topic to filter the resulting articles to biographies, keywords or general topics.',
        style={"width": "auto", 'font-size': 12, 'text-align':'left','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-admin-type",
        placement="bottom",
        style={'color':'black','backgroundColor':'transparent'},
    ),


    html.Div(
    [
    html.P(
        [
            html.Span(
                "Admin namespaces",
                id="tooltip-admin-namespace",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '400px'},
    ),

    dbc.Tooltip(
        html.P('Select a Topic to filter the resulting articles to biographies, keywords or general topics.',
        style={"width": "auto", 'font-size': 12, 'text-align':'left','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-admin-namespace",
        placement="bottom",
        style={'color':'black','backgroundColor':'transparent'},
    ),


    html.Div(
    [
    html.P(
        [
            "Retrieve by ",
            html.Span(
                "metric",
                id="tooltip-target-retrieveby",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a metric to sort the results (by default it uses the number of languages that consider this article to belongs to the LGBT culture).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-orderby",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])



interface_row4 = html.Div([

    html.Div(
    [
    html.P(
        [
            "Type of ",
            html.Span(
                "query",
                id="tooltip-target-query",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '250px'},
    ),


    dbc.Tooltip(
        html.Div(
            dcc.Markdown(type_query.replace('  ', '')),
        style={'font-size': 12, 'width':'750px','text-align':'left','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-query",
        placement="bottom",
        style={"width": "800px",'color':'black','backgroundColor':'black'},
    ),



    html.Div(
    [
    html.P(
        [
            "Language of the ",
            html.Span(
                "query",
                id="tooltip-target-langq",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select the language in which you are making the query for a better performance (optional).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-langq",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

])

interface_row5 = html.Div([



    html.Div(
    [
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
            "Select a metric to sort the results (by default it uses the number of languages that consider this article to belongs to the LGBT culture).",
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
            "Show the ",
            html.Span(
                "gaps",
                id="tooltip-target-exclude",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select Show the gaps to limit the results to only the articles that are missing in the target languages (Only Gaps), that are missing in at least one language (At least one gap) or that are not missing (No language gaps).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-exclude",
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
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    )


])




def dash_app_tool_pg_build_layout(params):

    halflayout = html.Div([
            navbar,
            html.H3(general_title, style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

 
            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H6('Select the Wikipedia with page gaps'),
            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='target_lang',
                options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                value='it',
                placeholder="Select languages",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.H6('Search for new pages'),
            interface_row2,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': source_lang_dict[i]} for i in sorted(source_lang_dict)],
                value='en',
                placeholder="Select languages",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.P(html.B('Option A: Search for pages using Admin Pages Types and Namespaces')),
            interface_row3,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='admin_page_types',
                options=[{'label': i, 'value': admin_page_types[i]} for i in sorted(admin_page_types)],
                value='none',
                placeholder="Select one admin page type or more (max 2).",
                multi=True,
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),


            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='namespaces',
                options=[{'label': i, 'value': page_namespaces_inv[i]} for i in sorted(page_namespaces_inv)],
                value='none',
                placeholder="Select one namespace or more (max 3).",
                multi=True,
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='retrieve_by',
                options=[{'label': i, 'value': page_metrics_dropdown_inv[i]} for i in sorted(page_metrics_dropdown_inv)],
                value='none',
                placeholder="Retrieve by",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.P(html.B('Option B: Search for pages using keywords/queries')),
            interface_row4,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='query_type',
                options=[{'label': i, 'value': query_type_dict[i]} for i in query_type_dict],
                value='none',
                placeholder="Select the type of query",           
                style={'width': '240px'}
             ), style={'display': 'inline-block','width': '250px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='query_lang',
                options=[{'label': i, 'value': query_lang_dict[i]} for i in sorted(query_lang_dict)],
                value='none',
                placeholder="Select a query language (optional)",           
                style={'width': '240px'}
             ), style={'display': 'inline-block','width': '250px'}),

            html.P('Query or Input Data'),
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Textarea)(
                id='textbox',
                placeholder='You can introduce your search query or input data to obtain the results.',
                value='',
                style={'width': '100%', 'height':'100'}
             ), style={'display': 'inline-block','width': '590px'}),

            # LINE
            html.Br(),
            html.H6('Filter the results'),
            interface_row5,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': page_metrics_dropdown_inv[i]} for i in sorted(page_metrics_dropdown_inv)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show the gaps",           
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

            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

        ])
                   
    
    functionstartTime = time.time()


    # 'target_lang','source_lang','admin_page_types','namespaces','query_type','query_lang','retrieve_by','order_by','show_gaps','limit'
    if len(params)!=0 and params['target_lang'].lower()!='none' and params['source_lang'].lower()!='none': 
        # print (params)

        conn = sqlite3.connect(databases_path + 'wikipedia_administrative_pages_analytics_production.db'); cur = conn.cursor()



        # TARGET LANGUAGE
        target_lang = params['target_lang'].lower()
        target_lang = target_lang.split(',')
        target_language = languages.loc[target_lang[0]]['languagename']


        # SOURCE lANGUAGE
        source_lang = params['source_lang'].lower()
        source_language = languages.loc[source_lang]['languagename']

        # CONTENT
        if 'admin_page_types' in params:
            admin_page_types_selected = params['admin_page_types']
        else:
            admin_page_types_selected = 'none'


        if 'namespaces' in params:
            namespaces = params['namespaces']
        else:
            namespaces = 'none'

        if 'retrieve_by' in params:
            retrieve_by = params['retrieve_by']
        else:
            retrieve_by = 'none'



        if 'query_type' in params:
            query_type = params['query_type']
        else:
            query_type = 'none'

        if 'query_lang' in params:
            query_lang = params['query_lang']
        else:
            query_lang = 'none'


        if 'textbox' in params:
            textbox=params['textbox']
        else:
            textbox='textbox'





        # FILTER
        if 'order_by' in params:
            order_by = params['order_by']
        else:
            order_by = 'none'

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

        query += 'r.num_editors, r.num_edits, r.num_pageviews, r.num_interwiki, r.num_bytes, r.date_created, '

        columns = ['num','qitem','page_title','num_editors','num_edits','num_pageviews','num_interwiki','num_bytes','date_created','lgbt_topic']

        if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references']: 
            query += 'r.'+order_by+', '
            columns = columns + [order_by]

        query += 'r.lgbt_topic '

        query += ' FROM '+source_lang+'wiki r '
        query += 'WHERE r.lgbt_topic > 0 '


        if topic != "none" and topic != "None" and topic != "all":
            if topic == 'keywords':
                query += 'AND r.lgbt_keyword_title IS NOT NULL '
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
            elif topic == 'ccc':
                query += 'AND r.ccc_binary = 1 AND percent_outlinks_to_CCC > 0.15 '
            elif topic == 'ccc_not_people':
                query += 'AND r.ccc_binary = 1 AND percent_outlinks_to_CCC > 0.15 AND r.gender IS NULL '
            else:
                query += 'AND r.'+topic+' IS NOT NULL '


        if order_by == "none" or order_by == "None":
#            pass
            query += 'ORDER BY r.lgbt_topic DESC '

        elif order_by in ['num_outlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references','num_pageviews']: 
            query += 'ORDER BY r.'+order_by+' DESC '

        if limit != "none":
            query += 'LIMIT '+str(limit*10)+';'
        else:
            query += 'LIMIT 500;'
            # query += 'LIMIT '+str(limit)+';'


        columns = columns + ['target_lang']

        # print (query)
        # print (show_gaps)
 
        df = pd.read_sql_query(query, conn)#, parameters)
#        df = pd.read_csv(databases_path + 'lgbt.csv')

        # print (df.head(10))
        # print (len(df))
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')


        # df = df.head(limit)
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(source_lang); mysql_cur_read = mysql_con_read.cursor()
        df = wikilanguages_utils.get_interwikilinks_articles(source_lang, target_lang, df, mysql_con_read)




        if order_by == "none" or order_by == "None":
            df = df.sort_values(by='lgbt_topic', ascending=False)
        else:
            df = df.sort_values(by=order_by, ascending=False)

        df = df.fillna('')

        columns_dict = {'num':'Nº','page_title':source_language+' Title','target_lang':'Target Langs.','qitem':'Qitem'}
        columns_dict.update(page_metrics_dropdown)



        main_title = 'LGBT+ articles retrieved from '+source_language+' Wikipedia and its coverage by the target languages'





        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([


                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                footbar,

            ], className="container")

            return layout




        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
        # print (df.columns)
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')
        # print (show_gaps)

        # # PREPARE THE DATA
        df=df.rename(columns=columns_dict)
        # print (df.head(100))       

        columns_ = []
        for x in columns:
            columns_.append(columns_dict[x])
        columns = columns_
        columns.append(target_language+' Title')
        # print (columns)

        df_list = list()
        k = 0
        z = 0
        for index, rows in df.iterrows():
            df_row = list()

            for col in columns:
                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == source_language+' Title':
                    title = rows[source_language+' Title']
                    if not isinstance(title, str):
                        title = title.iloc[0]
                    df_row.append(html.A(title.replace('_',' '), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == target_language+' Title':

                    t_title = rows['page_title_1']
                    if isinstance(t_title, str):  
                        df_row.append(html.A(t_title.replace('_',' '), href='https://'+target_lang[0]+'.wikipedia.org/wiki/'+t_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Outlinks' or col == 'References' or col == 'Images':
                    title = rows[source_language+' Title']
                    df_row.append(html.A( rows[col], href='https://'+target_lang[0]+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

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
                    title = rows[source_lang+' Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Creation Date':
                    date = rows['Creation Date']
                    if date == 0 or date == '' or date == None: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)

                elif col == 'Target Langs.':

                    z=0
                    target_lang_titles = []
                    for i in range(1,len(target_lang)+1):
                        if rows['page_title_'+str(i)] == '':
                            z+=1
                        target_lang_titles.append(rows['page_title_'+str(i)])
#                    print (target_lang_titles)

                    text = ''

                    for x in range(0,len(target_lang)):
                        cur_title = target_lang_titles[x]
                        if cur_title!= None and cur_title != '' and cur_title != 0:
                            if text!='': text+=', '

                            text+= '['+target_lang[x]+']'+'('+'http://'+target_lang[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'
    
                    df_row.append(dcc.Markdown(text))

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'LGBT Indicator':
                    df_row.append(rows['LGBT Indicator'])

                else:
                    df_row.append(rows[col])

            # print (rows)
            # print (len(rows))
            # print (len(df_row))


            if show_gaps == 'one-gap-min' and z == 0: continue
            elif show_gaps == 'only-gaps' and z < len(target_lang_titles): continue
            elif show_gaps == 'no-gaps' and z > 0: continue

            
            if k <= limit:
                df_list.append(df_row)

        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')
        # print (df.head(10))


        # RESULTS
        df1 = pd.DataFrame(df_list)
        dash_app_tool_pg.title = general_title+title_addenda

        # LAYOUT
        layout = html.Div([


            # here there is the table            
            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(text_results.replace('  ', '')),

            html.Br(),
            html.H6(main_title, style={'textAlign':'center'}),


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
#        print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' before printing')


    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([

            halflayout,


            footbar,
        ], className="container")

    return layout



# option A
@dash_app_tool_pg.callback(
    [Output('query_type', 'disabled'),Output('query_lang', 'disabled'),Output('textbox','disabled')],
    [Input('admin_page_types', 'value'),Input('namespaces', 'value'),Input('retrieve_by', 'value')]
)
def update_output(value1, value2, value3):
    # print (value1, value2)
    # print ('A')

    # quan un dels dos no és buit, aleshores anul·lem B.
    if (type(value1) == list and len(value1)>0) or (type(value2) == list and len(value2)>0) or (value3!= None and value3.lower() != 'none'):
        return True, True, True
    else:
        return False, False, False


# option B
@dash_app_tool_pg.callback(
    [Output('admin_page_types', 'disabled'),Output('namespaces', 'disabled'),Output('retrieve_by','disabled')],
    [Input('query_type', 'value'),Input('query_lang', 'value'),Input('textbox','value')]
)
def update_output2(value1,value2,value3):
    print (value1, value2, value3)
    print ('B')
    if (value1 != None and value1.lower() != 'none') or (value2 != None and value2.lower() != 'none') or (value3.lower() != 'none' and value3 != ''):
        return True, True, True
    else:
        return False, False, False




# callback update URL
component_ids_app_tool_pg =  ['target_lang','source_lang','admin_page_types','namespaces','query_type','query_lang','retrieve_by','order_by','show_gaps','limit']
@dash_app_tool_pg.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app_tool_pg])
def update_url_state(*values):
#    print (values) 

#    if not isinstance(values[2], str) or not isinstance(values[3], str):
    values = values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9]

    state = urlencode(dict(zip(component_ids_app_tool_pg, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app_tool_pg.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps_wapa.parse_state(href)
    return dash_app_tool_pg_build_layout(state)




    
# if __name__ == '__main__':
#     dash_app_tool_pg.run_server(debug=True)#,dev_tools_ui=False)

