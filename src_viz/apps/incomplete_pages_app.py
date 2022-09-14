import sys
import dash_apps_wapa
sys.path.insert(0, '/srv/wapa/src_viz')
from dash_apps_wapa import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app_tool_ip = Dash(__name__, server = app_wapa, url_base_pathname = webtype + '/incomplete_pages/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app_tool_ip.config['suppress_callback_exceptions']=True

general_title = 'Incomplete Pages'
dash_app_tool_ip.title = general_title+' '+title_addenda
dash_app_tool_ip.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])




lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    lang_dict[lang_name] = languagecode



admin_page_types_inv = {'policies_and_guidelines':'Policies and guidelines', 'help':'Help', 'maintenance':'Maintenance', 'tools':'Tools', 'essays':'Essays', 'disclaimers':'Disclaimers', 'copyright':'Copyright', 'village_pump':'Village pump', 'deletion':'Deletion', 'wikiprojects':'WikiProjects', 'stubs':'Stubs'}
admin_page_types = {v: k for k, v in admin_page_types_inv.items()}
admin_page_types_inv.update({'all_topics':'All Topics'})


page_namespaces = {'ns4':'Wikipedia (ns4)','ns12':'Help (ns12)','ns14':'Category (ns14)','ns100':'Portal (ns100)'}
page_namespaces_inv = {v: k for k, v in page_namespaces.items()}





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

text_base = '''In this page you can check the completeness of a list of articles from a language edition you introduce manually or a Top CCC list by comparing it to the versions of the articles in other language editions. In other words, you can compare each article stats (number of Bytes, number of references, number of images, number of outlinks, among others) in other languages, and then, decide whether to expand these articles or not. 
'''


text_default = '''On this page, you can search for content gaps related to administrative pages in Wikipedia language editions. Search for valuable articles from a Wikipedia language edition that are missing in a target Wikipedia.'''





interface_row1 = html.Div([


    html.Div(
    [
    html.P(
        [
            "Wikipedia ",
            html.Span(
                "language edition",
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



interface_row2 = html.Div([

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



interface_row3 = html.Div([

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
        style={'font-size': 12, 'text-align':'left','padding': '12px 12px 12px 12px'}
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





interface_row4 = html.Div([


    html.Div(
    [
    html.P(
        [
            "Show only ",
            html.Span(
                "metric",
                id="tooltip-target-shfeat",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Filter the results and show only those limited to a list of metrics.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-shfeat",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '400px'},
    ),



    html.Div(
    [
    html.P(
        [
            "Show only pages from ",
            html.Span(
                "language",
                id="tooltip-target-lang",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Filter the results and show only those limited to a specific Wikipedia language edition.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-lang",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])



interface_row5= html.Div([


    html.Div(
    [
    html.P(
        [
            "Order ",
            html.Span(
                "by",
                id="tooltip-order-by",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a metric to sort the results.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-order-by",
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
                "number of articles",
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
    ),

    ])





'''


option_B = html.Div([
    html.Div(
    [
    html.P(
        [
            html.Span(
                "Option B",
                id="tooltip-target-optionB",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
            ": Paste a list of articles' titles",
        ], style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}
    ),
    dbc.Tooltip(
        html.P(
            "For the Option B: you need to choose a *source language* and paste the list of articles (titles or full URL) separated by a comma, semicolon or a line feed.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-optionB",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '400px'},
    )
    ])





interface_row4 = html.Div([


    html.Div(
    [
    html.P(
        [
            "Order articles ",
            html.Span(
                "by",
                id="tooltip-target-feat",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a metric to sort the results.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-feat",
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
                "number of articles",
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
    ),

    ])


'''


def dash_app_tool_ip_build_layout(params):



    halflayout = html.Div([
            navbar,
            html.H3(general_title, style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

 
            # HERE GOES THE INTERFACE
            # LINE 
            html.H6('Search for incomplete pages'),
            interface_row1,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': lang_dict[i]} for i in sorted(lang_dict)],
                value='en',
                placeholder="Select languages",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.P(html.B('Option A: Search for pages using Admin Pages Types/Namespaces')),
            interface_row2,
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
            interface_row3,
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
                options=[{'label': i, 'value': lang_dict[i]} for i in sorted(lang_dict)],
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
            html.H5('Filter the results'),

            interface_row4,
            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id = 'show_only_metric',
                options=[{'label': i, 'value': page_metrics_dropdown_inv[i]} for i in sorted(page_metrics_dropdown_inv)],
                value='none',
                multi=True,
                placeholder="Show only articles more complete in these metrics",           
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),

            html.Div(
            dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
                id='show_only_lang',
                options=[{'label': i, 'value': lang_dict[i]} for i in sorted(lang_dict)],
                value='none',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Br(),

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
            dash_apps_wapa.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='300',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

   
            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

        ])
                   


    if len(params)!=0 and (params['source_lang'].lower()!='none' or params['source_lang_text'].lower()!='none'):

        if params['list'].lower()=='none':
            list_name='editors'
        else:
            list_name=params['list'].lower()
    
        if params['source_lang'].lower()!='none':
            source_lang=params['source_lang'].lower()
            lists = 1
            text = 0
        else:
            source_lang=params['source_lang_text'].lower()
            text = 1
            lists = 0

        if 'source_country' in params:
            country=params['source_country'].upper()
            if country == 'NONE' or country == 'ALL': country = 'all'
        else:
            country = 'all'


        if 'textbox' in params:
            textbox=params['textbox'].lower()
        else:
            textbox='textbox'

        if 'order_by' in params:
            order_by=params['order_by'].lower()
        else:
            order_by='none'

        source_language = languages.loc[source_lang]['languagename']
        # print (source_lang,lists,text)

        if 'limit' in params:
            limit = params['limit']
        else:
            limit = 'none'

        if 'show_only_metric' in params and params['show_only_metric']!='none':
            show_only = set()
            a = params['show_only_metric'].split(',')
            for x in a: show_only.add(metrics_dict_inv[x])
        else:
            show_only = set()
#        print (show_only)
        s_len = len(show_only)

        if 'show_only_lang' in params and params['show_only_lang'].lower()!='none':
            show_only_lang=params['show_only_lang'].lower()
#            print (show_only_lang)
        else:
            show_only_lang='none'

        conn = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cur = conn.cursor()

        if lists == 1:
            conn2 = sqlite3.connect(databases_path + 'top_ccc_articles_production.db'); cur2 = conn2.cursor()

            # COLUMNS
            query = 'SELECT qitem, f.num_wdproperty, f.num_interwiki, f.page_title_original as page_title, '

            query += 'f.num_inlinks, f.num_outlinks, f.num_bytes, f.num_references, f.num_images, f.num_edits, f.num_editors, f.num_discussions, f.num_pageviews, f.metricd_article '

            query += 'FROM '+source_lang+'wiki_top_articles_lists r '
            query += 'INNER JOIN '+source_lang+'wiki_top_articles_metrics f USING (qitem) '
            query += "WHERE r.list_name = '"+list_name+"' "
            if country: query += 'AND r.country IS "'+country+'" '

            if order_by != 'none':
                query += 'ORDER BY f.'+order_by+' DESC;'
            else:
                query += 'ORDER BY r.position ASC;'

            df = pd.read_sql_query(query, conn2)#, parameters)
            df = df.fillna(0)

        else:
            # COLUMNS
            query = 'SELECT qitem, num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_images, num_edits, num_editors, num_discussions, num_pageviews, metricd_article '

            query += 'FROM '+source_lang+'wiki '

            page_titles = list(text_to_pageids_page_titles(source_lang, textbox).values())
            page_asstring = ','.join( ['?'] * len(page_titles) )
            query += 'WHERE page_title IN (%s) ' % page_asstring

            if order_by != 'none':
                query += 'ORDER BY '+order_by+' DESC '

            page_titles = tuple(page_titles)
            df = pd.read_sql_query(query, conn, params = page_titles)#, parameters)
            df = df.fillna(0)

        df['languagecode'] = '_original_'


        qitems = df.qitem.tolist()
        # functionstartTime = time.time()

        for lang in closest_langs[source_lang]: 

            page_asstring = ','.join( ['?'] * len(qitems) )
            query = 'SELECT qitem, num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_images, num_edits, num_editors, num_discussions, num_pageviews, metricd_article '

            query += 'FROM '+lang+'wiki WHERE qitem IN (%s);' % page_asstring

            df1 = pd.read_sql_query(query, conn, params = tuple(qitems))#, parameters)
            df1 = df1.fillna(0)
            df1['languagecode'] = lang

            df = df.append(df1)

        df = df.sort_values(['qitem', 'languagecode'], ascending=[True, True])

        # print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        df=df.rename(columns=columns_dict)

        columns = df.columns.tolist()
        columns.insert(0,'Nº')
#        print (columns)

        # print (columns)
        # print (df.head(10))
        # print (show_only)
        # print (s_len)
        # print (show_only_lang)

        k=0
        df_list = list()
        for index, rows in df.iterrows():

            if rows['Lang.'] == '_original_':
                ref_rows = rows
                continue

            if show_only_lang != 'none' and rows['Lang.'] != show_only_lang:
                continue

            keep_row = set()

            df_row = list()
            for col in columns:
                # num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_images, metricd_article

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))   
                                 
                elif col == 'Lang.':
                    link = html.A(rows['Lang.'], href='https://'+rows['Lang.']+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none'})
                    df_row.append(link)

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Article Title':

                    abbr_label = html.Abbr(rows['Article Title'].replace('_',' '),title=source_language+' Wikipedia article title: '+ref_rows['Article Title'].replace('_',' '))
                    link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                    df_row.append(link)

                elif col == 'Bytes':
#                    print (rows[col])
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  round(float(int(value - value_original)/1000),1)

                        color = html.Div('+'+str(difference)+'k', style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(round(value_original/1000,1))+'k')
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(round(value/1000,1))+'k',title=col+' in ' + source_language+' Wikipedia article: '+str(round(value_original/1000,1))+'k')
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)


                elif col == 'Images' or col == 'References' or col == 'Outlinks':

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'metricd Article': 

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original == 0 and value != 0:
                        color = html.Div('Yes', style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(abbr_label)
                        keep_row.add(col)
                    elif value != 0:                       
                        link = html.A('Yes', href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)
                    else:
                        link = html.A('No', href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)

                elif col == 'Inlinks':

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)

                elif col == 'Editors' or col == 'Edits':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'Discussions':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'Pageviews':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://tools.wmflabs.org/pageviews/?project='+rows['Lang.']+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://tools.wmflabs.org/pageviews/?project='+rows['Lang.']+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

            if len(keep_row) > 0:              
                if show_only == None:
                    df_list.append(df_row)

                elif len(keep_row.intersection(show_only))==s_len:
                    df_list.append(df_row)
                else:
                    k=k-1
            else:
                k=k-1

        df1 = pd.DataFrame(df_list)

        # NO RESULTS PAGE
        if len(df1) == 0: # there are no results.
            layout = html.Div([
     
                html.Br(),
                html.Br(),

                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately the list of incomplete articles is empty for this language and parameters. Try another combination of parameters and query again.'),

                footbar,

            ], className="container")

            return layout



        # RESULTS PAGE
        layout = html.Div([

            halflayout,

            html.Br(),
            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(text_table.replace('  ', '')),

            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(df_row[x]) for x in range(len(columns))
            ]) for df_row in df_list]),

            footbar,

        ], className="container")

    else:
        # FIRST PAGE
        layout = html.Div([

            halflayout,
            footbar,


        ], className="container")


    return layout



def text_to_pageids_page_titles(languagecode, textbox):

    textbox = textbox.lower()
    page_titles = []

    if ('.org') in textbox:
        textbox = textbox.replace('https://'+languagecode+'.wikipedia.org/wiki/','')

    if '\n' in textbox:
        textbox = textbox.replace('\n','\t')

    if ';' in textbox:
        textbox = textbox.replace(';','\t')

    if ',' in textbox:
        textbox = textbox.replace(',','\t')

    page_titles = textbox.split('\t')

    page_titles = set(page_titles)

    params = []
    for x in page_titles:
        x = str(x)
        params.append(x.replace(' ','_'))

    page_asstring = ','.join( ['%s'] * len(params) )

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    query = 'SELECT page_id, page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND CONVERT(page_title USING utf8mb4) COLLATE utf8mb4_general_ci IN (%s)' % page_asstring

    mysql_cur_read.execute(query,params)
    rows = mysql_cur_read.fetchall()

    page_dict = {}
    for row in rows:
        page_id = row[0]
        page_title = row[1].decode('utf-8')
        page_dict[page_id] = page_title


    return page_dict




# option A
@dash_app_tool_ip.callback(
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
@dash_app_tool_ip.callback(
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
component_ids_app_tool_ip =  ['source_lang','admin_page_types','namespaces','query_type','query_lang','retrieve_by','show_only_metric','show_only_lang','order_by','limit']
@dash_app_tool_ip.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app_tool_ip])
def update_url_state(*values):
#    print (values)

    values = values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9]

    state = urlencode(dict(zip(component_ids_app_tool_ip, values)))
    return '?'+state


# callback update page layout
@dash_app_tool_ip.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps_wapa.parse_state(href)
    return dash_app_tool_ip_build_layout(state)