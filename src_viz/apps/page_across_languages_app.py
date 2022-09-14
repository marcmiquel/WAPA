# -*- coding: utf-8 -*-
import sys
import dash_apps_wapa
sys.path.insert(0, '/srv/wapa/src_viz')
from dash_apps_wapa import *





### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_tool_pal = Dash(__name__, server = app_wapa, url_base_pathname = webtype + '/page_across_languages/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
#dash_app_tool_pal = Dash(url_base_pathname = '/page_across_languages/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app_tool_pal.config['suppress_callback_exceptions']=True

general_title = 'Page Across Languages'
dash_app_tool_pal.title = general_title+title_addenda
print (general_title)


dash_app_tool_pal.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])






source_lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    source_lang_dict[lang_name] = languagecode


page_metrics_dropdown = {'num_bytes':'Number of Bytes', 'num_references':'Number of References', 'num_images':'Number of Images', 'num_multilingual_sisterprojects':'Number of Pages in Multilingual Sister Projects', 'num_wdproperty':'Number WD Properties', 'num_wdidentifiers':'Number of WD Identifiers', 'num_outlinks':'Number of Outlinks', 'num_outlinks_to_admin_pages':'Number of Outlinks to Admin Pages', 'percent_outlinks_to_admin_pages':'Percent of Outlinks to Admin Pages', 'num_inlinks':'Number of Inlinks', 'num_interwiki':'Number of Interwiki', 'num_inlinks_from_admin_pages':'Number of Inlinks From Admin Pages', 'percent_inlinks_from_admin_pages':'Percent of Inlinks From Admin Pages', 'num_categories_contains':'Number of Categories Contained', 'num_pages_contains':'Number of Pages Contained', 'num_pages_admin_contains':'Number of Admin Pages Contained', 'num_pageviews':'Number of Pageviews', 'num_edits':'Number of Edits', 'num_edits_last_month':'Number of Edits Last Month', 'active_months':'Number of Months With Edits', 'editing_days':'Number of Days With Edits', 'num_discussions':'Number of Discussion Edits', 'num_reverts':'Number of Reverts', 'num_anonymous_edits':'Number of Anon. Edits', 'num_bot_edits':'Number of Bot. Edits', 'num_editors':'Number of Editors', 'num_admin_editors':'Number of Admin. Editors', 'median_year_first_edit':"Median of Editors' Year of First Edit", 'median_editors_edits':"Median of Editors' Edits", 'num_edits_last_month_by_admin':'Number of Edits Last Month by Admin', 'num_edits_last_month_by_anonymous':'Number of Edits Last Month by Anonyomous', 'num_edits_last_month_by_newcomer_90d':'Number of Edits Last Month by a Newcomer of 90 Days', 'num_edits_last_month_by_newcomer_1y':'Number of Edits Last Month by a Newcomer of 1 Year', 'num_edits_last_month_by_newcomer_5y':'Number of Edits Last Month by a Newcomer of 5 Years', 'date_last_edit':'Date of the Last Edit', 'date_last_discussion':'Date of the Last Edit in the Discussion Page', 'days_last_50_edits':'Days Since The Last 50 Edits', 'days_last_5_edits':'Days Since The Last 5 Edits', 'days_last_edit':'Days Since The Last 5 Edit', 'date_created':'Date of Creation', 'timestamp':'Edit Timestamp', 'first_timestamp_lang':'Wikipedia where First Created', 'total_months':'Number of Months Since Creation', 'max_active_months_row':'Number of Months in a Row Editing Activity', 'max_inactive_months_row':'Number of Months in a Row with No Editing Activity', 'percent_active_months':'Percent of Months With Editing Activity', 'percent_editing_days':'Percent of Editing Days'} 
page_metrics_dropdown_inv = {v: k for k, v in page_metrics_dropdown.items()}




## ----------------------------------------------------------------------------------------------------- ##

text_default = '''On this page, you can compare an admin. page characteristics across Wikipedia language editions. Introduce the page title and the metrics you want to compare. Browse the results and sort the columns.'''     


text_results = '''
The following table shows the resulting list of articles about LGBT in the source language and its availability in the target languages.

The Qitem column provides the id and a link to the Wikidata corresponding page. The column Title provides the title in the source language. The next columns (editors, edits, pageviews, interwiki, creation date) show the value for some features in the first source language. The column LGBT indicator tells the number of languages in which the algorithm identified this article as part of the LGBT culture. If the content is ordered by another feature, this is added as an extra column. The column Target Langs. provides links to the article version in each of the selected target languages. The last column shows the title in the first target language.
'''


## ----------------------------------------------------------------------------------------------------- ##


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
        html.P("Select the language editions from which you want to retrieve the Admin Page Gaps they have in common. You can retrieve a single language if you want. The relevance features will be according to the first language.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-sourcelanguage",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    html.Div(
    [
    html.P(
        [
            "Enter a ",
            html.Span(
                "Page title",
                id="tooltip-page-title",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
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

    html.Div(
    [
    html.P(
        [   
            "Enter a ",
            html.Span(
                "Qitem",
                id="tooltip-qitem",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '200px'},
    ),

    dbc.Tooltip(
        html.P('Select a Topic to filter the resulting articles to biographies, keywords or general topics.',
        style={"width": "auto", 'font-size': 12, 'text-align':'left','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-qitem",
        placement="bottom",
        style={'color':'black','backgroundColor':'transparent'},
    ),


    ])



interface_row2 = html.Div([



    html.Div(
    [
    html.P(
        [
            "Choose the ",
            html.Span(
                "columns",
                id="tooltip-columns",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature to sort the results (by default it uses the number of languages that consider this article to belongs to the LGBT culture).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-columns",
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
            "Order by ",
            html.Span(
                "feature",
                id="tooltip-target-orderby",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature to sort the results (by default it uses the number of languages that consider this article to belongs to the LGBT culture).",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-orderby",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


])



def dash_app_tool_pal_build_layout(params):
    half_layout = html.Div([
        navbar,
        html.H3(general_title, style={'textAlign':'center'}),
        html.Br(),

        dcc.Markdown(text_default.replace('  ', '')),
        html.Br(),
        html.H5('Select the admin page'),

        html.P(
            html.B('Option 1: Choose a language and enter a page title'),
            style={'display': 'inline-block','width': '600px'}),

        html.P(
            html.B('Option 2: Enter a Qitem'),
            style={'display': 'inline-block','width': '400px'}),

        interface_row1,
        html.Div(
        dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
            id='source_lang',
            options=[{'label': i, 'value': source_lang_dict[i]} for i in sorted(source_lang_dict)],
            # value='none',
            placeholder="Select a language",
            style={'width': '190px'}
         ), style={'display': 'inline-block','width': '200px'}),
#        dcc.Link('Query',href=""),

        html.Div(
        dash_apps_wapa.apply_default_value(params)(dcc.Input)(
            id='page_title',
            type='text',
            placeholder="Paste or write a page title",
            style={'width': '390px'}
         ), style={'display': 'inline-block','width': '400px'}),

        html.Div(
        dash_apps_wapa.apply_default_value(params)(dcc.Input)(
            id='qitem',
            type='text',
            placeholder="Paste or write a Qitem",
            style={'width': '190px'}
         ), style={'display': 'inline-block','width': '200px'}),


        # LINE
        html.Br(),
        html.H5('Select the metrics'),
        interface_row2,
        html.Div(
        dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
            id='metrics',
            options=[{'label': i, 'value': page_metrics_dropdown_inv[i]} for i in sorted(page_metrics_dropdown_inv)],
            value=['date_created','num_bytes','num_pageviews','num_interwiki','num_inlinks','num_editors','num_edits_last_month_by_newcomer_1y','days_last_5_edits'],
            placeholder="Select metrics",
            multi=True,           
            style={'width': '790px'}
         ), style={'display': 'inline-block','width': '800px'}),

        html.Br(),
        interface_row3,
        html.Div(
        dash_apps_wapa.apply_default_value(params)(dcc.Dropdown)(
            id='sort_by',
            options=[{'label': i, 'value': page_metrics_dropdown_inv[i]} for i in sorted(page_metrics_dropdown_inv)],
            value='none',
            placeholder="Sort the table by (optional)",           
            style={'width': '390px'}
         ), style={'display': 'inline-block','width': '400px'}),

        html.Div(
        html.A(html.Button('Query Results!'),
            href=''),
        style={'display': 'inline-block','width': '200px'}),

    ])

    if len(params)!=0 and (params['source_lang'].lower()!='none' or params['qitem'][0].lower()=='q'): 
        # print (params)

        # SOURCE lANGUAGE
        if 'source_lang' in params:
            source_lang = params['source_lang'].lower()
            if source_lang in wikilanguagecodes:
                source_language = languages.loc[source_lang]['languagename']
            else:
                source_lang = 'none'
                source_language = 'none'
        else:
            source_lang = 'none'
            source_language = 'none'

        if 'page_title' in params:
            page_title = params['page_title'].replace(' ','_')
        else:
            page_title = 'none'


        if 'qitem' in params:
            qitem = params['qitem']
        else:
            qitem = 'none'

        if 'metrics' in params:
            metrics = params['metrics']
        else:
            metrics = 'none'


        # FILTER
        if 'sort_by' in params:
            sort_by = params['sort_by']
        else:
            sort_by = 'none'


#        print (source_lang, source_language, page_title, qitem, metrics, sort_by)

        # CREATING THE QUERY FROM THE PARAMS
        list_df = []
        conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()

        page_title_qitem = None
        if source_lang in wikilanguagecodes:
            namespaces_dict = lang_namespaces_dict[source_lang]

            if ':' in page_title:
                for ns, ns_value in namespaces_dict.items():
                    if ns in page_title:
                        query = 'SELECT qitem FROM '+source_lang+'wiki_pages WHERE page_title = ? AND page_namespace = '+str(ns_value)+';'
                        page_title = page_title.replace(ns,'')
                        break
            else:
                query = 'SELECT qitem FROM '+source_lang+'wiki_pages WHERE page_title = ? AND page_namespace IN (4, 12);'
            page_title_qitem = pd.read_sql_query(query, conn, params = [page_title]).values.tolist()[0][0]
     

        if page_title_qitem != None:
            qitem = page_title_qitem
        if qitem != 'none':
            #qitem = 'Q4656487'
            df = pd.DataFrame(columns=['languagecode','page_id', 'page_namespace', 'page_title', 'date_created', 'num_bytes', 'num_pageviews', 'num_interwiki', 'num_editors', 'days_last_5_edits'])
            for languagecode in wikilanguagecodes:
                query = 'SELECT "'+languagecode+'wiki" as languagecode, page_id, page_namespace, page_title, date_created, num_bytes, num_pageviews, num_interwiki, num_editors, days_last_5_edits FROM '+languagecode+'wiki_pages WHERE qitem = "'+qitem+'";'
                dfx = pd.read_sql_query(query, conn);
                list_df.append(dfx)
            new_df = pd.concat(list_df)

        df = new_df

        if sort_by == "none" or sort_by == "None":
            df = df.sort_values(by='num_bytes', ascending=False)
        else:
            df = df.sort_values(by=sort_by, ascending=False)

        df = df.fillna('')



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([
                half_layout,
                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                footbar,

            ], className="container")

            return layout



        columns_dict = {'num':'Nº','page_title':' Title'}
        columns_dict.update(page_metrics_dropdown)
        df=df.rename(columns=columns_dict)

        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
        # print (df.columns)
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')
        # print (show_gaps)
        datatable = dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{"name": i, "id": i} for i in df.columns],
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable="multi",
            row_deletable=True,
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current= 0,
            page_size= 500
            )



        # # PREPARE THE DATA

        # print (df.head(100))       

        print (metrics)
        columns = []
        columns_ = []
        # columns.append(target_language+' Title')
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

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                else:
                    df_row.append(rows[col])
            
            df_list.append(df_row)

        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')
        # print (df.head(10))


        # RESULTS
        df1 = pd.DataFrame(df_list)
        dash_app_tool_pal.title = general_title+title_addenda

        # LAYOUT
        layout = html.Div([
            half_layout,

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(text_results.replace('  ', '')),

            html.Br(),
            datatable,

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
            half_layout,
            footbar,
        ], className="container")

    return layout


@dash_app_tool_pal.callback(
    [Output('page_title', 'disabled'),Output('source_lang', 'disabled')],
    Input('qitem', 'value')
)
def update_output(value):
    if value != None and value != '' and value != 'None':
        return True, True
    else:
        return False, False


@dash_app_tool_pal.callback(
    Output('qitem', 'disabled'),
    [Input('page_title', 'value'),Input('source_lang', 'value')]
)
def update_output(value1,value2):
    if (value1 != None or value2 != None) and value1 != 'None' and value2 != 'None' and value1 != '':
        return True
    else:
        return False



# callback update URL
component_ids_app_tool_agf =  ['source_lang','page_title','qitem','metrics','sort_by']
@dash_app_tool_pal.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app_tool_agf])
def update_url_state(*values):
#    print (values)

    if not isinstance(values[3], str):
        values = values[0],values[1],values[2],','.join(values[3]),values[4]

    state = urlencode(dict(zip(component_ids_app_tool_agf, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app_tool_pal.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps_wapa.parse_state(href)
    return dash_app_tool_pal_build_layout(state)




    
# if __name__ == '__main__':
#     dash_app_tool_pal.run_server(debug=True)#,dev_tools_ui=False)

