# -*- coding: utf-8 -*-

# time
import time
import datetime
import dateutil
import calendar
# system
import os
import sys
import re
import csv
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# requests and others
import requests
# files
import gzip
import zipfile
import bz2
import json
import csv
import codecs
# data
import pandas as pd


databases_path = '/srv/wapa/databases/'
datasets_path = '/srv/wapa/datasets/'
dumps_path = '/srv/wapa/dumps/'

wikidata_db = 'wikidata.db'

diversity_categories_db = 'diversity_categories.db'
diversity_categories_production_db  = 'diversity_categories_production.db'

wikipedia_administrative_pages_analytics_db = 'wikipedia_administrative_pages_analytics.db'
wikipedia_administrative_pages_analytics_production_db = 'wikipedia_administrative_pages_analytics_production.db'

stats_db = 'stats.db'
stats_production_db = 'stats_production.db'

images_db = 'images.db'
images_production_db = 'images_production.db'

revision_db = 'revision.db'
imageslinks_db = 'imagelinks.db'

administrative_pages_analytics_log = 'administrative_pages_analytics_log.db'


# Loads language_territories_mapping.csv file
def load_wikipedia_languages_territories_mapping():

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();  

    query = 'SELECT WikimediaLanguagecode, languagenameEnglishethnologue, territoryname, territorynameNative, QitemTerritory, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, officialnationalorregional, region, subregion, intermediateregion FROM wikipedia_languages_territories_mapping;'

    territories = pd.read_sql_query(query, conn)
    territories = territories.set_index(['WikimediaLanguagecode'])

#    print (territories.head(20))

# READ FROM META: 
# check if there is a table in meta:
#    generate_print_language_territories_mapping_table()
# uses pywikibot and verifies the differences with the file.
# sends an e-mail with the difference and the 'new proposed file'
#    send_email_toolaccount('subject', 'message')
# stops.
# we verify this e-mail, we re-start the task.
    return territories


# Loads wikipedia_language_editions.csv file
def load_wiki_projects_information():
    # in case of extending the project to other WMF sister projects, it would be necessary to revise these columns and create a new file where a column would specify whether it is a language edition, a wikictionary, etc.

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();

    query = 'SELECT languagename, Qitem, WikimediaLanguagecode, Wikipedia, WikipedialanguagearticleEnglish, languageISO, languageISO3, languageISO5, nativeLabel, region, subregion, intermediateregion FROM wiki_projects;'

    languages = pd.read_sql_query(query, conn)
    languages = languages.set_index(['WikimediaLanguagecode'])


    return languages


def load_language_pairs_territory_status():

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();

    query = 'SELECT qitem,territoryname_english, territoryname_higher, ISO3166, ISO3166_2, language_lower_name, language_higher_name, wikimedia_lower, wikimedia_higher, type_overlap, status_lower, status_higher, equal_status, indigenous_lower, indigenous_higher FROM wikipedia_language_pairs_territory_status WHERE equal_status=0;'
    
    pairs = pd.read_sql_query(query, conn)
    pairs = pairs.set_index(['wikimedia_lower'])

    return pairs



def get_namespace_names(languagecode):
    read_dump = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-siteinfo-namespaces.json.gz'
    if check_dump(read_dump, 'wapa') != True: return {}

    try:
        dump_in = gzip.open(read_dump, 'r')
    except:
        return {}

    line = dump_in.readline()
    line = line.rstrip().decode('utf-8')#[:-1]
    entity = json.loads(line)['query']
    namespaces = entity['namespaces'].values()
    namespacealiases = entity['namespacealiases']

    namespaces_dict = {}
    # namespaces_alieses_dict = {}
    
    # (4, 12, 14, 100, -1, -2): # wikipedia, help, category, portal, special, media.

    # (4, 10, 12, 14, 100, 118, -1, -2): # wikipedia, template, help, category, portal, draft, special, media. 

    for value in namespaces:
        if value['id'] in (4, 10, 12, 14, 100, 102, 118, -1, -2): namespaces_dict[value['*']+':'] = value['id'] 

    for value in namespacealiases:
        if value['id'] in (4, 10, 12, 14, 100, 102, 118, -1, -2): namespaces_dict[value['*']+':'] = value['id']

    return namespaces_dict




def load_all_languages_information():
    print ('')
    # in this case, we obtain denomynm, language name, etc. for a language without a wikipedia.
    # This comes from Wikidata.

def load_all_territories_information():
    print ('')
    # in this case, we obtain territories for a language without a wikipedia.
    # This comes from Ethnologue and complemented.


def load_wikipedia_language_editions_numberofarticles(wikilanguagecode, db):
    wikipedialanguage_numberarticles = {}

    if db=='production': 
        database = wikipedia_diversity_db.split('.')[0]+'_production.db';
    else:
        database = wikipedia_diversity_db

    print ('database in use in load_wikipedia_language_editions_numberofarticles: '+database)

    conn = sqlite3.connect(databases_path + database); cursor = conn.cursor()

    count = 0
    # Obtaining CCC for all WP
    for languagecode in wikilanguagecode:
        try:
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki;'
            cursor.execute(query)
            number = cursor.fetchone()[0]
            count+= number
            wikipedialanguage_numberarticles[languagecode]=number
        except:
            print ('this language is not in the database yet: '+languagecode)

    print ('wikipedialanguage_numberarticles loaded.')
    print ('this is the total number of articles in the Diversity Database: '+str(count))
    return wikipedialanguage_numberarticles



def load_dicts_page_ids_qitems(printme, languagecode):
    page_titles_qitems = {}
    page_titles_page_ids = {}

    # conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()

    a='1'
    try:
        query = 'SELECT 1 FROM '+languagecode+'wiki_pages;'
        cursor.execute(query)
        a='0'
    except:
        print ('sqlite3.OperationalError: no such table: '+languagecode)
    if a=='1':
        return (page_titles_qitems, page_titles_page_ids)

    i=1
    while (i!=0):
        try:
            query = 'SELECT page_title, qitem, page_id FROM '+languagecode+'wiki_pages;'
#            query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
            for row in cursor.execute(query):
                page_title=row[0].replace(' ','_')
                page_titles_page_ids[page_title]=row[2]
                page_titles_qitems[page_title]=row[1]
            i = 0
        except:
            print('Database is lock. We try again.')
            time.sleep(120)


    if printme == 1:
        print ('language: '+languagecode)
        print ('page_ids loaded.')
        print ('qitems loaded.')
        print ('they are:')
        print (len(page_titles_qitems))

    return (page_titles_qitems, page_titles_page_ids)



def load_dicts_page_ids_qitems_namespace(printme, languagecode):

    page_ids_qitems = {}
    page_ids_page_titles = {}
    page_ids_namespaces = {}


    # conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()

    a='1'
    try:
        query = 'SELECT 1 FROM '+languagecode+'wiki_pages;'
        cursor.execute(query)
        a='0'
    except:
        print ('sqlite3.OperationalError: no such table: '+languagecode)
    if a=='1':
        return (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

    i=1
    while (i!=0):
        try:
            query = 'SELECT page_title, qitem, page_id, page_namespace FROM '+languagecode+'wiki_pages;'
#            query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
            for row in cursor.execute(query):
                page_title=row[0].replace(' ','_')
                qitem = row[1]
                page_id = row[2]
                page_namespace = row[3]

                page_ids_qitems[page_id] = qitem
                page_ids_page_titles[page_id] = page_title
                page_ids_namespaces[page_id] = page_namespace

            i = 0
        except:
            print('Database is lock. We try again.')
            time.sleep(120)


    if printme == 1:
        print ('language: '+languagecode)
        print ('page_ids loaded.')
        print ('qitems loaded.')
        print ('namespaces loaded.')
        print ('they are:')
        print (len(page_ids_page_titles))

    return (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)


def load_all_qitems(wikilanguagecodes):
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    all_qitems = []
    for languagecode in wikilanguagecodes:
        query = 'SELECT qitem FROM '+languagecode+'wiki_pages WHERE qitem IS NOT NULL;'
        dfx = pd.read_sql_query(query, conn).qitem.tolist()
        all_qitems += dfx
    all_qitems = list(set(all_qitems))
    
    return all_qitems


def obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, num):

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    query = ('CREATE TABLE IF NOT EXISTS obtain_closest_for_all_languages ('+
    'langcode text,'+
    'languages text,'+
    'PRIMARY KEY (langcode));')
    cursor.execute(query); conn.commit()

    query = 'SELECT * FROM obtain_closest_for_all_languages;'
    rows = cursor.execute(query);

    closest_langs = {}

    if rows == None:
        for languagecode in wikilanguagecodes:
            top, upperlower, closest= obtain_proximity_wikipedia_languages_lists(languagecode,wikipedialanguage_numberarticles, None, None, num)
            if len(closest)==0: continue
            row=''
            for x in range(0,num): row+=closest[x]+'\t'
            parameters.append((languagecode,row))
            query = 'INSERT OR IGNORE INTO obtain_closest_for_all_languages (langcode, languages) VALUES (?,?)'
            cursor.executemany(query,parameters)
            conn.commit()
            closest_langs[languagecode] = closest

    else:
        for row in rows:
            closest = row[1]
            closest = closest.split('\t')
            languagecode = row[0]
            closest_langs[languagecode] = closest

    return closest_langs



def get_langs_group(all_groups, topX, region, subregion, wikipedialanguage_numberarticles, territories, languages):
    if all_groups != None:
        if 'Top' in all_groups:
            topX = int(all_groups.split('Top ')[1])

        if all_groups in territories['subregion'].unique().tolist():
            subregion = all_groups

        if all_groups in territories['region'].unique().tolist():
            region = all_groups

    if all_groups == "All languages":
        topX = len(wikipedialanguage_numberarticles)

    langlist = []
    if topX != None:
        i = 0
        for w in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True):
            if i==topX: 
                break
            langlist.append(w)
            i+=1

    if region != None:
        langlist = list(set(territories.loc[territories['region']==region].index.tolist()))

    if subregion != None:
        langlist = list(set(territories.loc[territories['subregion']==subregion].index.tolist()))

    langlistnames = {}
    for languagecode in langlist:
        try:
            lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
            langlistnames[lang_name] = languagecode
        except:
            lang_name = ''
            

    return langlist, langlistnames



# It returns a list of languages based on the region preference introduced.
def obtain_region_wikipedia_language_list(languages, region, subregion, intermediateregion):
# use as: wikilanguagecodes = wikilanguages_utils.obtain_region_wikipedia_language_list('Asia', '', '').index.tolist()
#    print('* This is the list of continents combinations: '+' | '.join(languages.region.unique())+'\n')
#    print('* This is the list of subregions (in-continents) combinations: '+' | '.join(languages.subregion.unique())+'\n')
#    print('* This is the list of intermediate regions (in continents and regions) combinations: '+' | '.join(languages.intermediateregion.unique())+'\n')
#    print (languages)
    if region!='':
        languages_region = languages.loc[languages['region'] == region]

    if subregion!='':
        languages_region = languages[languages['subregion'].str.contains(subregion)]

    if intermediateregion!='':
        languages_region = languages[languages['intermediateregion'].str.contains(intermediateregion)]

    return languages_region



# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4') # utf8mb4
        return mysql_con_read
    except:
        print ('This language has no database or we cannot connect to it: '+languagecode)
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')

# Create a database connection.
def establish_mysql_connection_write():
    mysql_con_write = mdb.connect(host="tools.db.svc.eqiad.wmflabs",db='s53619__wcdo',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
    return mysql_con_write


# Additional sources: Pycountry and Babel libraries for countries and their languages.
def extract_check_new_wiki_projects():
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?itemLabel ?language ?languageLabel ?alias ?nativeLabel ?languageISO ?languageISO3 ?languageISO5 ?languagelink ?WikimediaLanguagecode WHERE {
      ?item wdt:P31 wd:Q10876391.
      ?item wdt:P407 ?language.
      
      OPTIONAL{?language wdt:P1705 ?nativeLabel.}
#     ?item wdt:P856 ?officialwebsite.
#     ?item wdt:P1800 ?bbddwp.
      ?item wdt:P424 ?WikimediaLanguagecode.

      OPTIONAL {?language skos:altLabel ?alias FILTER (LANG (?alias) = ?WikimediaLanguagecode).}
      OPTIONAL{?language wdt:P218 ?languageISO .}
      OPTIONAL{?language wdt:P220 ?languageISO3 .}
      OPTIONAL{?language wdt:P1798 ?languageISO5 .}
      
      OPTIONAL{
      ?languagelink schema:about ?language.
      ?languagelink schema:inLanguage "en". 
      ?languagelink schema:isPartOf <https://en.wikipedia.org/>
      }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?WikimediaLanguagecode'''

    
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
#    url = 'https://query.wikidata.org/sparql'

    data = requests.get(url,headers={'User-Agent': 'https://wikitech.wikimedia.org/wiki/User:Marcmiquel'}, params={'query': query, 'format': 'json'})
    print (data)
    data = data.json()

    #print (data)

    extracted_languages = []
    wikimedialanguagecode = ''
    Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];
    for item in data['results']['bindings']:    
#        print (item)
        #input('tell me')
        if wikimedialanguagecode != item['WikimediaLanguagecode']['value'] and wikimedialanguagecode!='':

            try:
                currentLanguagename=Locale.parse(wikimedialanguagecode).get_display_name(wikimedialanguagecode).lower()
                if currentLanguagename not in nativeLabel:
#                    print ('YAHOO')
                    nativeLabel.append(currentLanguagename)
#                    print(currentLanguagename)
            except:
                pass

            extracted_languages.append({
            'languagename': ";".join(languagename),          
            'Qitem': ";".join(Qitem),
            'WikimediaLanguagecode': wikimedialanguagecode.replace('-','_'),
            'Wikipedia':wikipedia,
            'WikipedialanguagearticleEnglish': englisharticle,
            'languageISO': ";".join(languageISO),
            'languageISO3': ";".join(languageISO3),
            'languageISO5': ";".join(languageISO5),
            'nativeLabel': ";".join(nativeLabel),
            })

            #print (extracted_languages)
            #input('common')
            Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];

        Qitemcurrent = item['language']['value'].replace("http://www.wikidata.org/entity/","")
        if Qitemcurrent not in Qitem:
            Qitem.append(Qitemcurrent)
        languagenamecurrent = item['languageLabel']['value']
        if languagenamecurrent not in languagename: languagename.append(languagenamecurrent)

        try:
            nativeLabelcurrent = item['nativeLabel']['value'].lower()
            if nativeLabelcurrent not in nativeLabel: nativeLabel.append(nativeLabelcurrent)
        except:
            pass

        try: 
            aliascurrent = item['alias']['value'].lower()
#            print (aliascurrent)
            if aliascurrent not in nativeLabel and len(aliascurrent)>3: nativeLabel.append(aliascurrent)
        except:
            pass

        try: 
            languageISOcurrent = item['languageISO']['value']
            if languageISOcurrent not in languageISO: languageISO.append(languageISOcurrent)
        except:
            pass

        try:
            languageISO3current = item['languageISO3']['value']
            if languageISO3current not in languageISO3: languageISO3.append(languageISO3current)
        except:
            pass

        try:
            languageISO5current = item['languageISO5']['value']
            if languageISO5current not in languageISO5: languageISO5.append(languageISO5current)
        except:
            pass

        try: englisharticle = item['languagelink']['value'] 
        except: englisharticle = 'no link'
        wikimedialanguagecode = item['WikimediaLanguagecode']['value'] # si 
        wikipedia = item['itemLabel']['value']

        #print (result)
    df = pd.DataFrame(extracted_languages)
    df = df.set_index(['languagename'])
 
#    df = df.set_index(['WikimediaLanguagecode'])
    filename= 'new_wikipedia_language_editions'
    newlanguages = []

    # CHECK IF THERE IS ANY NEW LANGUAGE
    languages = load_wiki_projects_information()
    langs_qitems = languages.Qitem.tolist()
    df_qitems = df.Qitem.tolist()
    for q in df_qitems:
        if q in langs_qitems:
            df.drop(df.loc[df['Qitem']==q].index, inplace=True)
    
    # exceptions
    languages=languages.rename(index={'be_x_old': 'be_tarask'})
    languages=languages.rename(index={'zh_min_nan': 'nan'})
    languageid_file = languages.index.tolist();
    languageid_file.append('nb')

    languageid_calculated = df['WikimediaLanguagecode'].tolist();
#    print ('These are the ones just extracted from Wikidata: ')
#    print (languageid_calculated)

    newlanguages = list(set(languageid_calculated) - set(languageid_file))

    exceptions = ['mo']
    indexs = []
    for x in newlanguages:
        if x in exceptions: continue
        indexs = indexs + df.index[(df['WikimediaLanguagecode'] == x)].tolist()
    newlanguages = indexs

    if len(newlanguages)>0: 
        message = 'These are the new languages: '+', '.join(newlanguages)
        print (message)
        df = df.loc[~df.index.duplicated(keep='first')]
        df=df.reindex(newlanguages)
        send_email_toolaccount('WDO: New languages to introduce into the file.', message)
        print ('The new languages are in a file named: ')
        print (databases_path + filename+'.csv')
        df.to_csv(databases_path + filename+'.csv',sep='\t')
    else:
        print ('There are no new Wikipedia language editions.')

    return newlanguages




def get_months_queries():

    def datespan(startDate, endDate, delta=datetime.timedelta(days=1)):
        currentDate = startDate
        while currentDate < endDate:
            yield currentDate
            currentDate += delta

    periods_accum = {}
    periods_monthly = {}

    current_cycle = get_current_cycle_year_month()
    endDay = datetime.datetime.strptime(str(current_cycle),'%Y-%m').date()+datetime.timedelta(days=30)

#    endDay = datetime.date.today()
    for day in datespan(datetime.date(2001, 1, 16), endDay, delta=datetime.timedelta(days=30)):
        month_period = day.strftime('%Y-%m')

        first_day = day.replace(day = 1).strftime('%Y%m%d%H%M%S')
        last_day = day.replace(day = calendar.monthrange(day.year, day.month)[1]).strftime('%Y%m%d%H%M%S')

#        print ('monthly:')
        month_condition = 'date_created >= "'+ first_day +'" AND date_created < "'+last_day+'"'
        periods_monthly[month_period]=month_condition
#        print (month_condition)    

#        print ('accumulated: ')
        if month_period == datetime.date.today().strftime('%Y-%m'):
            month_condition = 'date_created < '+last_day + ' OR date_created IS NULL'
        else:
            month_condition = 'date_created < '+last_day

        periods_accum[month_period]=month_condition
#        print (month_condition)

#    print (periods_monthly,periods_accum)
    return periods_monthly,periods_accum



def get_new_cycle_year_month():

    pathf = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
    current_cycle_date = time.strftime('%Y%m%d%H%M%S', time.gmtime(os.path.getmtime(pathf)))
    current_cycle_date = datetime.datetime.strptime(current_cycle_date,'%Y%m%d%H%M%S')-dateutil.relativedelta.relativedelta(months=1)
    current_cycle = current_cycle_date.strftime('%Y-%m')

    print ('new cycle for the data: '+current_cycle)
    return current_cycle


def get_current_cycle_year_month():

    query = 'SELECT MAX(year_month) FROM function_account WHERE script_name = "wikipedia_administrative_pages_analytics.py" AND function_name = "wd_dump_iterator";'
    conn = sqlite3.connect(databases_path + administrative_pages_analytics_log); cursor = conn.cursor()

    cursor.execute(query)
    current_cycle = cursor.fetchone()[0]

    print ('current cycle for the data: '+current_cycle)
    return current_cycle



def is_insert(line):
    return 'INSERT INTO' in line or False

def get_values(line):
    return line.partition(' VALUES ')[2]

def get_table_name(line):
    match = re.search('INSERT INTO `([0-9_a-zA-Z]+)`', line)
    if match:
        return match.group(1)
    else:
        print(line)

def get_columns(line):
    match = re.search('INSERT INTO `.*` \(([^\)]+)\)', line)
    if match:
        return list(map(lambda x: x.replace('`', '').strip(), match.group(1).split(',')))

def values_sanity_check(values):
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True

def parse_values(values):
    rows = []
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    latest_row = ['' if field == '\x00' else field for field in latest_row]

                    rows.append(latest_row)
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            latest_row = ['' if field == '\x00' else field for field in latest_row]
            rows.append(latest_row)

        return rows




def copy_db_for_production(dbname, scriptname, databases_path):
    function_name = 'copy_db_for_production'
    dbname_production = dbname.split('.')[0]+'_production.db'

    # if verify_function_run_db(function_name, 'check','')==1: return
    functionstartTime = time.time()
    try:
        shutil.copyfile(databases_path + dbname, databases_path + dbname_production)
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        print ('File '+dbname+' copied as '+dbname_production+' at the end of the '+scriptname+' script. It took: '+duration)
    except:
        print ('Not possible to create the production version.')
    # verify_function_run_db(function_name, 'mark', duration)



def delete_wikidata_db():
    os.remove(databases_path + wikidata_db)


############################################################################

# Sends an e-mail
def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

# Finish e-mail
def finish_email(startTime, filename, title):
    try:
        sys.stdout=None; send_email_toolaccount(title + ' completed successfuly', open(filename, 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount(title + ' aborted because of an error', open(filename, 'r').read()+'err')


def check_dump(dumps_path, script_name):
    try:
        os.path.isfile(dumps_path)
        return True
    except:
        print ('dump error at script '+script_name)
        return False
        # send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()



############################################################################


def backup_db():
    try:
        shutil.copyfile(databases_path + wikipedia_administrative_pages_analytics_db, databases_path + "wikipedia_administrative_pages_analytics_backup.db")
        print ('File wikipedia_administrative_pages_analytics.db copied as wikipedia_administrative_pages_analytics_backup.db at the end of the content_retrieval.py script.')
    except:
        print ('Not possible to create the backup.')






def verify_function_run(cycle_year_month, script_name, function_name, action, duration):
    function_name_string = function_name

    # print ('\n\n',cycle_year_month, script_name, function_name, action, duration); return # comment this.


    conn = sqlite3.connect(databases_path + administrative_pages_analytics_log); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS function_account (script_name text, function_name text, year_month text, finish_time text, duration text, PRIMARY KEY (script_name, function_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM function_account WHERE script_name = ? AND function_name = ? AND year_month = ?;'
        cursor.execute(query,(script_name, function_name, cycle_year_month))
        duration = cursor.fetchone()
        if duration != None:
            print ('= Process Accountant: The function "'+function_name_string+'" has already been run. It lasted: '+duration[0])
            return 1
        else:
            print ('- Process Accountant: The function "'+function_name_string+'" has not run yet. Do it! Now: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S")+'. Year Month Cycle: '+cycle_year_month))
            return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S");
        query = "INSERT INTO function_account (script_name, function_name, year_month, finish_time, duration) VALUES (?,?,?,?,?);"
        cursor.execute(query,(script_name, function_name, cycle_year_month, finish_time, duration))
        conn.commit()
        print ('+ Process Accountant. Function: '+function_name+' is NOW RUN! Script: '+script_name+'. After '+duration+'.\n')



def verify_script_run(cycle_year_month, script_name, action, duration):
    script_name_string = script_name

    conn = sqlite3.connect(databases_path + administrative_pages_analytics_log)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS script_account (script_name text, year_month text, finish_time text, duration text, PRIMARY KEY (script_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM script_account WHERE script_name = ? AND year_month = ?;'
        cursor.execute(query,(script_name, cycle_year_month))
        duration = cursor.fetchone()
        if duration != None:
            print ('= Process Accountant: The script "'+script_name_string+'" has already been run. It lasted: '+duration[0])
            return 1
        else:
            print ('- Process Accountant: The script "'+script_name_string+'" has not run yet. Do it! Now: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S")+'. Year Month Cycle: '+cycle_year_month))
            return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S");
        query = "INSERT INTO script_account (script_name, year_month, finish_time, duration) VALUES (?,?,?,?);"
        cursor.execute(query,(script_name, cycle_year_month, finish_time, duration))
        conn.commit()
        print ('+ Process Accountant. Script: '+script_name+' is NOW RUN! After '+duration+'.\n')





def store_lines_per_second(duration, lines, function_name, file, period):

    conn = sqlite3.connect(databases_path + administrative_pages_analytics_log)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS lines_per_second (linespersecond real, lines integer, duration integer, function_name text, file text, year_month text, PRIMARY KEY (function_name, year_month));")

    linespersecond = lines/duration

    query = "INSERT OR IGNORE INTO lines_per_second (linespersecond, lines, duration, function_name, file, year_month) VALUES (?,?,?,?,?,?);"
    cursor.execute(query,(linespersecond, lines, duration, function_name, file, period))
    conn.commit()

    print ('in function '+function_name+' reading the dump '+file+', the speed is '+str(linespersecond)+' lines/second, at this period: '+period)

