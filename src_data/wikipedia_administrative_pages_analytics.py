# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
from wikilanguages_utils import *
# time
import time
import datetime
from datetime import date, timedelta
from dateutil import relativedelta
import calendar
# system
import os
import sys
import shutil
import re
import random
import operator
from statistics import median
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import gzip
import zipfile
import bz2
import json
import csv
import codecs
# requests and others
import requests
import urllib
import webbrowser
import reverse_geocoder as rg
import numpy as np
from random import shuffle
# data
import pandas as pd
import gc


# MAIN
def main():


###################################################################### EL BO!
# canonical languages of the project: ['ca','en','it','ja','de','ru','sv','fr','es','ar','pl','pt','ro','ha','yo','ig','tw','hr','hy']
    
    # GET DATA

    # GET CATEGORIES
    for languagecode in wikilanguagecodes: # 
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode) # 'ca'
        extend_categories(languagecode,page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

    # ASSIGN ADMIN TYPES 
    for languagecode in wikilanguagecodes: # 
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode) # 'ca'
        store_admin_categories_local(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

    wikilanguagecodes_ordered_by_number_categories, language_admin_categories = retrieve_admin_categories_local() # first, we do those languages containing all the original admin. categories.

    print ('We start the category crawling for languages with original admin. categories.')
    for languagecode in wikilanguagecodes_ordered_by_number_categories:
        print (languagecode+' has: '+str(len(language_admin_categories[languagecode]))+' original admin. categories.')

        extend_admin_categories_existing_crawling(languagecode, language_admin_categories)

    return









### GENERAL SCRIPT ORDER

###################################################################### EL BO!


    # GET DATA
    wd_dump_iterator(); 
    print ('wikidata dump iterated');

    create_wikipedia_administrative_pages_analytics_db()
    insert_page_ids_page_titles_qitems_wikipedia_administrative_pages_analytics_db()
    extend_page_title_same_category_title()


    # GET WIKIDATA PROPERTIES
    for languagecode in wikilanguagecodes: # ['ca']:
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode) # 'ca'
        extend_instance_of(languagecode, page_ids_qitems)
        extend_interwiki_qitem_properties_identifiers_sister_projects(languagecode, page_ids_qitems)


######################################################################

    # GET CATEGORIES
    for languagecode in wikilanguagecodes: # 
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode) # 'ca'
        extend_categories(languagecode,page_ids_qitems, page_ids_page_titles, page_ids_namespaces)


    # ASSIGN ADMIN TYPES 
    for languagecode in wikilanguagecodes: # 
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode) # 'ca'
        store_admin_categories_local(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

    wikilanguagecodes_ordered_by_number_categories, language_admin_categories = retrieve_admin_categories_local() # first, we do those languages containing all the original admin. categories.

    print ('We start the category crawling for languages with original admin. categories.')
    for languagecode in wikilanguagecodes_ordered_by_number_categories:
        print (languagecode+' has: '+str(len(language_admin_categories[languagecode]))+' original admin. categories.')

        extend_admin_categories_existing_crawling(languagecode, language_admin_categories)



    wikilanguagecodes_minus = wikilanguagecodes
    for x in wikilanguagecodes_ordered_by_number_categories: wikilanguagecodes_minus.remove(x)
    print ('We start the interwiki approach for the languages with missing original admin. categories.')
    for languagecode in wikilanguagecodes_minus: # then, we do the rest of the languages.
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode)
        print (languagecode+' has: '+str(len(wikilanguagecodes_ordered_by_number_categories[languagecode]))+' original admin. categories.')

        extend_admin_categories_interwiki_approach(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)



######################################################################
    
    # GET FEATURES
    extend_page_title_same_category_title()
    create_stats_db()

    for languagecode in wikilanguagecodes: 
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode)
        extend_editing_history(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

        extend_external_links(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)
        extend_links(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces)

    extend_pageviews()
    extend_images()
    extend_first_timestamp_lang()









################################################################

# Creates a AP database for a list of languages.
def create_wikipedia_administrative_pages_analytics_db():

    function_name = 'create_wikipedia_administrative_pages_analytics_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    try:
        os.remove(databases_path + wikipedia_administrative_pages_analytics_db); print (wikipedia_administrative_pages_analytics_db+' deleted.');
    except:
        pass

    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()


    # Creates a table for each Wikipedia language edition.
    nonexistingwp = []
    for languagecode in wikilanguagecodes:

        # Checks whether the Wikipedia currently exists.
        try:
            wikilanguages_utils.establish_mysql_connection_read(languagecode)
        except:
            print ('Not created. The '+languages.loc[languagecode]['Wikipedia']+' with code '+languagecode+' is not active (closed or in incubator). Therefore, we do not create a table.')
            nonexistingwp.append(languagecode)
            continue

        # Create the PAGES table.
        query = ('CREATE TABLE '+languagecode+'wiki_pages ('+

        # general
        'qitem text, '+
        'page_id integer, '+
        'page_title text, '+
        'page_title_same_category_title text, '+ # when there is a category or page with the same title.

        'date_created integer, '+
        'first_timestamp_lang text,'+ # language of the oldest timestamp for the article
        'num_interwiki integer, '+



        #########

        #### ANNOTATION 1: NAMESPACES
        'page_namespace integer, '+


        #### ANNOTATION 2: CATEGORIES
        # characteristics of categorization
            # for categories only
        'num_categories_contains integer, '+ # the num. of category pages it contains.
        'num_pages_contains integer, '+ # the num. of pages it contains.
        'num_pages_admin_contains integer, '+ # the num. of pages (NS = 4, 12, 100) it contains.
        'num_level_from_top integer, '+ # the number of jumps from the top of the crawling (based on the the very top: "Main").


            # for all pages
        'num_categories_has integer, '+ # the number of categories it has.
        'actual_categories text, '+ # names separated by ;
        'main_category text, '+ # name of the largest category at that level, i.e., category containing more pages.

        # admin page types
        'Wikipedia_policies_and_guidelines_level integer, '+
        'Wikipedia_help_level integer, '+
        'Wikipedia_copyright_level integer, '+
        'WikiProjects_level integer, '+
        'Wikipedia_village_pump_level integer, '+
        'Wikipedia_essays_level integer, '+
        'Wikipedia_tools_level integer, '+
        'Wikipedia_disclaimers_level integer, '+
        'Wikipedia_deletion_level integer, '+
        'Wikipedia_maintenance_level integer, '+
        'Stubs_level integer, '+


        #### ANNOTATION 3: WIKIDATA
        # wikidata instance of
        'instance_of_Wikimedia_project_page integer, '+
        'instance_of_Wikimedia_internal_item integer, '+
        'instance_of_Wikimedia_project_policies_guidelines integer, '+
        'instance_of_Wikimedia_help_page integer, '+
        'instance_of_Wikimedia_wikiproject integer, '+
        'instance_of_Wikimedia_wikimedia_portal integer, '+

        ########

        # # # characteristics of relationships
        'num_inlinks_from_admin_pages integer, '+
        'num_outlinks_to_admin_pages integer, '+
        'percent_inlinks_from_admin_pages real, '+
        'percent_outlinks_to_admin_pages real, '+


        # characteristics of article relevance
        'num_bytes integer, '+
        'num_external_links integer, '+
        'num_images integer, '+

        'num_inlinks integer, '+
        'num_outlinks integer, '+
        'num_pageviews integer, '+


        # metrics of history
            # initial metrics
        'num_edits integer, '+
        'num_discussions integer, '+

        'num_anonymous_edits integer, '+
        'num_bot_edits integer, '+
        'num_reverts integer, '+

        'num_editors integer, '+
        'num_admin_editors integer, '+

        'median_year_first_edit integer, '+
        'median_editors_edits integer, '+


            # new last month metrics
        'num_edits_last_month integer, '+
        'num_edits_last_month_by_admin integer, '+
        'num_edits_last_month_by_anonymous integer, '+
        'num_edits_last_month_by_newcomer_90d integer, '+
        'num_edits_last_month_by_newcomer_1y integer, '+
        'num_edits_last_month_by_newcomer_5y integer, '+

            # regularity and engagement metrics
        'total_months integer, '+
        'active_months integer, '+

        'max_active_months_row integer, '+
        'max_inactive_months_row integer, '+

        'percent_active_months float, '+
        'editing_days integer, '+
        'percent_editing_days float,'+
        'days_last_50_edits integer,'+
        'days_last_5_edits integer,'+
        'days_last_edit integer, '+

        'date_last_edit integer, '+
        'date_last_discussion integer, '+

        # metrics of wikidata
        'sister_projects text, '+
        'num_multilingual_sisterprojects integer, '+
        'num_wdproperty integer, '+
        'num_wdidentifiers integer, '+

        'PRIMARY KEY (qitem,page_id));')


        cursor.execute(query)
        conn.commit()

        try:
            print ('Created the Wikipedia ADMINISTRATIVE PAGES ANALYTICS table for language: '+languagecode)
        except:
            print (languagecode+' already has a Wikipedia ADMINISTRATIVE PAGES ANALYTICS table.')



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def create_stats_db():

    function_name = 'create_stats_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    try:
        os.remove(databases_path + stats_db); print (stats_db+' deleted.');
    except:
        pass

    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

    functionstartTime = time.time()

    query = ('CREATE table if not exists wapa_cumulative ('+
    'content text not null, '+
    'editor text not null, '+

    'set1 text not null, '+
    'set1descriptor not null, '+

    'set2 not null, '+
    'set2descriptor not null, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('There is already Wikipedia Administrative Pages Stats table.')

    query = ('CREATE table if not exists wapa_incremental ('+
    'content text not null, '+
    'editor text not null, '+

    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('There is already Wikipedia Administrative Pages Stats table.')


    query = ('CREATE table if not exists wapa_stats ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+
    'statistic text,'+
    'value float,'+
    'period text,'+
    'PRIMARY KEY (content,set1,set1descriptor,statistic,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('already created.')


    query = ('CREATE table if not exists admin_categories ('+
    'languagecode text not null, '+
    'qitem text not null, '+
    'category_name text, '+
    'category_name_local text,'+
    'page_id integer,'+
    'run integer,'+
    'alternative_category integer,'+
    'PRIMARY KEY (languagecode, qitem, category_name))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('already created.')



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)









def create_wikidata_db():

    conn = sqlite3.connect(databases_path + wikidata_db)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS sitelinks (qitem text, langcode text, page_title text, sisterprojects text, PRIMARY KEY (qitem, langcode));")

    cursor.execute("CREATE TABLE IF NOT EXISTS labels (qitem text, langcode text, label text, PRIMARY KEY (qitem, langcode));")

    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (qitem text, properties integer, sitelinks integer, wd_identifiers integer, sisterprojects_sitelinks integer, PRIMARY KEY (qitem));")

    cursor.execute("CREATE TABLE IF NOT EXISTS instance_of_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    ###
    print ('Created the Wikidata sqlite3 file and tables.')
    return conn



def wd_dump_iterator():
    function_name = 'wd_dump_iterator'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    try: os.remove(databases_path + wikidata_db);
    except: pass;

    conn = create_wikidata_db(); cursor = conn.cursor()
    conn.commit()

    option = 'read'
    if option == 'download':
        print ('* Downloading the latest Wikidata dump.')
        url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz" # download the dump: https://dumps.wikimedia.org/wikidatawiki/entities/20180212/
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter
        r = requests.get(url, stream=True)
        with open(dumps_path + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=10240): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
        read_dump = databases_path + local_filename

    if option == 'copy':
        filename = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
        local_filename = url.split('/')[-1]
        shutil.copyfile(filename, dumps_path + local_filename)
        read_dump = databases_path + local_filename
        print ('Wikidata Dump copied.')


    if option == 'read': # 8 hours to process the 2% when read from the other server. sembla que hi ha un problema i és que llegir el dump és més lent que descarregar-lo.
        read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'

    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    # return

    wikilanguages_utils.check_dump(read_dump, script_name)

    dump_in = gzip.open(read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    n_qitems = 97763950
 
    sitelinks_values = [];
    labels_values = [];
    metadata_list = []; 
    instance_of_values = []

    print ('Iterating the dump.')
    while line != '':
        iter += 1
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]

        try:
            entity = json.loads(line)
            qitem = entity['id']
            if not qitem.startswith('Q'): continue

        except:
            print ('JSON error.')

        sitelinks = []
        wd_sitelinks = entity['sitelinks']
        if len(wd_sitelinks) == 0: continue

        if ':' in list(wd_sitelinks.values())[0]: continue # it means it is a category or any other type of page.

      
        lang_sisters = ''

        # SITELINKS
        for code, title in sorted(wd_sitelinks.items(), reverse=True):
            # print (code, title)
            # input('')
#            if code in wikilanguagecodes: 
            
            cd = code.split('wik')
            l = cd[0]
            p = 'wik'+cd[1]

            if p == 'wiki':
#            if code in wikilanguagecodeswiki: # wikipedia article
                sitelinks_values.append((qitem,code,title['title'],lang_sisters))
                sitelinks.append(code)
                lang_sisters = ''
            else:
                lang_sisters+= p+';'


        sisterprojects_sitelinks = len(wd_sitelinks) - len(sitelinks)

        # LABELS
        if len(sitelinks) != 0:           
            for code, title in entity['labels'].items(): # bucle de llengües
                code = code + 'wiki'
                if code not in wd_sitelinks and code in wikilanguagecodeswiki:
                    labels_values.append((qitem,code,title['value']))


        # PROPERTIES
        claims = entity['claims']
        identifiers = []

        # print (claims)
        # input('')
        # print ([qitem,len(claims),len(entity['sitelinks'])])
        # input('')


        # properties
        for wdproperty, claimlist in claims.items():
            try:
                if claimlist[0]['mainsnak']['datatype'] == 'external-id': identifiers.append(wdproperty)
            except:
                pass
            if wdproperty != 'P31': continue

            for snak in claimlist:
                mainsnak = snak['mainsnak']

                # the rest of properties
                try:
                    qitem2 = 'Q{}'.format(mainsnak['datavalue']['value']['numeric-id'])
                except:
                    continue

                if qitem2 in ['Q14204246','Q17442446','Q4656150','Q56005592','Q16695773','Q4663903']:
                    instance_of_values.append((qitem,wdproperty,qitem2))
                    continue

        # meta info
        metadata_list.append((qitem,len(claims),len(sitelinks)-1, len(identifiers), sisterprojects_sitelinks))
        # print ((qitem,len(claims),len(sitelinks)-1, len(identifiers), sisterprojects_sitelinks))
        # input('')


#        if iter % 850000 == 0:
        if iter % 850000 == 0:
            # insert
            cursor.executemany("INSERT INTO sitelinks (qitem, langcode, page_title, sisterprojects) VALUES (?,?,?,?)",sitelinks_values)

            cursor.executemany("INSERT INTO labels (qitem, langcode, label) VALUES (?,?,?)",labels_values)

            cursor.executemany("INSERT OR IGNORE INTO metadata (qitem, properties, sitelinks, wd_identifiers, sisterprojects_sitelinks) VALUES (?,?,?,?,?)", metadata_list)

            cursor.executemany("INSERT OR IGNORE INTO instance_of_properties (qitem, property, qitem2) VALUES (?,?,?)",instance_of_values)

            conn.commit()

            sitelinks_values = []
            labels_values = []
            metadata_list = []
            instance_of_values = []

            print (iter)
            print (100*iter/n_qitems)
            print ('current time: ' + str(time.time() - functionstartTime))
            print ('number of line per second: '+str(iter/(time.time() - functionstartTime)))
#            break


    # last round 
    # insert
    cursor.executemany("INSERT OR IGNORE INTO sitelinks (qitem, langcode, page_title, sisterprojects) VALUES (?,?,?,?)",sitelinks_values)  

    cursor.executemany("INSERT OR IGNORE INTO labels (qitem, langcode, label) VALUES (?,?,?)",labels_values)

    cursor.executemany("INSERT OR IGNORE INTO metadata (qitem, properties, sitelinks, wd_identifiers, sisterprojects_sitelinks) VALUES (?,?,?,?,?)", metadata_list)

    cursor.executemany("INSERT OR IGNORE INTO instance_of_properties (qitem, property, qitem2) VALUES (?,?,?)",instance_of_values)



    conn.commit()
    conn.close()

    print ('DONE with the JSON.')
    print ('It has this number of lines: '+str(iter))


    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    wikilanguages_utils.store_lines_per_second((time.time() - functionstartTime), iter, function_name, read_dump, cycle_year_month)




# Checks all the databses and updates the database.
def insert_page_ids_page_titles_qitems_wikipedia_administrative_pages_analytics_db():

    function_name = 'insert_page_ids_page_titles_qitems_wikipedia_administrative_pages_analytics_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()
    
    
    for languagecode in wikilanguagecodes:#['it']:#
        print (languagecode)

        # get all articles from wikidata / page_title_qitems_wd
        page_titles_qitems_wd={}
        query = 'SELECT page_title, qitem FROM sitelinks WHERE langcode = "'+languagecode+'wiki";'
        for row in cursor.execute(query):
            page_title=row[0].replace(' ','_')
            page_titles_qitems_wd[page_title]=row[1]
        print (len(page_titles_qitems_wd))
        print ('qitems obtained.')
        # IMPORTANT: not all pages from every Wikipedia have a Qitem related, as sometimes the link is not created. This is relevant for very small Wikipedias.

        namespaces_dict = wikilanguages_utils.get_namespace_names(languagecode)
        # print(namespaces_dict)


        # get all articles from dump
        # create parameters (page_id, page_title, qitem)
        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
        wikilanguages_utils.check_dump(dumps_path, script_name)

        dump_in = gzip.open(dumps_path, 'r')
        parameters = [];
        print ('Iterating the dump.')
        iter = 0
        while True:
            line = dump_in.readline()
            try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
            except UnicodeDecodeError: line = str(line)
            iter += 1

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                # table_name = wikilanguages_utils.get_table_name(line)
                # columns = wikilanguages_utils.get_columns(line)
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                for row in rows:

 #                   print (row)
                    try: page_namespace = int(row[1])
                    except: continue


                    # wikipedia namespace: https://en.wikipedia.org/wiki/Wikipedia:Namespace
                    if page_namespace in (4, 10, 12, 14, 100, 118, -1, -2): # wikipedia, template, help, category, portal, draft, special, media. 

                        page_id = int(row[0])                     
                        page_title = str(row[2]) #.decode('utf-8')
                        page_is_redirect = int(row[3])
                        if page_is_redirect == 1:
                            continue

                        page_len = int(row[9])


                        qitem = None
                        for name, ns in namespaces_dict.items():
                            if ns == page_namespace:
                                try:
                                    qitem = page_titles_qitems_wd[name+page_title]
                                except:
                                    pass

                            if qitem != None: break

                        if qitem == None:
                            try:
                                qitem = page_titles_qitems_wd[page_title]
                            except:
                                pass


                        parameters.append((page_namespace, page_len, page_title, page_id, qitem))

                            # input('')
  #                      input('')

            if iter % 10000 == 0:
                print (iter)
                print ('current time: ' + str(time.time() - functionstartTime))
                print ('number of lines per second: '+str(iter/(time.time() - functionstartTime)))

                    # page_ids.remove(page_id)
        print ('done with the dump.')

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_pages (page_namespace, num_bytes, page_title, page_id, qitem) VALUES (?,?,?,?,?);'
        cursor2.executemany(query,parameters)
        conn2.commit()
        

        print (len(parameters))
        print ('articles for this language are in and updated: '+languagecode+'\n')


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def extend_page_title_same_category_title():

    functionstartTime = time.time()
    function_name = 'extend_page_title_same_category_title'
    print (function_name)
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
 

    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode)

#        query = 'SELECT page_title, count(*) FROM cawiki_pages WHERE page_namespace IN (4, 14) GROUP BY page_title HAVING count(*) > 1 ORDER BY count(*) DESC;'
 #       query_update = 'UPDATE cawiki_pages SET page_title_same_category_title = 1 WHERE page_title IN (SELECT page_title FROM cawiki_pages WHERE page_namespace IN (4, 14) GROUP BY page_title HAVING count(*) > 1);' # https://www.codegrepper.com/code-examples/sql/update+from+select+sqlite

        query_update = 'UPDATE '+languagecode+'wiki_pages SET page_title_same_category_title = 1 WHERE page_title IN (SELECT page_title FROM '+languagecode+'wiki_pages WHERE page_namespace IN (4, 14, 100) GROUP BY page_title HAVING count(*) > 1) and page_namespace IN (4, 14, 100);' # 

        cursor2.execute(query_update)
    conn2.commit()
    print ('done.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def extend_instance_of(languagecode, page_ids_qitems):

    functionstartTime = time.time()
    function_name = 'extend_instance_of '+languagecode
    print (function_name)
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

    qitems_page_ids = {v: k for k, v in page_ids_qitems.items()}

    updated = []
    query = "SELECT ip.qitem, ip.qitem2, sl.page_title FROM instance_of_properties ip INNER JOIN sitelinks sl ON sl.qitem = ip.qitem WHERE sl.langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):
        instance_of_Wikimedia_project_page = None
        instance_of_Wikimedia_internal_item = None
        instance_of_Wikimedia_project_policies_guidelines = None
        instance_of_Wikimedia_help_page = None
        instance_of_Wikimedia_wikiproject = None
        instance_of_Wikimedia_wikimedia_portal = None

        try:
            qitem=row[0]
            qitem2=row[1]
            page_title = row[2].replace(' ','_')
            page_id = qitems_page_ids[qitem]

            if qitem2 == 'Q14204246':
                instance_of_Wikimedia_project_page = 1
            elif qitem2 == 'Q17442446':
                instance_of_Wikimedia_internal_item = 1
            elif qitem2 == 'Q4656150':
                instance_of_Wikimedia_project_policies_guidelines = 1
            elif qitem2 == 'Q56005592':
                instance_of_Wikimedia_help_page = 1
            elif qitem2 == 'Q16695773':
                instance_of_Wikimedia_wikiproject = 1
            elif qitem2 == 'Q4663903':
                instance_of_Wikimedia_wikimedia_portal = 1
            else:
                print ('We have a problem!')

            updated.append((instance_of_Wikimedia_project_page,instance_of_Wikimedia_internal_item, instance_of_Wikimedia_project_policies_guidelines, instance_of_Wikimedia_help_page, instance_of_Wikimedia_wikiproject, instance_of_Wikimedia_wikimedia_portal, page_id, qitem))
            # print((page_title,instance_of_Wikimedia_project_page,instance_of_Wikimedia_internal_item, instance_of_Wikimedia_project_policies_guidelines, instance_of_Wikimedia_help_page, instance_of_Wikimedia_wikiproject, instance_of_Wikimedia_wikimedia_portal, page_id, qitem))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki_pages SET instance_of_Wikimedia_project_page = ?, instance_of_Wikimedia_internal_item = ?, instance_of_Wikimedia_project_policies_guidelines = ?, instance_of_Wikimedia_help_page = ?, instance_of_Wikimedia_wikiproject = ?, instance_of_Wikimedia_wikimedia_portal = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





# Extends the Articles table with the number of interwiki links.
def extend_interwiki_qitem_properties_identifiers_sister_projects(languagecode, page_ids_qitems):

    functionstartTime = time.time()
    function_name = 'extend_interwiki_qitem_properties_identifiers_sister_projects '+languagecode
    print (function_name)
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

    qitems_page_ids = {v: k for k, v in page_ids_qitems.items()}

    updated = []
    query = "SELECT metadata.qitem, metadata.sitelinks, metadata.properties, metadata.wd_identifiers, metadata.sisterprojects_sitelinks, sitelinks.page_title, sitelinks.sisterprojects FROM metadata INNER JOIN sitelinks ON sitelinks.qitem = metadata.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):
    

        try:
            qitem=row[0]
            iw_count=row[1]
            num_wdproperties=row[2]
            wd_identifiers = row[3]
            sisterprojects_sitelinks = row[4]
            page_title = row[5].replace(' ','_')
            page_id=qitems_page_ids[qitem]
            sisterprojects = row[6]

            updated.append((iw_count,num_wdproperties,wd_identifiers,sisterprojects_sitelinks, sisterprojects, page_id, qitem))
            # print ((iw_count,num_wdproperties,wd_identifiers,sisterprojects_sitelinks, sisterprojects, page_title, page_id,qitem))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki_pages SET num_interwiki = ?, num_wdproperty = ?, num_wdidentifiers = ?, num_multilingual_sisterprojects = ?, sister_projects = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def search_highest_largest_category_from_list(languagecode, list_potential_base_categories, cat_titles_page_ids):

    conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()


    categories_levels = {}
    categories_below = {}

    for page_title in list_potential_base_categories:
        print (page_title)

        total_categories = set()
        query = 'SELECT subcategory_title FROM category_links_cat_cat WHERE category_title = "'+page_title+'"'
        df = pd.read_sql_query(query, conn).set_index('subcategory_title')
        categories_collected = set(df.index.tolist())

        total_categories.add(page_title)
        total_categories.update(categories_collected)

        levels = 1
        increment = 1000
 
        while 1:

            print ('* current level: '+str(levels))
            i = 0
            j = increment
            categories_collected_iterate = list(categories_collected)
            while len(categories_collected_iterate[i:j]) > 0:

                page_asstring = ','.join( ['?'] * len(categories_collected_iterate[i:j]) )
                query = 'SELECT DISTINCT clcc.subcategory_title FROM category_links_cat_cat clcc INNER JOIN categories c ON clcc.subcategory_title = c.category_title WHERE c.hidden != 1 AND clcc.category_title IN (%s) ' % page_asstring
                df = pd.read_sql_query(query, conn, params = categories_collected_iterate[i:j]).set_index('subcategory_title')

                categories_collected.update(set(df.index.tolist()))
                i += increment
                j += increment

                # print (i,j)
                # print ('mec')

            print (len(categories_collected))
            categories_collected = categories_collected - total_categories
            print (len(categories_collected))
            total_categories.update(categories_collected)
            print (len(total_categories))

            levels = levels + 1
            if len(categories_collected) == 0: break

        categories_levels[page_title] = levels
        categories_below[page_title] = len(total_categories)
        print ('---')
        print (len(total_categories))
        print ('--- done ---')

    # print (categories_levels)
    # print ('ja')
    # print (categories_below)
    # print ('ja')

    max_category_level = max(categories_levels, key=categories_levels.get)
    max_category_below = max(categories_below, key=categories_below.get)
    print (max_category_level, max_category_below) # max_category_below is the one we will use.

    page_id = str(cat_titles_page_ids[max_category_below])


    return cat_titles_page_ids, page_id, max_category_below



def extend_categories(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):

    functionstartTime = time.time()
    function_name = 'extend_categories '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    print (function_name)

    # WE CREATE THE DATABASE 
    conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()




    query = ('CREATE TABLE IF NOT EXISTS categories (category_title text, page_id integer, hidden integer, cat_pages integer, cat_subcats integer, PRIMARY KEY (category_title));')
    cursor.execute(query); conn.commit()
    query = ('CREATE TABLE IF NOT EXISTS category_links_cat_art (category_title text, page_id integer, PRIMARY KEY (category_title, page_id));')
    cursor.execute(query); conn.commit()
    query = ('CREATE TABLE IF NOT EXISTS category_links_cat_cat (category_title text, subcategory_title text, PRIMARY KEY (category_title, subcategory_title));')
    cursor.execute(query); conn.commit()


    # FIRST, LET'S GET ALL THE CATEGORIES.
    # PAGE DUMP
    category_page_ids_page_titles = {}
    category_page_titles_page_ids = {}

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)
    
    dump_in = gzip.open(dumps_path, 'r')
    iter = 0
    while True:
        iter+=1
        line = dump_in.readline()
        try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                page_id = int(row[0])
                page_namespace = int(row[1])
                cat_title = str(row[2])

                # if "Categories_amb_enllaç_commonscat" in cat_title:
                #     print (cat_title)
                #     input('')

                if page_namespace != 14: continue
                category_page_ids_page_titles[page_id]=cat_title
                category_page_titles_page_ids[cat_title]=page_id

    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (len(category_page_titles_page_ids))
    print ('page dump categories loaded')



    # CATEGORY DUMP
    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-category.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)
    

    category_page_titles_cat_pages = {}
    category_page_titles_cat_subcats = {}

    dump_in = gzip.open(dumps_path, 'r')
    iter = 0
    while True:
        iter+=1
        line = dump_in.readline()
        try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                category_title = str(row[1])
                cat_pages = int(row[2])
                cat_subcats = int(row[3])
                category_page_titles_cat_pages[category_title] = cat_pages
                category_page_titles_cat_subcats[category_title] = cat_subcats
                if category_title not in category_page_titles_page_ids:
                    category_page_titles_page_ids[category_title] = None

    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (len(category_page_titles_page_ids))
    print ('category dump categories loaded')



    # HERE WE TAKE THE HIDDEN ONES
    hidden_categories = {}
    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page_props.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)
    
    dump_in = gzip.open(dumps_path, 'r')
    iter = 0
    while True:
        iter+=1
        line = dump_in.readline()
        try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)
            for row in rows:
                page_id = int(row[0])
                page_prop = str(row[1])

                if page_prop == 'hiddencat':
                    hidden_categories[page_id] = page_prop


    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (len(hidden_categories))
    print ('hidden categories dump loaded')


    # SECOND: We introduce them into the database.
    categories_contains = []
    categories_contains_q = []

    categories_list = []
    iter = 0
    for category_title, page_id in category_page_titles_page_ids.items():
        iter = iter + 1

        try:
            hidden_categories[page_id]
            hidden = 1
        except:
            hidden = 0

        try:
            cat_subcats = category_page_titles_cat_subcats[category_title]
        except:
            cat_subcats = 0


        try:
            cat_pages = category_page_titles_cat_pages[category_title]
        except:
            cat_pages = 0

        categories_list.append((category_title, page_id, hidden, cat_pages, cat_subcats))


        try:
            qt = page_ids_qitems[page_id]
        except:
            qt = None

        if qt == None:
            categories_contains_q.append((cat_subcats, cat_pages, page_id, qt))
        else:
            categories_contains.append((cat_subcats, cat_pages, page_id))
        # print ((category_title, page_id, hidden, cat_pages, cat_subcats))


        if iter % 50000 == 0:
            print (iter)

    cursor.executemany('INSERT OR IGNORE INTO categories (category_title, page_id, hidden, cat_pages, cat_subcats) VALUES (?,?,?,?,?)', categories_list);
    conn.commit()
    print (len(categories_list))
    categories_list = []
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print ('categories stored.')


    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_contains = ?, num_pages_contains = ? WHERE page_namespace = 14 AND page_id = ? AND qitem = ?;'
    cursor2.executemany(query,categories_contains_q); conn2.commit()
    conn2.commit()

    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_contains = ?, num_pages_contains = ? WHERE page_namespace = 14 AND page_id = ? AND qitem IS NULL;'
    cursor2.executemany(query,categories_contains); conn2.commit()
    conn2.commit()




    # THIRD:
    # Category links. Muntar estructura de category links amb diccionaris i sets. Un diccionari amb les relacions entre cat-page i un altre entre cat-cat.
    # https://www.mediawiki.org/wiki/Manual:Categorylinks_table
    category_links_cat_cat = []
    category_links_cat_art = []

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-categorylinks.sql.gz'
    print ('reading and storing categorylinks: ')
    print (dumps_path)
    wikilanguages_utils.check_dump(dumps_path, script_name)
    dump_in = gzip.open(dumps_path, 'r')

    iter = 0
    while True:
        iter+=1
        line = dump_in.readline()
        try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:

                try:
                    page_id = int(row[0])
                    cat_title = str(row[1].strip("'"))
                except:
                    continue

                if cat_title not in category_page_titles_page_ids:
                    continue

                if page_id in category_page_ids_page_titles: # is this a category
                    category_links_cat_cat.append((cat_title, category_page_ids_page_titles[page_id]))
                else: # this is an article
                    category_links_cat_art.append((cat_title, page_id))


        if iter % 5000 == 0:
            print (str(iter)+' categorylinks lines read.')

            cursor.executemany('INSERT OR IGNORE INTO category_links_cat_cat (category_title, subcategory_title) VALUES (?,?)', category_links_cat_cat);
            conn.commit()
            cursor.executemany('INSERT OR IGNORE INTO category_links_cat_art (category_title, page_id) VALUES (?,?)', category_links_cat_art);
            conn.commit()

            category_links_cat_cat = []
            category_links_cat_art = []

    # The last iteration
    cursor.executemany('INSERT OR IGNORE INTO category_links_cat_cat (category_title, subcategory_title) VALUES (?,?)', category_links_cat_cat);
    conn.commit()
    cursor.executemany('INSERT OR IGNORE INTO category_links_cat_art (category_title, page_id) VALUES (?,?)', category_links_cat_art);
    conn.commit()

    category_links_cat_cat = []
    category_links_cat_art = []


    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print ('all category links loaded and stored.')





    # FOURTH:
    # FINDING THE TOP CATEGORY AND ASSIGNING JUMPS OR LEVELS
    # conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
    # conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()
    no_top_category = 0
    cat_titles_page_ids = {}
    cat_titles_qitems = {}
    query = 'SELECT page_title, page_id, qitem FROM '+languagecode+'wiki_pages WHERE page_namespace = 14;'
    for row in cursor2.execute(query):
        cat_title=row[0].replace(' ','_')
        p_id = row[1]
        cat_titles_page_ids[cat_title] = p_id
        cat_titles_qitems[cat_title] = row[2]

    cat_page_ids_titles = {}
    query = 'SELECT page_id, category_title FROM categories;'
    for row in cursor.execute(query):
        cat_page_ids_titles[row[0]] = row[1]
    print (len(cat_page_ids_titles))


    cursor2.execute('SELECT page_id FROM '+languagecode+'wiki_pages WHERE Qitem = "Q1281";')
    try:
        page_id = cursor2.fetchone()[0]
    except:
        page_id = None

    if page_id is None:

        print ('it has no Contents category, so we look for it.')

        query = 'SELECT DISTINCT clcc.category_title FROM category_links_cat_cat clcc INNER JOIN categories c ON clcc.category_title = c.category_title WHERE hidden != 1 AND page_id IS NOT NULL AND clcc.category_title NOT IN (SELECT DISTINCT subcategory_title FROM category_links_cat_cat clcc INNER JOIN categories c ON clcc.category_title = c.category_title WHERE hidden!= 1);'
        df = pd.read_sql_query(query, conn).set_index('category_title')
        list_potential_base_categories = df.index.tolist()

        print (list_potential_base_categories)
    #    list_potential_base_categories = ['Principal']

        if len(list_potential_base_categories) == 0: 
            no_top_category = 1
        else:
            cat_titles_page_ids, page_id, page_title = search_highest_largest_category_from_list(languagecode, list_potential_base_categories, cat_titles_page_ids)

            print ('the largest category among the "highest" is: '+page_title)

    else:
        page_title = cat_page_ids_titles[page_id]


    if no_top_category == 0:

        # STARTING WITH THE CATEGORY CRAWLING AND ASSIGNING LEVELS
        # conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
        # conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

        # assigning LEVEL 0
        query = 'UPDATE '+languagecode+'wiki_pages SET num_level_from_top = 0 WHERE page_namespace = 14 AND page_id = '+str(page_id)
        cursor2.execute(query); conn2.commit()


        total_categories = set()
        query = 'SELECT subcategory_title FROM category_links_cat_cat WHERE category_title = "'+page_title+'"'
        df = pd.read_sql_query(query, conn).set_index('subcategory_title')
        categories_collected = set(df.index.tolist())

        # LEVEL 1
        parameters = []
        parameters_qitems = []

        for x in categories_collected:
            try:
                qitem = cat_titles_qitems[x]
                if qitem == None:
                    parameters.append((cat_titles_page_ids[x],qitem))
                else:
                    parameters_qitems.append((cat_titles_page_ids[x],))
            except:
                pass
        query = 'UPDATE '+languagecode+'wiki_pages SET num_level_from_top = 1 WHERE page_namespace = 14 AND page_id = ? AND qitem = ?'
        cursor2.executemany(query,parameters)
        conn2.commit()
        query = 'UPDATE '+languagecode+'wiki_pages SET num_level_from_top = 1 WHERE page_namespace = 14 AND page_id = ? AND qitem IS NULL;'
        cursor2.executemany(query,parameters_qitems)
        conn2.commit()


        total_categories.add(page_title) # ADD THE LEVEL ZERO
        total_categories.update(categories_collected) # ADD THE LEVEL ONE
        print ('* current level: 1')
        print (len(total_categories))

        levels = 2
        increment = 1000
        parameters = []

        while 1:

            print ('* current level: '+str(levels))
            i = 0
            j = increment
            categories_collected_iterate = list(categories_collected)
            while len(categories_collected_iterate[i:j]) > 0:

                page_asstring = ','.join( ['?'] * len(categories_collected_iterate[i:j]) )
                query = 'SELECT DISTINCT clcc.subcategory_title FROM category_links_cat_cat clcc INNER JOIN categories c ON clcc.subcategory_title = c.category_title WHERE c.hidden != 1 AND clcc.category_title IN (%s) ' % page_asstring
                df = pd.read_sql_query(query, conn, params = categories_collected_iterate[i:j]).set_index('subcategory_title')

                categories_collected.update(set(df.index.tolist()))
                i += increment
                j += increment


            print (len(categories_collected))
            categories_collected = categories_collected - total_categories
            print (len(categories_collected))
            total_categories.update(categories_collected)
            print (len(total_categories))



            parameters = []
            parameters_qitems = []
            for x in categories_collected:
                try:
                    qitem = cat_titles_qitems[x]
                    if qitem == None:
                        parameters.append((cat_titles_page_ids[x],))                    
                    else:
                        parameters_qitems.append((cat_titles_page_ids[x],qitem))
                except:
                    pass

           # page_asstring = ','.join( ['?'] * len(parameters) )
            query = 'UPDATE '+languagecode+'wiki_pages SET num_level_from_top = "'+str(levels)+'" WHERE page_namespace = 14 AND page_id = ? AND qitem = ?;'
            cursor2.executemany(query,parameters_qitems)
            query = 'UPDATE '+languagecode+'wiki_pages SET num_level_from_top = "'+str(levels)+'" WHERE page_namespace = 14 AND page_id = ? AND qitem IS NULL;'
            cursor2.executemany(query,parameters)


            levels = levels + 1
            if len(categories_collected) == 0: break
        conn2.commit()

        print ('---')
        print (len(total_categories))
        print ('--- done with levels---')
        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))




    # FIFTH:
    # STORING BASIC STATS FOR CATEGORIES
    # CATEGORIES OF ARTICLES
    # conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
    # conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()
    print ('storing basic stats for categories')
    print ('categories of pages')

    parameters_cat_basic_stats = []
    query = 'SELECT clca.page_id, clca.category_title, c.cat_pages, c.cat_subcats FROM category_links_cat_art clca INNER JOIN categories c ON clca.category_title = c.category_title WHERE c.hidden != 1 ORDER BY clca.page_id;'

    cur_page = ''
    old_page = ''
    params = []
    params_q = []

    cat_subcats = {}
    cat_pages = {}
    iter = 0
    for row in cursor.execute(query):

        cur_page = row[0]
        cur_category = row[1]

        if old_page != cur_page and old_page != '':

            actual_categories = ''
            main_category = max(cat_pages, key=cat_pages.get)
            for i, j in cat_pages.items():
                actual_categories+= i+';'
            actual_categories = actual_categories[:len(actual_categories)-1]

            try:
                qitem = page_ids_qitems[old_page]
                if qitem == None:
                    params.append((len(cat_subcats),actual_categories,main_category,old_page,qitem))
                else:
                    params_q.append((len(cat_subcats),actual_categories,main_category,old_page))

                # print ((len(cat_subcats),actual_categories,main_category,old_page,qitem))
            except:
                pass

            # print (params, iter)
            # print (cat_subcats, cat_pages)

            cat_subcats = {}
            cat_pages = {}
            iter += 1
            # input('')

        cat_subcats[cur_category] = row[2]
        cat_pages[cur_category] = row[3]

        old_page = cur_page

        if iter % 100000 == 0:
            query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem = ?;'
            cursor2.executemany(query,params); conn2.commit()
            query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem IS NULL;'
            cursor2.executemany(query,params_q); conn2.commit()

            # print (len(params))
            params = []
            params_q = []


    # last iteration
    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,params); conn2.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor2.executemany(query,params_q); conn2.commit()

    # print (len(params))
    params = []
    params_q = []
    print ('categories of articles stats done.')



    # CATEGORIES OF CATEGORIES
    print ('categories of categories')
    parameters_cat_basic_stats = []
    query = 'SELECT c2.page_id, clcc.category_title, c.cat_pages, c.cat_subcats FROM category_links_cat_cat clcc INNER JOIN categories c ON c.category_title = clcc.category_title INNER JOIN categories c2 ON c2.category_title = clcc.subcategory_title WHERE c.hidden != 1 ORDER BY clcc.subcategory_title;'

    cur_page = ''
    old_page = ''
    params = []
    params_q = []

    cat_subcats = {}
    cat_pages = {}
    iter = 0
    for row in cursor.execute(query):


        cur_page = row[0]
        cur_category = row[1]

        if old_page != cur_page and old_page != '':

            actual_categories = ''
            main_category = max(cat_pages, key=cat_pages.get)
            for i, j in cat_pages.items():
                actual_categories+= i+';'
            actual_categories = actual_categories[:len(actual_categories)-1]

            try:
                qitem = page_ids_qitems[old_page]
                if qitem == None:
                    params.append((len(cat_subcats),actual_categories,main_category,old_page,qitem))
                else:
                    params_q.append((len(cat_subcats),actual_categories,main_category,old_page))
            except:
                pass

            cat_subcats = {}
            cat_pages = {}
            iter += 1
        cat_subcats[cur_category] = row[2]
        cat_pages[cur_category] = row[3]

        old_page = cur_page

        if iter % 100000 == 0:
            query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem = ?;'
            cursor2.executemany(query,params); conn2.commit()
            conn2.commit()
            query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem IS NULL;'
            cursor2.executemany(query,params_q); conn2.commit()


            # print (len(params))
            params = []
            params_q = []

        iter += 1

    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,params); conn2.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_categories_has = ?, actual_categories = ?, main_category = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor2.executemany(query,params_q); conn2.commit()
    # print (len(params))

    params = []
    print ('categories of categories stats done.')
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))



    # SIXTH:
    # FINDING ADMIN PAGES FOR EVERY CATEGORY
    print ('admin pages of categories')
    increment = 1000
    i = 0
    j = increment


    page_ids_help_wikipedia = []
    query = 'SELECT page_id FROM '+languagecode+'wiki_pages WHERE page_namespace IN (4, 12);'
    for row in cursor2.execute(query):
        page_ids_help_wikipedia.append(row[0])
    page_ids_iterate = page_ids_help_wikipedia

    params = []
    params_q = []

    cats_admin_pages_count = {}
    while len(page_ids_iterate[i:j]) > 0:
        print (i,j)
        # print ('*')
        # print (len(page_ids_iterate[i:j]))

        page_asstring = ','.join( ['?'] * len(page_ids_iterate[i:j]) )
        query = 'SELECT clca.category_title, count(clca.page_id) FROM category_links_cat_art clca WHERE clca.page_id IN (%s) GROUP BY clca.category_title;' % page_asstring
        # query = 'SELECT clca.category_title, clca.page_id FROM category_links_cat_art clca INNER JOIN categories c ON clca.category_title = c.category_title WHERE clca.page_id IN (%s);' % page_asstring

        for row in cursor.execute(query, page_ids_iterate[i:j]):
            cur_page_title = row[0]
            cur_count = row[1]

            try:
                cats_admin_pages_count[cur_page_title] = cats_admin_pages_count[cur_page_title] + cur_count
            except:
                cats_admin_pages_count[cur_page_title] = cur_count

        i += increment
        j += increment

    print ('iterated.') # 2632
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))


    for cur_page_title, cur_count in cats_admin_pages_count.items():
        try:
            qitem = cat_titles_qitems[cur_page_title]
            if qitem == None:
                params.append((cur_count, cat_titles_page_ids[cur_page_title], qitem))
            else:
                params_q.append((cur_count, cat_titles_page_ids[cur_page_title]))
        except:
            pass
            # print ((cur_page_title, cur_count))


    query = 'UPDATE '+languagecode+'wiki_pages SET num_pages_admin_contains = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,params); conn2.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_pages_admin_contains = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor2.executemany(query,params_q); conn2.commit()


    print (str(len(params)+len(params_q))+ ' introduced.')
    params = []
    params_q = []
    conn2.commit()

    print ('num_pages_admin_contains done.')


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)






def store_admin_categories_local(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):


    functionstartTime = time.time()
    function_name = 'store_admin_categories_local '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    print(function_name)


    # CREATE table if not exists admin_categories (languagecode text not null, qitem text not null, category_name text, category_name_local text,page_id integer,run integer,PRIMARY KEY (languagecode, qitem, category_name));

    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

    qitems = []
    params = []
    page_asstring = ','.join( ['?'] * len(admin_categories) )
    query = 'SELECT qitem, page_title, page_id FROM '+languagecode+'wiki_pages WHERE qitem IN (%s);' % page_asstring
    for row in cursor2.execute(query,(list(admin_categories.keys()))):
        qitem = row[0]
        local_name = row[1]
        page_id = row[2]
        qitems.append(qitem)
        params.append((languagecode, qitem, admin_categories[qitem], local_name, page_id, 0))

    for qitem in admin_categories.keys():
        if qitem not in qitems:
            params.append((languagecode, qitem, admin_categories[qitem], None, None, 1))

    cursor.executemany('INSERT OR IGNORE INTO admin_categories (languagecode, qitem, category_name, category_name_local, page_id, alternative_category) VALUES (?,?,?,?,?,?)', params);
    conn.commit()

    # select count(*), languagecode from admin_categories WHERE page_id is NOT NULL GROUP BY languagecode order by 1 DESC;
    # select count(*), category_name, qitem from admin_categories WHERE page_id is NOT NULL GROUP BY category_name order by 1 DESC;


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def retrieve_admin_categories_local():

    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

    wikilanguagecodes_ordered_by_number_categories = []
    query = 'SELECT languagecode, COUNT(*) FROM admin_categories WHERE page_id is NOT NULL GROUP BY languagecode ORDER BY 2 DESC;'
    
    for row in cursor.execute(query):
        wikilanguagecodes_ordered_by_number_categories.append(row[0])



    language_admin_categories = {}
    query = 'SELECT languagecode, qitem, category_name, page_id FROM admin_categories WHERE page_id IS NOT NULL ORDER BY languagecode;'

    cur_language = ''
    old_language = ''

    language_admin_categories = {}
    qitem_category_name_page_id = {}
    for row in cursor.execute(query):
        cur_language = row[0]

        if cur_language != old_language and old_language != '':
            language_admin_categories[old_language] = qitem_category_name_page_id
            qitem_category_name_page_id = {}

        qitem_category_name_page_id[row[1]] = [row[2],row[3]]
        old_language = cur_language


    language_admin_categories[old_language] = qitem_category_name_page_id

    for languagecode in wikilanguagecodes:
        try:
            language_admin_categories[languagecode]
        except:
            language_admin_categories[languagecode]=[]


    return wikilanguagecodes_ordered_by_number_categories, language_admin_categories



def admin_category_category_crawling(languagecode, admin_category_title, admin_category_page_id, admin_qitem):



    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()
    conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
    conn3 = sqlite3.connect(databases_path + stats_db); cursor3 = conn3.cursor()

    # edfile2 = open(databases_path+languagecode+'_'+admin_category_title+'_pages.txt', "w")

    total_pages_count = 0
    total_categories_count = 0


    # PREPARING THE DICTS WE NEED LATER
    cat_titles_page_ids = {}
    cat_titles_qitems = {}
    query = 'SELECT page_title, page_id, qitem FROM '+languagecode+'wiki_pages WHERE page_namespace = 14;'
    for row in cursor2.execute(query):
        cat_title=row[0].replace(' ','_')
        p_id = row[1]
        cat_titles_page_ids[cat_title] = p_id
        cat_titles_qitems[cat_title] = row[2]


    page_ids_qitems = {}
    page_ids_page_titles = {}
    page_ids_namespaces = {}

    page_ids_page_titles_admin_pages = {}
    query = 'SELECT page_title, qitem, page_id, page_namespace FROM '+languagecode+'wiki_pages;'
#            query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
    for row in cursor2.execute(query):
        page_title=row[0].replace(' ','_')
        qitem = row[1]
        page_id = row[2]
        page_namespace = row[3]

        page_ids_qitems[page_id] = qitem
        page_ids_page_titles[page_id] = page_title
        page_ids_namespaces[page_id] = page_namespace
        if page_namespace in (4, 12):
            page_ids_page_titles_admin_pages[page_id]=page_title




    local_admin_category_title = str(page_ids_page_titles[admin_category_page_id])
    print ('In '+languagecode+' this category is called: '+local_admin_category_title+'.')


    # START OF A CATEGORY CRAWLING
    try:
        query = 'ALTER TABLE '+languagecode+'wiki_pages ADD COLUMN '+admin_category_title+'_level integer;'
        cursor2.execute(query); conn2.commit()
    except:
        pass

    try:
        query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = null;'
        cursor2.execute(query); conn2.commit()
    except:
        pass


    # ASSIGNING JUMPS
    total_categories = set()
    total_pages = set()

    # this is LEVEL 1
    query = 'SELECT subcategory_title FROM category_links_cat_cat WHERE category_title = "'+local_admin_category_title+'"'
    df = pd.read_sql_query(query, conn).set_index('subcategory_title')
    categories_collected = set(df.index.tolist())

    parameters = []
    for x in categories_collected:
        try:
            parameters.append((cat_titles_page_ids[x],cat_titles_qitems[x]))
            total_categories_count+=1
        except:
            pass
    query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = 1 WHERE page_id = ? AND qitem = ?'
    cursor2.executemany(query,parameters)
    conn2.commit()


    query = 'SELECT DISTINCT clcc.page_id FROM category_links_cat_art clcc WHERE clcc.category_title = "'+local_admin_category_title+'"'
    df = pd.read_sql_query(query, conn).set_index('page_id')
    pages_collected = set(df.index.tolist())


    parameters = []
    parameters_qitem = []
    for x in pages_collected:
        # print (str(x)+'\t'+str(page_ids_page_titles[x])+'\t'+str(page_ids_qitems[x]))
        try:
            qt = page_ids_qitems[x]
            if qt != None:
                parameters.append((x,qt))
            else:
                parameters_qitem.append((x,))

            # edfile2.write('1'+'\t'+str(x)+'\t'+str(page_ids_qitems[x])+'\t'+str(page_ids_page_titles[x])+'\n')
            total_pages_count+=1
        except:
            pass
    query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = 1 WHERE page_id = ? AND qitem = ?'
    cursor2.executemany(query,parameters)
    conn2.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = 1 WHERE page_id = ? AND qitem is null;'
    cursor2.executemany(query,parameters_qitem)
    conn2.commit()


    total_categories.add(page_ids_page_titles[admin_category_page_id]) # ADD THE LEVEL ZERO
    total_categories.update(categories_collected) # ADD THE LEVEL ONE

    total_pages.update(pages_collected) # ADD THE LEVEL ONE

    print ('* current LEVEL: 1')
    print (str(len(categories_collected))+'. is the number of categories at Level 1: ')
    print (str(len(pages_collected))+' is the number of pages at Level 1.')

    print ('Categories at level 1:')
    print (categories_collected)



    levels = 2
    increment = 1000

    level_zero_pages = 0
    while 1:

        print ('\n* current LEVEL: '+str(levels))

        i = 0
        j = increment
        categories_collected_iterate = list(categories_collected)
        while len(categories_collected_iterate[i:j]) > 0:

            page_asstring = ','.join( ['?'] * len(categories_collected_iterate[i:j]) )
            query = 'SELECT DISTINCT clcc.subcategory_title FROM category_links_cat_cat clcc INNER JOIN categories c ON clcc.subcategory_title = c.category_title WHERE c.hidden != 1 AND clcc.category_title IN (%s) ' % page_asstring
            df = pd.read_sql_query(query, conn, params = categories_collected_iterate[i:j]).set_index('subcategory_title')

            categories_collected.update(set(df.index.tolist()))
 

            page_asstring = ','.join( ['?'] * len(categories_collected_iterate[i:j]) )
            query = 'SELECT DISTINCT clcc.page_id FROM category_links_cat_art clcc WHERE clcc.category_title IN (%s) ' % page_asstring
            df = pd.read_sql_query(query, conn, params = categories_collected_iterate[i:j]).set_index('page_id')

            pages_collected.update(set(df.index.tolist()))

            i += increment
            j += increment


        print ('- categories:')
        print (len(categories_collected))
        categories_collected = categories_collected - total_categories
        print (len(categories_collected))
        print (list(categories_collected)[5:10])
        total_categories.update(categories_collected)
        print (len(total_categories))
        print ('- categories.\n')

        print ('- pages:')
        print (len(pages_collected))
        pages_collected = pages_collected - total_pages
        print (len(pages_collected))
        pages_collected = pages_collected.intersection(set(page_ids_page_titles_admin_pages.keys()))
        print ('\t\t'+str(len(pages_collected))+' real.') # aquet és l'important
        b = []
        for x in list(pages_collected)[5:10]: b.append(page_ids_page_titles[x])
        print (b)
        total_pages.update(pages_collected)
        print (len(total_pages))
        print ('- pages.\n')

        if len(pages_collected) == 0:
            level_zero_pages+=1




        parameters_categories = []
        parameters_categories_qitem = []
        for x in categories_collected:
            try:
                qt = cat_titles_qitems[x]
                if qt != None:
                    parameters_categories.append((cat_titles_page_ids[x],qt))
                else:
                    parameters_categories_qitem.append((cat_titles_page_ids[x],))

                total_categories_count+=1

            except:
                pass

        query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = "'+str(levels)+'" WHERE page_id = ? AND qitem = ?;'
        cursor2.executemany(query,parameters_categories)
        query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = "'+str(levels)+'" WHERE page_id = ? AND qitem is null;'
        cursor2.executemany(query,parameters_categories_qitem)




        parameters_pages = []
        parameters_pages_qitem = []
        for x in pages_collected:
            try:
                qt = page_ids_qitems[x]
                if qt != None:
                    parameters_pages.append((x,qt))
                else:
                    parameters_pages_qitem.append((x,))

                total_pages_count+=1


            except:
                pass

        query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = "'+str(levels)+'" WHERE page_id = ? AND qitem = ?;'
        cursor2.executemany(query,parameters_pages)
        conn2.commit()
        query = 'UPDATE '+languagecode+'wiki_pages SET '+admin_category_title+'_level = "'+str(levels)+'" WHERE page_id = ? AND qitem is null;'
        cursor2.executemany(query,parameters_pages_qitem)
        conn2.commit()



        levels = levels + 1
        if len(categories_collected) == 0: break

        if level_zero_pages == 3:
            print ('we stop here.')
            break


    print ('---')
    print ('total number of pages (non categories) collected: '+str(total_pages_count))
    print ('total number of categories collected: '+str(total_categories_count))
    print ('--- done with levels of category: '+admin_category_title+' / '+ str(page_ids_page_titles[admin_category_page_id])+' ---')
    print ('* ')


    # update the admin_categories table
    query = 'UPDATE admin_categories SET run = 1 WHERE languagecode = "'+languagecode+'" AND qitem = "'+admin_qitem+'";'
    cursor3.execute(query)
    conn3.commit()

    return pages_collected, categories_collected



def extend_admin_categories_existing_crawling(languagecode, language_admin_categories):


    functionstartTime = time.time()
    function_name = 'extend_admin_categories_existing_crawling '+languagecode
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    admin_categories_language = language_admin_categories[languagecode]
    print (admin_categories_language)
    if len(admin_categories_language) ==  0:
        print ('There has been no categorization, since there is not top admin category local in this language.')
        return

    # admin_categories_language = {'Q2944440': ['Stubs', 2009362], 'Q2954058': ['Wikipedia essays', 2080248], 'Q4582366': ['Wikipedia policies and guidelines', 937176], 'Q4588883': ['Wikipedia help', 1536902], 'Q4615845': ['Wikipedia deletion', 1580927], 'Q4852393': ['Wikipedia copyright', 1048326], 'Q5324375': ['Wikipedia maintenance', 738040], 'Q5492333': ['WikiProjects', 706467], 'Q6192227': ['Wikipedia tools', 3132667], 'Q7216441': ['Wikipedia disclaimers', 1804709], 'Q9118779': ['Wikipedia village pump', 44675965]}




    admin_category_counter = 1
    for qitem, category in admin_categories_language.items():
        admin_category_title = category[0].replace(' ','_')
        admin_category_page_id = category[1]

        print ('\n\n\n')
        print (qitem, admin_category_title, admin_category_page_id)

        admin_category_category_crawling(languagecode, admin_category_title, admin_category_page_id, qitem)

        ### END OF CATEGORY CRAWLING FOR A CATEGORY
        admin_category_counter+= 1


    print ('There have been '+str(admin_category_counter)+' categorizations with the category crawling and original admin categories.')



    # duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def extend_admin_categories_assigning_one_category(languagecode):


    functionstartTime = time.time()
    function_name = 'extend_admin_categories_assigning_one_category '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()

    list_categories = ''
    list_cats = []
    for qitem, admin_category_title in admin_categories.items():
        list_categories+= admin_category_title+'_level, '
        list_cats.append(admin_category_title)

    list_categories = list_categories[:len(list_categories)-2]

    params = []

    selected_categories = {}
    query = 'SELECT page_title, page_id, '+list_categories+' FROM '+languagecode+'wiki_pages;'
    print (query)

    input('')
    for row in cursor2.execute(query):
        page_title=row[0]
        page_id = row[1]

        category_levels = {}
        x = 2
        for admin_category_title in list_categories:
            category_levels[admin_category_title] = row[x]
            x=+1

        admin_categories_top_level = ''
        category_levels_sorted = {k: v for k, v in sorted(x.items(), key=lambda item: item[1])}
        for category, level in category_levels_sorted.items():
            admin_categories_top_level+= admin_categories_top_level+category+':'+level+';'

        admin_categories_top_level = admin_categories_top_level[:len(admin_categories_top_level)-1]
        admin_page_main_category = category_levels_sorted.keys()[0]

        params.append((admin_categories_top_level,admin_page_main_category,page_id))



    # update the admin_categories table
    query = 'UPDATE '+languagecode+'wiki_pages SET admin_categories_top_level = ?, admin_page_main_category = ? WHERE page_id = ?;'
    cursor2.executemany(query,params); conn2.commit()
    conn2.commit()
    params = []


    try:
        os.remove(databases_path + languagecode + 'wiki_category_links_temp.db'); print (languagecode + 'wiki_category_links_temp.db'+' deleted.');
    except:
        pass


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def extend_admin_categories_interwiki_approach(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):

    functionstartTime = time.time()
    function_name = 'extend_admin_categories_interwiki_approach '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor3 = conn3.cursor()

    cat_titles_page_ids = {}
    cat_titles_qitems = {}
    query = 'SELECT page_title, page_id, qitem FROM '+languagecode+'wiki_pages WHERE page_namespace = 14;'
    for row in cursor2.execute(query):
        cat_title=row[0].replace(' ','_')
        p_id = row[1]
        cat_titles_page_ids[cat_title] = p_id
        cat_titles_qitems[cat_title] = row[2]

    cat_page_ids_titles = {}
    query = 'SELECT category_title, page_id FROM categories;'
    for row in cursor3.execute(query):
        cat_page_ids_titles[row[1]] = row[0]



    missing_categories = {}
    query = 'SELECT qitem, category_name FROM admin_categories WHERE languagecode = '+languagecode+' AND run IS NULL AND alternative_category = 1;'
    # SELECT qitem, category_name FROM admin_categories WHERE languagecode = 'it' AND run != 1 AND alternative_category = 1;

    for row in cursor.execute(query):
        missing_categories[row[0]] = row[1]


    if len(missing_categories) == 0:
        print ('For this language: '+languagecode+', all the main administrative categories existed and there is no need for further page categorization.')
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return


    print ('For this language: '+languagecode+', these are the administrative pages categories that do not exist:')
    print (missing_categories)


    counter = 1
    for qitem, admin_category_title in missing_categories.items():

        print ('* Category number :'+str(counter))

        potential_base_categories = {}
        query = 'SELECT page_id, ep.admin_categories_top_level, page_title FROM '+languagecode+'wiki_pages p INNER JOIN enwiki_pages ep ON p.qitem = ep.qitem WHERE page_namespace = 14 AND ep.admin_page_main_category = "'+admin_category_title+'";'

            # we look for the categories which had the 'missing category' assigned in enwiki.
        for row in cursor.execute(query):
            admin_categories_top_level = row[1]
            for cat_level in admin_categories_top_level.split(';'):
                cat_level_s = cat_level.split(':')

                if cat_level_s[0] == admin_category_title:
                    potential_base_categories[row[0]] = row[1]
                # print (row[2])


        closer_to_the_top = min(potential_base_categories, key=potential_base_categories.get)
        print ('From the categories we collected in enwiki, this is the highest to the top in the graph in enwiki: ')
        print (closer_to_the_top)

        print ("Let's check them all anyway... to see which one is the largest in target langwiki")
        list_potential_base_categories = list(potential_base_categories.keys())
        print ('There are '+str(len(list_potential_base_categories))+' candidate categories.')
        print (list_potential_base_categories)

        print ('Looking for the highest and largest category from this list.')
        cat_titles_page_ids, page_id, page_title = search_highest_largest_category_from_list(languagecode, list_potential_base_categories, cat_titles_page_ids)

        print ('This is the largest: '+page_title+' '+str(page_id))


        # time for the category crawling.
        pages_collected, categories_collected = admin_category_category_crawling(languagecode, admin_category_title, qitem)


        # we do check that the majority of the assigned pages/categories in enwiki are in the collected *pages* and *categories*,
        potential_articles = {}
        query = 'SELECT page_id, page_title FROM '+languagecode+'wiki_pages p INNER JOIN enwiki_pages ep ON p.qitem = ep.qitem WHERE page_namespace != 14 AND ep.admin_page_main_category = "'+admin_category_title+'";' # ALTERNATIVA: Posar un LIKE % i fer servir admin_categories_top_level en lloc de main_category.

        for row in cursor.execute(query):
            potential_articles[row[0]] = row[1]

        potential_articles_set = set(potential_articles.keys())
        potential_categories_set = set(potential_base_categories.keys())

        itsect_pages = len(pages_collected.intersection(potential_articles_set))
        itsect_categories = len(categories_collected.intersection(potential_categories_set))
        
        percent_pages_coinc = 100*itsect_pages/len(pages_collected)
        percent_categories_coinc = 100*itsect_categories/len(categories_collected)

        print ('The percentage of pages from enwiki assigned to the '+admin_category_title+' that we could collect in the category crawling: '+str(percent_pages_coinc))

        print ('The percentage of categories from enwiki assigned to the '+admin_category_title+' that we could collect in the category crawling: '+str(percent_categories_coinc))

        if percent_pages_coinc < 70: print('NOT WORKING FOR PAGES. We need to change something.')
        if percent_categories_coinc < 70: print('NOT WORKING FOR CATEGORIES.')


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def assign_types_of_admin_pages():


    function_name = 'assign_types_of_admin_pages'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()



    # FIRST, WE ASSIGN THE FINAL PAGES BASED ON THE THREE SOURCES: NAMESPACES, CATEGORIES, AND WIKIDATA:
    print ('assigning final values to admin_page_type')
    for languagecode in wikilanguagecodes:
        print (languagecode)
    #    print ('cleant. NOW WE STaRT.')
    #    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = NULL;'
    #    cursor2.execute(query);
    #    conn2.commit()

        # POSITIVE GROUNDTRUTH
        query = 'UPDATE '+languagecode+'wiki SET admin_page_type = 1 WHERE ccc_geolocated=1;'
        cursor2.execute(query);
        conn2.commit()
        print ('geolocated in, done.')

        query = 'UPDATE '+languagecode+'wiki SET admin_page_type = 1 WHERE country_wd IS NOT NULL;'
        cursor2.execute(query);
        conn2.commit()
        print ('country_wd, done.')

        query = 'UPDATE '+languagecode+'wiki SET admin_page_type = 1 WHERE location_wd IS NOT NULL;'
        cursor2.execute(query);
        conn2.commit()
        print ('location_wd, done.')


    # SECOND, WE COMPUTE SOME STATS.

    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    time_range = 'last month'


    # zero interwiki links
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE admin_page_type IS NOT NULL;'
    cursor.execute(query)
    all_articles_count = cursor.fetchone()[0]

    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE num_interwiki = 0 GROUP BY 1;'
    admin_pages_type_zero_ILL = {}
    count_articles = 0
    for row in cursor.execute(query):
        try: count = int(row[1])
        except: count = 0

        admin_pages_type_zero_ILL[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_type_zero_ILL.items():
        insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_ill', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_ill', count_articles, all_articles_count, cycle_year_month)


    # zero inlinks
    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE num_inlinks = 0 GROUP BY 1;'
    admin_pages_type_zero_Inlinks = {}
    count_articles = 0
    for row in cursor.execute(query):
        try: count = int(row[1])
        except: count = 0
        admin_pages_type_zero_Inlinks[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_type_zero_Inlinks.items():
        insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_inlinks', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_inlinks', count_articles, all_articles_count, cycle_year_month)


    # zero subcategories
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE admin_page_type IS NOT NULL AND page_namespace = 14;'
    cursor.execute(query)
    all_categories_count = cursor.fetchone()[0]

    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE page_namespace = 14 AND num_categories_contains = 0 GROUP BY 1;'
    admin_pages_categories_zero_subcats = {}
    count_articles = 0
    for row in cursor.execute(query):
        try: count = int(row[1])
        except: count = 0
        admin_pages_categories_zero_subcats[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_categories_zero_subcats.items():
        insert_stats_values(time_range,cursor2,'categories',None,languagecode,'admin_pages',admin_page_type,'zero_subcats', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'categories',None,languagecode,'admin_pages',admin_page_type,'zero_subcats', count_articles, all_categories_count, cycle_year_month)


    # zero subcategories and pages
    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE page_namespace = 14 AND num_categories_contains = 0 AND num_pages_contains = 0 GROUP BY 1;'
    admin_pages_categories_zero_subcats = {}
    count_articles = 0
    for row in cursor.execute(query):
        try: count = int(row[1])
        except: count = 0
        admin_pages_categories_zero_subcats[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_categories_zero_subcats.items():
        insert_stats_values(time_range,cursor2,'categories',None,languagecode,'admin_pages',admin_page_type,'zero_subcats_pages', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'categories',None,languagecode,'admin_pages',admin_page_type,'zero_subcats_pages', count_articles, all_categories_count, cycle_year_month)



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def extend_links(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):

    functionstartTime = time.time()
    function_name = 'extend_links '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()

    # try: cursor.execute('SELECT 1 FROM '+languagecode+'wiki_pages;')
    # except: return

    content_selection_page_title = {}
    content_selection_page_id = {}

    content_selection_page_title = {}
    content_selection_page_id = {}
    query = 'SELECT page_id, page_title, page_namespace FROM '+languagecode+'wiki_pages;'
    for row in cursor.execute(query):
        page_id = row[0]
        page_title = row[1].replace(' ','_')
        page_namespace = row[2]
        content_selection_page_id[page_id]=page_title
        content_selection_page_title[page_title]=page_id

        if page_namespace in [4, 12]:
            content_selection_page_title_admin_page[page_id] = page_title
            content_selection_page_id_admin_page[page_title] = page_id


#    print (len(page_titles_page_ids),len(content_selection_page_id),len(other_content_selection_page_id))
    num_of_outlinks = {}
    num_of_inlinks = {}

    num_outlinks_ap = {}
    num_inlinks_ap = {}

    for page_id in content_selection_page_id.keys():
        num_of_outlinks[page_id]=0
        num_of_inlinks[page_id]=0

        num_outlinks_ap[page_id]=0
        num_inlinks_ap[page_id]=0
    print (len(num_of_outlinks))


#    dumps_path = 'gnwiki-20190720-pagelinks.sql.gz' # read_dump = '/public/dumps/public/wikidatawiki/latest-all.json.gz'

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-pagelinks.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)
    try:
        dump_in = gzip.open(dumps_path, 'r')
    except:
        print ('error. the file pagelinks is not working.')

    w = 0
    iteratingstartTime = time.time()
    print ('Iterating the dump.')
    while True:
        line = dump_in.readline()
        try: line = line.decode("utf-8")
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            # table_name = wikilanguages_utils.get_table_name(line)
            # columns = wikilanguages_utils.get_columns(line)
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                w+=1
                # print(row)
                # input('')
                pl_from = int(row[0])
                pl_from_namespace = row[1]
                pl_title = str(row[2])
                pl_namespace = row[3]

#                if pl_from == 893:
#                    print(row)

                try:
                    pl_title_page_id = content_selection_page_title[pl_title]
                    # print (pl_title_page_id, pl_title)
                except:
                    pl_title_page_id = None



                ########
                # OUTLINKS
                # Here pl_from must be in AP.


                # outlinks from AP to any page.
                try:
                    num_of_outlinks[pl_from]= num_of_outlinks[pl_from] + 1
                    # print('num_outlinks')
                    # print (num_of_outlinks[pl_from])
                    # input('')
                except:
                    pass

                # outlinks from AP to AP.
                try:
                    AP=content_selection_page_id_admin_page[pl_title_page_id]
                    num_outlinks_ap[pl_from] = num_outlinks_ap[pl_from] + 1
                    # print ('num_outlinks_ap')
                    # print (num_outlinks_ap[pl_from])
                    # print (row)
                    # input('')
                except:
                    pass



                ########
                # INLINKS
                # Here pl_title or pl_title_page_id must be in AP.



                # inlinks to AP from any page.
                try:
                    num_of_inlinks[pl_title_page_id] = num_of_inlinks[pl_title_page_id] + 1
                    # print('num_inlinks')
                    # print(row)
                    # input('')
                except:
                    pass

                # inlinks to AP from AP.
                try:
                    AP=content_selection_page_id_admin_page[pl_from]
                    num_inlinks_ap[pl_title_page_id] = num_inlinks_ap[pl_title_page_id] + 1
                    # print('num_inlinks_ap')
                    # print(row)
                    # input('')
                except:
                    pass



                if w % 1000000 == 0: # 10 million
                    print (w)
                    print ('current time: ' + str(time.time() - iteratingstartTime)+ ' '+languagecode)
                    print ('number of lines per second: '+str(round(((w/(time.time() - iteratingstartTime))/1000),2))+ ' thousand.')


#    input('')
    print ('Done with the dump.')


    parameters = []
    parameters_q = []
    for page_title, page_id in content_selection_page_title.items():
        qitem = page_ids_qitems[page_id]

        num_outlinks = 0
        num_outlinks_to_AP = 0

        num_inlinks = 0
        num_inlinks_from_AP = 0

        try:
            num_outlinks = num_of_outlinks[page_id]
        except:
            pass

        try:
            num_outlinks_to_AP = num_outlinks_ap[page_id]
        except:
            pass
        if num_outlinks!= 0: percent_outlinks_to_AP = float(num_outlinks_to_AP)/float(num_outlinks)
        else: percent_outlinks_to_AP = 0


        try:
            num_inlinks = num_of_inlinks[page_id]
        except:
            pass

        try:
            num_inlinks_from_AP = num_inlinks_ap[page_id]
        except:
            pass
        if num_inlinks!= 0: percent_inlinks_from_AP = float(num_inlinks_from_AP)/float(num_inlinks)
        else: percent_inlinks_from_AP = 0

        if qitem != None:
            parameters.append((num_outlinks,num_outlinks_to_AP,percent_outlinks_to_AP,num_inlinks,num_inlinks_from_AP,percent_inlinks_from_AP,page_id,qitem))
        else:
            parameters_q.append((num_outlinks,num_outlinks_to_AP,percent_outlinks_to_AP,num_inlinks,num_inlinks_from_AP,percent_inlinks_from_AP,page_id))            

        # print ((num_outlinks,num_outlinks_to_AP,percent_outlinks_to_AP,num_inlinks,num_inlinks_from_AP,percent_inlinks_from_AP,page_id,qitem,page_title))


    query = 'UPDATE '+languagecode+'wiki_pages SET (num_outlinks,num_outlinks_to_admin_pages,percent_outlinks_to_admin_pages,num_inlinks,num_inlinks_from_admin_pages,percent_inlinks_from_admin_pages)=(?,?,?,?,?,?) WHERE page_id = ? AND qitem = ?;'        
    cursor.executemany(query,parameters)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET (num_outlinks,num_outlinks_to_admin_pages,percent_outlinks_to_admin_pages,num_inlinks,num_inlinks_from_admin_pages,percent_inlinks_from_admin_pages)=(?,?,?,?,?,?) WHERE page_id = ? AND qitem IS NULL;'        
    cursor.executemany(query,parameters_q)
    conn.commit()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def extend_pageviews():
    function_name = 'extend_pageviews'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()



    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()

    # for languagecode in wikilanguagecodes:
    #     query = 'UPDATE '+languagecode+'wiki_pages SET num_pageviews = 0;'
    #     cursor.execute(query)
    # conn.commit()
    # print ('zero.')


    lang_page_titles_complete_page_ids = {}
    lang_page_ids_qitems = {}
    for last_language in wikilanguagecodes:


        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,last_language)
        namespaces_dict = wikilanguages_utils.get_namespace_names(last_language)

        namespaces_dict_inv = {}
        for v, k in namespaces_dict.items():
            try:
                namespaces_dict_inv[k] = namespaces_dict_inv[k]+';'+v
            except:
                namespaces_dict_inv[k] = v

        # namespaces_dict_inv = {v: k for k, v in namespaces_dict.items()}
        # print (namespaces_dict_inv)

        page_titles_complete_page_ids = {}
        for page_id, namespace in page_ids_namespaces.items():
            ns_names = namespaces_dict_inv[namespace]
            if ';' in ns_names:
                ns = ns_names.split(';')
                for n in ns:
                    page_titles_complete_page_ids[n+page_ids_page_titles[page_id]] = page_id
                    # print (n+page_ids_page_titles[page_id])
            else:
                page_titles_complete_page_ids[ns_names+page_ids_page_titles[page_id]] = page_id


        lang_page_ids_qitems[last_language]=page_ids_qitems
        lang_page_titles_complete_page_ids[last_language]=page_titles_complete_page_ids
    print ('we have the good page_titles.')




    last_day = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
    first_day = last_day.replace(day=1)
    month_year = last_day.strftime("%Y-%m")
    current_year =  last_day.year

    cur_day = first_day
    dumps = []
    while cur_day <= last_day:
        i = 0
        while i < 24:
            if i <= 9: hours = '0'+str(i)+'0000'
            else: hours = str(i)+'0000'
            i+= 1
            # print('pageviews-'+cur_day.strftime("%Y%m%d")+'-'+''+hours+'.gz')
            dumps.append('pageviews-'+cur_day.strftime("%Y%m%d")+'-'+''+hours+'.gz')
        cur_day = cur_day + datetime.timedelta(days=1)




    pageviews_language = {}
    for languagecode in wikilanguagecodes:
        pageviews_language[languagecode] = {}


    # https://dumps.wikimedia.org/other/pageviews/2022/2022-07/
    for filename in dumps:
        read_dump = '/public/dumps/public/other/pageviews/'+str(current_year)+'/'+month_year+'/'+filename

        try:
            dump_in = gzip.open(read_dump, 'r')
            no_dump = False
        except:
            print (filename+ ' does not exist.')


        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values=line.split(' ')
        last_language = values[0].split('.')[0]

        print ('* file: ' +filename)
        # print ('there we go.')

        pageviews_dict = {}
        iter = 0
        while line != '':
            iter += 1
            if iter % 1000000 == 0: print (str(iter/1000000)+' million lines.')
            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values=line.split(' ')


            if len(values)<3: continue
            language = values[0].split('.')[0]
            page_title = values[1]
            pageviews_count = values[2]

            if language!=last_language:

                if last_language in wikilanguagecodes:

                    page_titles_complete_page_ids = lang_page_titles_complete_page_ids[last_language]
                    for page_title, value in pageviews_dict.items():
                        try:
                            page_titles_complete_page_ids[page_title]
                        except:
                            continue

                        try:
                            pageviews_language[last_language][page_title] = pageviews_language[last_language][page_title] + value
                        except:
                            pageviews_language[last_language][page_title] = value

                        # if last_language == 'ca':
                        #     print (page_title, value)


                pageviews_dict={}
    #            input('')

            if pageviews_count == '': continue
    #            print (line)
            if page_title in pageviews_dict: 
                pageviews_dict[page_title]=pageviews_dict[page_title]+int(pageviews_count)
            else:
                pageviews_dict[page_title]=int(pageviews_count)

            last_language=language


        # last round
        if last_language in wikilanguagecodes:

            page_titles_complete_page_ids = lang_page_titles_complete_page_ids[last_language]
            for page_title, value in pageviews_dict.items():
                try:
                    page_titles_complete_page_ids[page_title]
                except:
                    continue
                try:
                    pageviews_language[last_language][page_title] = pageviews_language[last_language][page_title] + value
                except:
                    pageviews_language[last_language][page_title] = value

        print ('end of the dump.')

    


    print('end of dumps. time to store.')
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode)

        page_title_pageviews = pageviews_language[languagecode]
        print (len(page_title_pageviews))

        page_titles_complete_page_ids = lang_page_titles_complete_page_ids[languagecode]
        print (len(page_titles_complete_page_ids))

        page_ids_qitems = lang_page_ids_qitems[languagecode]

        print ('storing the pageviews.')
        # edfile2 = open(databases_path+languagecode+'_temporary_pageviews_metrics.txt', "w")


        pageviews_metrics_parameters = []
        pageviews_metrics_parameters_qitem_null = []
        for key,value in page_title_pageviews.items():

            try:
                page_id = page_titles_complete_page_ids[key]
            except:
                continue


            qitem = page_ids_qitems[page_id]
            #print (key,value,page_id,qitem)
            if qitem == None:
                pageviews_metrics_parameters_qitem_null.append((value,page_id))
            else:
                pageviews_metrics_parameters.append((value,page_id,qitem))


            # if languagecode == 'en':
            #     print (key, value)
            #     print (key+'\t'+str(value)+'\t'+str(page_id)+'\t'+qitem+'\n')

            # pageviews_metrics_parameters.append((value,page_id,qitem))

            # print ((key[0], key[1], pageviews_dict[(key[0],key[1])], page_id))
            # edfile2.write(str(value)+'\t'+str(page_id)+'\t'+qitem+'\n')
            # except:
            #     pass


        # a_file = open(databases_path+languagecode+"_temporary_pageviews_metrics.txt")
#        pageviews_metrics_parameters = csv.reader(a_file, delimiter="\t", quotechar = '|')


        print ('about to insert the pageviews.')
        query = 'UPDATE '+languagecode+'wiki_pages SET num_pageviews = ? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,pageviews_metrics_parameters)
        print (len(pageviews_metrics_parameters))
        conn.commit()

        query = 'UPDATE '+languagecode+'wiki_pages SET num_pageviews = ? WHERE page_id = ? AND qitem IS NULL;'
        cursor.executemany(query,pageviews_metrics_parameters_qitem_null)
        print (len(pageviews_metrics_parameters_qitem_null))
        conn.commit()
        print ('in!')

        # os.remove(databases_path +languagecode+'_temporary_pageviews_metrics.txt')
        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))



    # INSERT STATS ABOUT PAGEVIEWS
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    print ('inserting the stats.')
    time_range = 'last month'

    query = 'SELECT admin_page_type, SUM(num_pageviews) FROM '+languagecode+'wiki_pages GROUP BY 1;'
    admin_pages_type_pageviews = {}

    sum_pv = 0
    for row in cursor.execute(query):
        try: pv = int(row[1])
        except: pv = 0

        admin_pages_type_pageviews[row[0]]=pv
        sum_pv += pv

    for admin_page_type, pageviews in admin_pages_type_pageviews.items():
        insert_stats_values(time_range,cursor2,'pageviews',None,languagecode,'admin_pages','admin_pages',admin_page_type,pageviews, sum_pv, cycle_year_month)


    # zero pageviews
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE admin_page_type IS NOT NULL;'
    cursor.execute(query)
    all_articles_count = cursor.fetchone()[0]

    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE num_pageviews = 0 GROUP BY 1;'
    admin_pages_type_zero_Inlinks = {}
    count_articles = 0
    for row in cursor.execute(query):
        try: count = int(row[1])
        except: count = 0

        admin_pages_type_zero_Inlinks[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_type_zero_Inlinks.items():
        insert_stats_values(time_range,cursor2,'pageviews',None,languagecode,'admin_pages',admin_page_type,'zero_pageviews', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'pageviews',None,languagecode,'admin_pages',admin_page_type,'zero_pageviews', count_articles, all_articles_count, cycle_year_month)


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# Extends the Articles table with the number of external_links per Article.
def extend_external_links(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):

    functionstartTime = time.time()
    function_name = 'extend_external_links '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-externallinks.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)

    dump_in = gzip.open(dumps_path, 'r')

    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()


    num_external_links = {}
    for page_id,page_title in page_ids_page_titles.items():
        num_external_links[page_id]=0

    print (languagecode)
    print ('Iterating the dump.')
    while True:
        line = dump_in.readline()
        try: line = line.decode("utf-8")
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            # table_name = wikilanguages_utils.get_table_name(line)
            # columns = wikilanguages_utils.get_columns(line)
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                try:
                    el_from = int(row[1])
                    num_external_links[el_from]+=1
                except:
                    pass
    print ('Done with the dump.')

    parameters = []
    parameters_qitems = []
    for page_id, value in num_external_links.items():
        qitem = page_ids_qitems[page_id]
        if value == 0: continue

        if qitem != None:
            parameters.append((value,page_id,qitem))
        else:
            parameters_qitems.append((value,page_id))

        # print (value,page_id,qitem)

    query = 'UPDATE '+languagecode+'wiki_pages SET num_external_links=? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,parameters)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_external_links=? WHERE page_id = ? AND qitem is null;'
    cursor.executemany(query,parameters_qitems)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# Extends the Articles table with the number of images.
def extend_images():

    functionstartTime = time.time()
    function_name = 'extend_images'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    ####
    # create imagelinks.db
    conn = sqlite3.connect(databases_path + imageslinks_db); cursor = conn.cursor()
    for languagecode in wikilanguagecodes:
        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode)


        cursor.execute("CREATE TABLE IF NOT EXISTS imagelinks (langcode text, page_id integer, image_title text, PRIMARY KEY (langcode, page_id, image_title));")
        conn.commit()

        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-imagelinks.sql.gz'
        wikilanguages_utils.check_dump(dumps_path, script_name)

        dump_in = gzip.open(dumps_path, 'r')
        images_count = {}
        images_from = {}

        print ('Iterating the imagelinks dump: '+dumps_path)

        start_dict_true = {}
        k = 0
        j = 0
        parameters = []
        while True:
            line = dump_in.readline()
            try: line = line.decode("utf-8")
            except UnicodeDecodeError: line = str(line)

            if len(start_dict_true)<3:
                if '`il_from` ' in line: 
                    start_dict_true['il_from']=k
                    k+=1
                if '`il_to` ' in line: 
                    start_dict_true['il_to']=k
                    k+=1
                if '`il_from_namespace` ' in line: 
                    start_dict_true['il_from_namespace']=k
                    k+=1

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)
                for row in rows:

                    j+=1
                    try:
                        il_from_namespace = int(row[start_dict_true['il_from_namespace']])
                    except:
                        continue
                    if il_from_namespace == 0: continue


                    try:
                        il_from = int(row[start_dict_true['il_from']])
                    except:
                        continue

                    il_to = row[start_dict_true['il_to']]

                    parameters.append((languagecode, il_from, il_to))

                    if j % 500000 == 0:
                        print (languagecode+' imagelinks row: '+str(j))
                        query = 'INSERT OR IGNORE INTO imagelinks (langcode, page_id, image_title) VALUES (?,?,?);'
                        cursor.executemany(query,parameters)
                        conn.commit()
                        parameters = []

        query = 'INSERT OR IGNORE INTO imagelinks (langcode, page_id, image_title) VALUES (?,?,?);'
        cursor.executemany(query,parameters)
        conn.commit()

        print ('Done with the dump imagelinks for language: '+languagecode)
        print ('Lines: '+str(j))
        print ('* number of parameters to introduce: '+str(len(parameters))+'\n')
        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    


    ####
    # extend num_images in wikipedia_administrative_pages_analytics_db.db
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + imageslinks_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:

        (page_ids_qitems, page_ids_page_titles, page_ids_namespaces)=wikilanguages_utils.load_dicts_page_ids_qitems_namespace(0,languagecode)
        print (languagecode)

        query = "SELECT page_id, count(image_title) FROM imagelinks WHERE langcode = ? GROUP BY page_id;"

        params = []
        params_qitems = []
        for row in cursor2.execute(query, (languagecode,)):
            page_id = row[0]
            count = row[1]
            try:
                qitem = page_ids_qitems[page_id]
            except:
                continue

            if qitem != None:
                params.append((count, page_id, qitem))
            else:
                params_qitems.append((count, page_id))

            # print (count, page_id)

        query = 'UPDATE '+languagecode+'wiki_pages SET num_images = ? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,params)
        conn.commit()
        query = 'UPDATE '+languagecode+'wiki_pages SET num_images = ? WHERE page_id = ? AND qitem is null;'
        cursor.executemany(query,params_qitems)
        conn.commit()


    os.remove(databases_path+imageslinks_db)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def get_mediawiki_paths(languagecode):

    cym = cycle_year_month
    d_paths = []

    print ('/public/dumps/public/other/mediawiki_history/'+cym)
    if os.path.isdir('/public/dumps/public/other/mediawiki_history/'+cym)==False:
        cym = datetime.datetime.strptime(cym,'%Y-%m')-dateutil.relativedelta.relativedelta(months=1)
        cym = cym.strftime('%Y-%m')
        print ('/public/dumps/public/other/mediawiki_history/'+cym)

    dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.all-time.tsv.bz2'

    if os.path.isfile(dumps_path):
        print ('one all-time file.')
        d_paths.append(dumps_path)
    else:
        print ('multiple files.')
        for year in range (2025, 1999, -1):
            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'.tsv.bz2'
            if os.path.isfile(dumps_path): 
                d_paths.append(dumps_path)

        if len(d_paths) == 0:
            for year in range(2025, 1999, -1): # months
                for month in range(13, 0, -1):
                    if month > 9:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-'+str(month)+'.tsv.bz2'
                    else:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-0'+str(month)+'.tsv.bz2'

                    if os.path.isfile(dumps_path) == True:
                        d_paths.append(dumps_path)

    print(len(d_paths))
    print (d_paths)

    return d_paths




def extend_editing_history(languagecode, page_ids_qitems, page_ids_page_titles, page_ids_namespaces):

    functionstartTime = time.time()
    function_name = 'extend_editing_history '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    page_title_page_namespace_page_id = {}
    for page_id, page_title in page_ids_page_titles.items():
        page_namespace = page_ids_namespaces[page_id]
        page_title_page_namespace_page_id[page_title,page_namespace] = page_id


    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    # admin_page_type_dict = {}
    # query = 'SELECT page_id, admin_page_type FROM '+languagecode+'wiki_pages WHERE admin_page_type IS NOT NULL and page_namespace IN (4, 12, 100);' # No categories.
    # for row in cursor.execute(query):
    #     admin_page_type_dict[row[0]] = row[1]


    # last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
    last_month_date = datetime.datetime.strptime(cycle_year_month,'%Y-%m')
    next_month_date = datetime.datetime.strptime(cycle_year_month,'%Y-%m') + relativedelta.relativedelta(months=1)

    first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day = int(next_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day_last_month = next_month_date.replace(day = 1) - datetime.timedelta(days=1)


    # last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))
    print (first_day, last_day)



    # IF IT GETS TOO BIG, WE COULD CLEAN THE TABLE WITH THE PAGES ALREADY INSERTED.
    try:
        os.remove(databases_path+languagecode+'wiki_editors_pages.db')
    except:
        pass

    conn2 = sqlite3.connect(databases_path + languagecode+'wiki_editors_pages.db'); cursor2 = conn2.cursor()
    try:
        query = 'CREATE TABLE editors (page_id integer, editor integer, PRIMARY KEY (page_id, editor));'
        cursor2.execute(query)
        conn2.commit()
        query = 'CREATE TABLE editors_history (editor integer, edit_count integer, year_first_edit integer, flag text, PRIMARY KEY (editor));'

        cursor2.execute(query)
        conn2.commit()
        print ('editors table created.')
    except:
        print ('editors table could not be created.')
        pass


    """

    ### CREATE STATS DICTIONARIES / INITIALIZE (Stats.db)
    # edits --> comparing edits by editors


    types_of_edit = ['edits','reverts','discussion_edits','articles_created']
    types_of_content = ['wikipedia','admin_pages','type_admin_page1', 'type_admin_page2', 'type_admin_page3','ns_01','ns_02']
    types_of_editor = ['registered','registered_none','newcomer_90d','newcomer_1y','newcomer_5y','newcomer_none','lustrum_2000_2005','lustrum_2006_2010','lustrum_2011_2015','lustrum_2016_2020','lustrum_2021_2025','admin','admin_none']


    # edits --> comparing edits by editors
    monthly_edits_type_what = {}
    for x in types_of_edits:
        monthly_edits_type_what[x] = {} # monthly_edits_where = {}
        for y in types_of_content: 
            monthly_edits_type_what[x][y] = {} # monthly_edits_whom = {}
            for z in types_of_editor: 
                monthly_edits_type_what[x][y][z] = 0


    # [types of edit][types of content][type of editor]
    # monthly_edits_type_what


    # editors --> comparing editors by edits
    monthly_editors_type_what = {}
    for x in types_of_content:
        monthly_editors_type_what[x] = {} # monthly_editors_where = {}
        for y in types_of_editor:
            monthly_editors_type_what[x][y] = set()
    
    # [types of content][types of editor]
    # monthly_editors_type_what


    """



    ### CREATE DICTIONARIES FOR THE PAGES (Wikipedia_Administrative_Pages_Analytics.db)
    article_completed = {}

    first_edit_timestamp = {}
    last_edit_timestamp = {}
    last_discussion_timestamp = {}


        # initial metrics
    num_edits = {}
    num_discussions = {}
    edit_count_manual = {}

    num_anonymous_edits = {}
    num_bots_edits = {}
    num_reverts = {}


        # new last month metrics
    num_edits_last_month = {}
    num_edits_last_month_by_admin = {}
    num_edits_last_month_by_anonymous = {}
    num_edits_last_month_by_newcomer_90d = {}
    num_edits_last_month_by_newcomer_1y = {}
    num_edits_last_month_by_newcomer_5y = {}


        # regularity and engagement metrics
    active_months = {}
    cur_active_months_row = {}
    max_active_months_row = {}
    max_inactive_months_row = {}

    editing_days = {}
    days_last_50_edits = {}
    days_last_5_edits = {}
    days_last_edit = {}



    editor_history = {}



    for page_id in page_ids_page_titles.keys():
        num_edits[page_id] = 0
        num_discussions[page_id] = 0
        edit_count_manual[page_id] = []

        num_edits_last_month[page_id] = 0
        num_edits_last_month_by_admin[page_id] = 0
        num_edits_last_month_by_anonymous[page_id] = 0
        num_edits_last_month_by_newcomer_90d[page_id] = 0
        num_edits_last_month_by_newcomer_1y[page_id] = 0
        num_edits_last_month_by_newcomer_5y[page_id] = 0


        first_edit_timestamp[page_id] = 0
        last_edit_timestamp[page_id] = 0
        last_discussion_timestamp[page_id] = 0

        num_anonymous_edits[page_id] = 0
        num_bots_edits[page_id] = 0
        num_reverts[page_id] = 0

        active_months[page_id] = 0
        cur_active_months_row[page_id] = 0
        max_active_months_row[page_id] = 0
        max_inactive_months_row[page_id] = 0

        editing_days[page_id] = 0

        days_last_50_edits[page_id] = 0
        days_last_5_edits[page_id] = 0
        days_last_edit[page_id] = 0


    d_paths = get_mediawiki_paths(languagecode)
    print ('Total number of articles: '+str(len(num_edits)))


    if (len(d_paths)==0):
        print ('dump error at script '+script_name+'. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values=line.split(' ')

        parameters = []
        editors_params = []
        iter = 0
        last_year_month = 0

        while line != '':
            # iter += 1
            # if iter % 1000000 == 0: print (str(iter/1000000)+' million lines.')
            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values=line.split('\t')

            if len(values)==1: continue

            # page_title_historical = values[24]
            try: page_id = int(values[23])
            except: continue
    
            page_title = values[25]
            # if page_id not in first_edit_timestamp: continue # si fem això, ens podem trobar que no agafi les discussions...


            try: page_namespace = int(values[28])
            except: continue

            try: edit_count = values[34]
            except: continue

            if edit_count == 'null': edit_count = 1
            else: edit_count = int(edit_count)



            last_timestamp = values[3]
            if last_timestamp != 'null': 
                last_timestamp_dt = datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S')
                last_timestamp_int = int(last_timestamp_dt.strftime('%Y%m%d%H%M%S'))
            else: last_timestamp_int = 0



            # EDIT AND EDITOR TYPE INITIALIZED
            # admin_page_type = ''
            edit_type = None
            editor_registered = 0
            editor_lustrum = None
            editor_newcomer_90d = 0
            editor_newcomer_1y = 0
            editor_newcomer_5y = 0
            editor_admin = None

            ###

            revision_is_identity_revert = values[67]
            if revision_is_identity_revert == 'True':
                edit_type = 'revert'

            event_entity = values[1]
            event_type = values[2]
            if event_entity == 'page' and (event_type == 'create' or event_type == 'create-page'):
                edit_type = 'create_page'

            if page_namespace in (1,3,5,7,9,11,13,15,101,119,711,829):
                edit_type = 'discussion_edit'

            if event_type == 'revision':
                edit_type = 'edit'


            year_first_edit = ''
            editor_first_edit_timestamp = values[20]
            if editor_first_edit_timestamp != None and editor_first_edit_timestamp != '':
                
                year_first_edit = datetime.datetime.strptime(editor_first_edit_timestamp[:len(editor_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')
                editor_first_edit = datetime.datetime.strptime(editor_first_edit_timestamp[:len(editor_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S')

                if editor_first_edit >= (last_timestamp_dt - relativedelta.relativedelta(days=90)):
                    editor_newcomer_90d = 1
                    
                if editor_first_edit >= (last_timestamp_dt - relativedelta.relativedelta(days=365)):
                    editor_newcomer_1y = 1

                if editor_first_edit >= (last_timestamp_dt - relativedelta.relativedelta(days=365*5)):
                    editor_newcomer_5y = 1

                if int(year_first_edit) >= 2001 < 2006: editor_lustrum = '2001-2005'
                if int(year_first_edit) >= 2006 < 2011: editor_lustrum = '2006-2010'
                if int(year_first_edit) >= 2011 < 2016: editor_lustrum = '2011-2015'
                if int(year_first_edit) >= 2016 < 2021: editor_lustrum = '2016-2020'
                if int(year_first_edit) >= 2021 < 2026: editor_lustrum = '2021-2025'

            user_flag = values[11]
            if user_flag in ["sysop","bureaucrat","oversight","checkuser","steward"]: editor_admin = 1
            else: editor_admin = 0



            ###

            ### START OF THE SECTION FOR ADMIN TYPES
            # IF IT IS NS = 0, ARTICLES. WE DO NOT GET IN THIS.
            if page_namespace in (4, 10, 12, 14, 100, 118):

                try:
                    page_ids_page_titles[page_id]
                except:
                    continue

                if last_edit_timestamp[page_id] <= last_timestamp_int:
                    last_edit_timestamp[page_id] = last_timestamp_int

                    if edit_count > num_edits[page_id]: 
                        num_edits[page_id] = edit_count
                    # num_edits[page_id]=edit_count

                edit_count_manual[page_id].append(last_timestamp_dt)




                page_first_timestamp = values[33]
                if page_first_timestamp != 'null' and page_first_timestamp != '': 
                    page_first_timestamp = int(datetime.datetime.strptime(page_first_timestamp[:len(page_first_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'))
                else: page_first_timestamp = 0


                if last_timestamp_int == page_first_timestamp:
                    article_completed[page_id] = None
                    first_edit_timestamp[page_id]=page_first_timestamp


                last_timestamp_obj = datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S')



                # regularity
                # editing days
                seconds_last_edit = values[35]
                current_edit_day = datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')
                try:
                    previous_edit_day = (last_timestamp_obj - relativedelta.relativedelta(seconds=int(seconds_last_edit))).strftime('%Y%m%d')
                except:
                    previous_edit_day = ''

                if current_edit_day != previous_edit_day: 
                    editing_days[page_id]+=1


                # active months
                current_edit_month = datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S')
                current_edit_month_str = current_edit_month.strftime('%Y%m')

                try:
                    previous_edit_month = (last_timestamp_obj - relativedelta.relativedelta(seconds=int(seconds_last_edit)))
                    previous_edit_month_str = previous_edit_month.strftime('%Y%m')
                except:
                    previous_edit_month_str = ''
                    previous_edit_month = ''

                if current_edit_month_str != previous_edit_month_str: 
                    active_months[page_id]+=1


                # months row
                if previous_edit_month != '':
                    month_difference = (current_edit_month.year - previous_edit_month.year) * 12 + (current_edit_month.month - previous_edit_month.month)

                    if month_difference == 0:
                        pass

                    elif month_difference == 1:
                        cur_active_months_row[page_id] += 1

                        if cur_active_months_row[page_id] > max_active_months_row[page_id]:
                            max_active_months_row[page_id] = cur_active_months_row[page_id]

                    elif month_difference > 1:
                        cur_active_months_row[page_id] = 0

                    # max inactive months row
                    if month_difference > max_inactive_months_row[page_id]:
                        max_inactive_months_row[page_id] = month_difference




                user_id = values[5]
                # user_text = values[7]


                # bots
                event_is_bot_by = values[13]
                if event_is_bot_by != '':
                    try: num_bots_edits[page_id]+=1
                    except: pass

                if user_id != "":

                    if event_is_bot_by == '':
                        try:
                            editors_params.append((user_id,page_id))
                            # num_editors[page_id].add(int(user_anon))

                            try: 
                                editor_history[user_id]
                            except:

                                edit_count = values[21]

                                editor_history[user_id] = (user_id, edit_count, year_first_edit, user_flag)

                                # if user_id in [12048] or user_text == 'Hugo.arg':
                                #     print (user_id, edit_count, year_first_edit, user_flag, editor_admin, user_text)
                                #     print (editor_history[user_id])
                                #     input('')

                        except:
                            pass

                else:
                    num_anonymous_edits[page_id]+=1


                
                if revision_is_identity_revert == 'true':
                    try: num_reverts[page_id]+=1
                    except: pass


                if last_timestamp_int > first_day and last_timestamp_int < last_day:
                    # print (page_id, first_day, last_day, last_edit_timestamp)

                    if editor_admin == 1:
                        try: num_edits_last_month_by_admin[page_id]+=1
                        except: pass

                    if user_id == "":
                        try: num_edits_last_month_by_anonymous[page_id]+=1
                        except: pass


                    if editor_newcomer_90d == 1:

                        try: num_edits_last_month_by_newcomer_90d[page_id]+=1
                        except: pass

                    if editor_newcomer_1y == 1:
                        try: num_edits_last_month_by_newcomer_1y[page_id]+=1
                        except: pass

                    if editor_newcomer_5y == 1:
                        try: num_edits_last_month_by_newcomer_5y[page_id]+=1
                        except: pass


                    try: num_edits_last_month[page_id]+=1
                    except: pass



            # DISCUSSION NAMESPACES
            if page_namespace in (5, 11, 13, 15, 101, 119):

                try:
                    page_id_original = page_title_page_namespace_page_id[page_title,page_namespace-1]

                    if last_discussion_timestamp[page_id_original] < last_timestamp_int:
                        last_discussion_timestamp[page_id_original] = last_timestamp_int
                        num_discussions[page_id_original] = edit_count
                except:
                    pass
                    # print (page_title, page_namespace)
                    # print ('does not work.')

                    # print (values)
                    # input('')


            ### END OF THE SECTION FOR ADMIN TYPES




            """
            # CLASSIFY EDIT FOR STATS
            # Here we have a space to classify the current edit into the types.
            # dictionairessss


            admin_page_type = ''
            edit_type = None

            editor_registered = 0
            editor_lustrum = None
            editor_newcomer_90d = 0
            editor_newcomer_1y = 0
            editor_newcomer_5y = 0
            editor_admin = 0


            # [types of edit][types of content][type of editor]
            # monthly_edits_type_what

            # edits registered - anon
            if user_id != "":
                monthly_edits_type_what[edit_type]['wikipedia']['registered']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['registered']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['registered']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['registered']+= 1

            if user_id == "":
                monthly_edits_type_what[edit_type]['wikipedia']['registered_none']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['registered_none']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['registered_none']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['registered_none']+= 1


            # edits lustrum type
            if user_id != "":
                monthly_edits_type_what[edit_type]['wikipedia'][editor_lustrum]+= 1
                monthly_edits_type_what[edit_type]['admin_page'][editor_lustrum]+= 1
                monthly_edits_type_what[edit_type][admin_page_type][editor_lustrum]+= 1
                monthly_edits_type_what[edit_type][page_namespace][editor_lustrum]+= 1

            # edits newcomers - non newcomers
            if editor_newcomer_90d == 1:
                monthly_edits_type_what[edit_type]['wikipedia']['editor_newcomer_90d']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['editor_newcomer_90d']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['editor_newcomer_90d']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['editor_newcomer_90d']+= 1
            if editor_newcomer_1y == 1:
                monthly_edits_type_what[edit_type]['wikipedia']['editor_newcomer_1y']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['editor_newcomer_1y']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['editor_newcomer_1y']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['editor_newcomer_1y']+= 1
            if editor_newcomer_5y == 1:
                monthly_edits_type_what[edit_type]['wikipedia']['editor_newcomer_5y']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['editor_newcomer_5y']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['editor_newcomer_5y']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['editor_newcomer_5y']+= 1
            
            # edits admin
            if editor_admin == 1:
                monthly_edits_type_what[edit_type]['wikipedia']['editor_admin']+= 1
                monthly_edits_type_what[edit_type]['admin_page']['editor_admin']+= 1
                monthly_edits_type_what[edit_type][admin_page_type]['editor_admin']+= 1
                monthly_edits_type_what[edit_type][page_namespace]['editor_admin']+= 1


            # [types of content][types of editor]
            # monthly_editors_type_what


            # editors registered
            if user_id != "":
                monthly_editors_type_what['wikipedia'].add(user_id)
                monthly_editors_type_what['admin_page'].add(user_id)
                monthly_editors_type_what[admin_page_type].add(user_id)
                monthly_editors_type_what[page_namespace].add(user_id)


            # editors lustrum type
            if user_id != "":
                monthly_editors_type_what['wikipedia'][editor_lustrum].add(user_id)
                monthly_editors_type_what['admin_page'][editor_lustrum].add(user_id)
                monthly_editors_type_what[admin_page_type][editor_lustrum].add(user_id)
                monthly_editors_type_what[page_namespace][editor_lustrum].add(user_id)


            # editors newcomers - non newcomers
            if editor_newcomer_90d == 1:
                monthly_editors_type_what['wikipedia']['editor_newcomer_90d'].add(user_id)
                monthly_editors_type_what['admin_page']['editor_newcomer_90d'].add(user_id)
                monthly_editors_type_what[admin_page_type]['editor_newcomer_90d'].add(user_id)
                monthly_editors_type_what[page_namespace]['editor_newcomer_90d'].add(user_id)
            if editor_newcomer_1y == 1:
                monthly_editors_type_what['wikipedia']['editor_newcomer_1y'].add(user_id)
                monthly_editors_type_what['admin_page']['editor_newcomer_1y'].add(user_id)
                monthly_editors_type_what[admin_page_type]['editor_newcomer_1y'].add(user_id)
                monthly_editors_type_what[page_namespace]['editor_newcomer_1y'].add(user_id)
            if editor_newcomer_5y == 1:
                monthly_editors_type_what['wikipedia']['editor_newcomer_5y'].add(user_id)
                monthly_editors_type_what['admin_page']['editor_newcomer_5y'].add(user_id)
                monthly_editors_type_what[admin_page_type]['editor_newcomer_5y'].add(user_id)
                monthly_editors_type_what[page_namespace]['editor_newcomer_5y'].add(user_id)

            # editors admin
            if editor_admin == 1:
                monthly_editors_type_what['wikipedia']['editor_admin'].add(user_id)
                monthly_editors_type_what['admin_page']['editor_admin'].add(user_id)
                monthly_editors_type_what[admin_page_type]['editor_admin'].add(user_id)
                monthly_editors_type_what[page_namespace]['editor_admin'].add(user_id)

            """



            # CHANGE OF MONTH?
            current_year_month = datetime.datetime.strptime(last_timestamp_dt.strftime('%Y-%m'),'%Y-%m')

            if last_year_month != current_year_month and last_year_month != 0:
                lym = last_year_month.strftime('%Y-%m')
                print ('change of month / new: ',current_year_month, 'old: ',lym)


                """
                # STATS DATABASE INSERT
                # CLASSIFY
                # Here we would take out the info from the dictionaires and create the stats to insert them.
                # edits

                for edit_type in edit_types:


                    # insert_stats_values('monthly increment', cursor2, edit_type, editor, languagecode, set1descriptor, set2, set2descriptor, abs_value, base, last_year_month)


                    ### edits registered - anon

                    wp_edits_registered = monthly_edits_type_what[edit_type]['wikipedia']['registered']
                    admin_pages_edits_registered = monthly_edits_type_what[edit_type]['admin_page']['registered']
                    wp_edits_anon = monthly_edits_type_what[edit_type]['wikipedia']['anonymous']
                    admin_pages_edits_anon = monthly_edits_type_what[edit_type]['admin_page']['anonymous']

                    all_wp_edits = wp_edits_registered+wp_edits_anon
                    all_admin_pages_edits = admin_pages_edits_registered+admin_pages_edits_anon

                    insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'admin pages', '', admin_pages_edits_registered, wp_edits_registered, last_year_month)

                    insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'admin pages', '', admin_pages_edits_anon, wp_edits_anon, last_year_month)

                    insert_stats_values('monthly increment', cursor2, edit_type, 'all_editors', languagecode, 'wikipedia', 'admin pages', '', all_admin_pages_edits, all_wp_edits, last_year_month)

                    
                    for admin_page_type in admin_page_types: 
                        admin_page_type_registered_edits = monthly_edits_type_what[edit_type][admin_page_type]['registered']
                        admin_page_type_anon_edits = monthly_edits_type_what[edit_type][admin_page_type]['anonymous']

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_registered_edits, admin_pages_edits_registered, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_anon_edits, admin_pages_edits_anon, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_registered_edits+admin_page_type_anon_edits, all_admin_pages_edits, last_year_month)


                    for page_namespace in page_namespaces: 
                        page_namespace_registered_edits = monthly_edits_type_what[edit_type][page_namespace]['registered']
                        page_namespace_anon_edits = monthly_edits_type_what[edit_type][page_namespace]['anonymous']

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_registered_edits, wp_edits_registered, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_anon_edits, wp_edits_anon, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, 'registered', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_registered_edits+page_namespace_anon_edits, all_wp_edits, last_year_month)




                    # edits lustrum type
                    for editor_lustrum in editor_lustrums:
                        
                        wp_edits_lustrum = monthly_edits_type_what[edit_type]['wikipedia'][editor_lustrum]
                        admin_page_edits_lustrum = monthly_edits_type_what[edit_type]['admin_page'][editor_lustrum]
    
                        insert_stats_values('monthly increment', cursor2, edit_type, lustrum, languagecode, 'wikipedia', '', '', wp_edits_lustrum, wp_edits_registered, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, lustrum, languagecode, 'wikipedia', 'admin pages', '', admin_page_edits_lustrum, admin_pages_edits_registered, last_year_month)


                        for admin_page_type in admin_page_types:
                            admin_page_type_edits_lustrum = monthly_edits_type_what[edit_type][admin_page_type][editor_lustrum]
                            insert_stats_values('monthly increment', cursor2, edit_type, lustrum, languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_edits_lustrum, admin_pages_edits_registered, last_year_month)

                        for page_namespace in page_namespaces:
                            page_namespace_edits_lustrum = monthly_edits_type_what[edit_type][page_namespace][editor_lustrum]
                            insert_stats_values('monthly increment', cursor2, edit_type, lustrum, languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_edits_lustrum, wp_edits_registered, last_year_month)



                    # edits newcomers - newcomers non / admin - admin non
                    for editor_X in ['editor_newcomer_90d','editor_newcomer_1y','editor_newcomer_5y','editor_admin']:

                        # 90 days
                        wp_edits_X = monthly_edits_type_what[edit_type]['wikipedia'][editor_X]
                        wp_edits_X_none = wp_edits_registered - wp_edits_X

                        admin_pages_edits_X = monthly_edits_type_what[edit_type]['admin_page'][editor_X]
                        admin_pages_edits_X_none = admin_pages_edits_registered - admin_pages_edits_X

                        insert_stats_values('monthly increment', cursor2, edit_type, editor_X, languagecode, 'wikipedia', 'admin pages', '', wp_edits_X, wp_edits_registered, last_year_month)

                        insert_stats_values('monthly increment', cursor2, edit_type, editor_X+'_none', languagecode, 'wikipedia', 'admin pages', '', wp_edits_X_none, wp_edits_registered, last_year_month)

                        for admin_page_type in admin_page_types: 
                            admin_page_type_edits_X = monthly_edits_type_what[edit_type][admin_page_type][editor_X]
                            insert_stats_values('monthly increment', cursor2, edit_type, editor_X, languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_edits_X, wp_edits_registered, last_year_month)

                            admin_page_type_registered_edits = monthly_edits_type_what[edit_type][admin_page_type]['registered']
                            admin_page_type_edits_X_none = admin_page_type_registered_edits - admin_page_type_edits_X

                            insert_stats_values('monthly increment', cursor2, edit_type, editor_X+'_none', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_edits_X_none, wp_edits_registered, last_year_month)

                        for page_namespace in page_namespaces: 
                            page_namespace_edits_X = monthly_edits_type_what[edit_type][page_namespace][editor_X]


                            insert_stats_values('monthly increment', cursor2, edit_type, editor_X, languagecode, 'wikipedia', 'namespaces', page_namespace, admin_page_type_edits_X, wp_edits_registered, last_year_month)

                            page_namespace_registered_edits = monthly_edits_type_what[edit_type][page_namespace]['registered']
                            page_namespace_edits_X_none = page_namespace_registered_edits - page_namespace_edits_X

                            insert_stats_values('monthly increment', cursor2, edit_type, editor_X+'_none', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_edits_X_none, wp_edits_registered, last_year_month)





                # editors
                wp_editors_registered = len(monthly_editors_type_what['wikipedia'])
                admin_pages_editors_registered = len(monthly_editors_type_what['admin_page'])

                insert_stats_values('monthly increment', cursor2, 'editors', 'registered', languagecode, 'wikipedia', 'admin pages', '', admin_pages_editors_registered, wp_editors_registered, last_year_month)


                for admin_page_type in admin_page_types: 
                    admin_page_type_registered_editors = len(monthly_editors_type_what[admin_page_type])

                    insert_stats_values('monthly increment', cursor2, 'editors', 'registered', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_registered_editors, admin_pages_editors_registered, last_year_month)

                for page_namespace in page_namespaces: 
                    page_namespace_registered_editors = monthly_edits_type_what['editors'][page_namespace]['registered']

                    insert_stats_values('monthly increment', cursor2, 'editors', 'registered', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_registered_editors, wp_editors_registered, last_year_month)


                # edits lustrum type
                for editor_lustrum in editor_lustrums:
                    
                    wp_editors_lustrum = len(monthly_editors_type_what['wikipedia'][editor_lustrum])
                    admin_page_editors_lustrum = len(monthly_editors_type_what['admin_page'][editor_lustrum])

                    insert_stats_values('monthly increment', cursor2, 'editors', lustrum, languagecode, 'wikipedia', '', '', wp_editors_lustrum, wp_editors_registered, last_year_month)

                    insert_stats_values('monthly increment', cursor2, 'editors', lustrum, languagecode, 'wikipedia', 'admin pages', '', admin_page_editors_lustrum, admin_pages_editors_registered, last_year_month)


                    for admin_page_type in admin_page_types:
                        admin_page_type_editors_lustrum = monthly_edits_type_what['editors'][admin_page_type][editor_lustrum]
                        insert_stats_values('monthly increment', cursor2, 'editors', lustrum, languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_editors_lustrum, admin_pages_editors_registered, last_year_month)

                    for page_namespace in page_namespaces:
                        page_namespace_editors_lustrum = monthly_edits_type_what['editors'][page_namespace][editor_lustrum]
                        insert_stats_values('monthly increment', cursor2, 'editors', lustrum, languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_editors_lustrum, wp_editors_registered, last_year_month)



                # edits newcomers - newcomers non / admin - admin non
                for editor_X in ['editor_newcomer_90d','editor_newcomer_1y','editor_newcomer_5y','editor_admin']:

                    # 90 days
                    wp_editors_X = monthly_editors_type_what['editors']['wikipedia'][editor_X]
                    wp_editors_X_none = wp_editors_registered - wp_editors_X

                    admin_pages_editors_X = monthly_editors_type_what['editors']['admin_page'][editor_X]
                    admin_pages_editors_X_none = admin_pages_editors_registered - admin_pages_editors_X

                    insert_stats_values('monthly increment', cursor2, 'editors', editor_X, languagecode, 'wikipedia', 'admin pages', '', wp_editors_X, wp_editors_registered, last_year_month)

                    insert_stats_values('monthly increment', cursor2, 'editors', editor_X+'_none', languagecode, 'wikipedia', 'admin pages', '', wp_editors_X_none, wp_editors_registered, last_year_month)

                    for admin_page_type in admin_page_types: 
                        admin_page_type_editors_X = monthly_editors_type_what['editors'][admin_page_type][editor_X]
                        insert_stats_values('monthly increment', cursor2, 'editors', editor_X, languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_editors_X, wp_editors_registered, last_year_month)

                        admin_page_type_registered_editors = monthly_editors_type_what['editors'][admin_page_type]['registered']
                        admin_page_type_editors_X_none = admin_page_type_registered_editors - admin_page_type_editors_X

                        insert_stats_values('monthly increment', cursor2, 'editors', editor_X+'_none', languagecode, 'wikipedia', 'admin pages', admin_page_type, admin_page_type_editors_X_none, wp_editors_registered, last_year_month)

                    for page_namespace in page_namespaces: 
                        page_namespace_editors_X = monthly_editors_type_what['editors'][page_namespace][editor_X]


                        insert_stats_values('monthly increment', cursor2, 'editors', editor_X, languagecode, 'wikipedia', 'namespaces', page_namespace, admin_page_type_editors_X, wp_editors_registered, last_year_month)

                        page_namespace_registered_editors = monthly_editors_type_what['editors'][page_namespace]['registered']
                        page_namespace_editors_X_none = page_namespace_registered_editors - page_namespace_editors_X

                        insert_stats_values('monthly increment', cursor2, 'editors', editor_X+'_none', languagecode, 'wikipedia', 'namespaces', page_namespace, page_namespace_editors_X_none, wp_editors_registered, last_year_month)



                ### CREATE STATS DICTIONARIES / INITIALIZE (Stats.DB)
                # edits --> comparing edits by editors
                # Initialize dictionaries
                monthly_edits_type_what = {}
                for x in types_of_edits:
                    monthly_edits_type_what[x] = {} # monthly_edits_where = {}
                    for y in types_of_content: 
                        monthly_edits_type_what[x][y] = {} # monthly_edits_whom = {}
                        for z in types_of_editor: 
                            monthly_edits_type_what[x][y][z] = 0

                # editors --> comparing editors by edits
                monthly_editors_type_what = {}
                for x in types_of_content:
                    monthly_editors_type_what[x] = {} # monthly_editors_where = {}
                    for y in types_of_editor:
                        monthly_editors_type_what[x][y] = set()
                


                """

                # END OF "END OF MONTH CYCLE"
            last_year_month = current_year_month





        # END OF THE DUMP
        # HERE WE INSERT THE DATA FOR THE EDITORS AND COMPLETED ARTICLES.
        query = 'INSERT OR IGNORE INTO editors (editor, page_id) VALUES (?,?);'
        cursor2.executemany(query,editors_params)
        conn2.commit()
        editors_params = []



        editor_history_params = []
        for user_id, data in editor_history.items():
            # print (data)
            editor_history_params.append(data)

        query = 'INSERT OR IGNORE INTO editors_history (editor, edit_count, year_first_edit, flag) VALUES (?,?,?,?);'
        cursor2.executemany(query,reversed(editor_history_params))
        conn2.commit()
        editor_history_params = []
        editor_history = {}

        parameters = []
        parameters_qitems = []


        print ('time to insert:')
        print (len(article_completed))
        for page_id in article_completed.keys():

            if num_edits_last_month[page_id] > num_edits[page_id]: 
                num_edits[page_id] = num_edits_last_month[page_id]

            try:
                qitem = page_ids_qitems[page_id]
            except:
                print ('pass')
                continue


            first_edit_timestamp_obj = datetime.datetime.strptime(str(first_edit_timestamp[page_id]),'%Y%m%d%H%M%S')
            last_edit_timestamp_obj = datetime.datetime.strptime(str(last_edit_timestamp[page_id]),'%Y%m%d%H%M%S')
            total_days = abs((last_day_last_month - first_edit_timestamp_obj).days)


            if total_days == 0: 
                total_days = 1

            edt_days = editing_days[page_id]
            percent_editing_days = 100*edt_days/total_days

            # print (total_days)
            # print (first_edit_timestamp_obj, last_day_last_month, last_edit_timestamp_obj, abs((last_day_last_month - first_edit_timestamp_obj).days), edt_days, percent_editing_days)
            # if abs((last_day_last_month - first_edit_timestamp_obj).days) == 0:
            #     input('')

            total_months = (last_day_last_month.year - first_edit_timestamp_obj.year) * 12 + (last_day_last_month.month - first_edit_timestamp_obj.month)
            if total_months == 0: total_months = 1

            act_months = active_months[page_id]
            percent_active_months = 100*act_months/total_months

            # print (page_id, total_days, edt_days, percent_editing_days, total_months, act_months, percent_active_months)


            list_timestamps = sorted(edit_count_manual[page_id])
            edt_c_manual = len(list_timestamps)

            # if num_edits_manual>5:
            #     print (list_timestamps)
            #     input('')
            days_last_50_edits = None
            days_last_5_edits = None
            days_last_edit = None

            if edt_c_manual >= 50:  

                fifty_obj = list_timestamps[len(list_timestamps)-50]
                days = abs((last_day_last_month - fifty_obj).days)
                days_last_50_edits=days

            if edt_c_manual >= 5:

                five_obj = list_timestamps[len(list_timestamps)-5]
                days = abs((last_day_last_month - five_obj).days)
                days_last_5_edits=days

            if edt_c_manual >= 1:

                last_obj = list_timestamps[len(list_timestamps)-1]
                days = abs((last_day_last_month - last_obj).days)
                days_last_edit=days


            try:


                if qitem == None:
                    parameters.append((num_edits[page_id], num_discussions[page_id], num_edits_last_month[page_id],num_edits_last_month_by_admin[page_id], num_edits_last_month_by_anonymous[page_id], num_edits_last_month_by_newcomer_90d[page_id], num_edits_last_month_by_newcomer_1y[page_id], num_edits_last_month_by_newcomer_5y[page_id], first_edit_timestamp[page_id], last_edit_timestamp[page_id], last_discussion_timestamp[page_id], num_anonymous_edits[page_id], num_bots_edits[page_id], num_reverts[page_id], total_months, active_months[page_id], max_active_months_row[page_id], max_inactive_months_row[page_id], percent_active_months, editing_days[page_id], percent_editing_days, days_last_50_edits, days_last_5_edits, days_last_edit, page_id))
                else:
                    parameters_qitems.append((num_edits[page_id], num_discussions[page_id], num_edits_last_month[page_id],num_edits_last_month_by_admin[page_id], num_edits_last_month_by_anonymous[page_id], num_edits_last_month_by_newcomer_90d[page_id], num_edits_last_month_by_newcomer_1y[page_id], num_edits_last_month_by_newcomer_5y[page_id], first_edit_timestamp[page_id], last_edit_timestamp[page_id], last_discussion_timestamp[page_id], num_anonymous_edits[page_id], num_bots_edits[page_id], num_reverts[page_id], total_months, active_months[page_id], max_active_months_row[page_id], max_inactive_months_row[page_id], percent_active_months, editing_days[page_id], percent_editing_days, days_last_50_edits, days_last_5_edits, days_last_edit, page_id, qitem))

#                print ((num_edits[page_id], num_discussions[page_id], num_edits_last_month[page_id],num_edits_last_month_by_admin[page_id], num_edits_last_month_by_anonymous[page_id], num_edits_last_month_by_newcomer_90d[page_id], num_edits_last_month_by_newcomer_1y[page_id], num_edits_last_month_by_newcomer_5y[page_id], first_edit_timestamp[page_id], last_edit_timestamp[page_id], last_discussion_timestamp[page_id], num_anonymous_edits[page_id], num_bots_edits[page_id], num_reverts[page_id], active_months[page_id], max_active_months_row[page_id], max_inactive_months_row[page_id], percent_active_months, editing_days[page_id], percent_editing_days, days_last_50_edits[page_id], days_last_5_edits[page_id], days_last_edit[page_id], page_id))


                # del num_editors[page_title]
                del num_edits[page_id]
                del num_discussions[page_id]
                del num_edits_last_month[page_id]

                del num_edits_last_month_by_admin[page_id]
                del num_edits_last_month_by_anonymous[page_id]
                del num_edits_last_month_by_newcomer_90d[page_id]
                del num_edits_last_month_by_newcomer_1y[page_id]
                del num_edits_last_month_by_newcomer_5y[page_id]

                del first_edit_timestamp[page_id]
                del last_edit_timestamp[page_id]
                del last_discussion_timestamp[page_id]

                del num_anonymous_edits[page_id]
                del num_bots_edits[page_id]
                del num_reverts[page_id]


                del active_months[page_id]
                del cur_active_months_row[page_id]
                del max_active_months_row[page_id]
                del max_inactive_months_row[page_id]

                del editing_days[page_id]

                del days_last_50_edits[page_id]
                del days_last_5_edits[page_id]
                del days_last_edit[page_id]

                del edit_count_manual[page_id]

            except:
                pass

        article_completed = {}

        conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()


        query = 'UPDATE '+languagecode+'wiki_pages SET num_edits = ?, num_discussions = ?, num_edits_last_month = ?, num_edits_last_month_by_admin = ?, num_edits_last_month_by_anonymous = ?, num_edits_last_month_by_newcomer_90d = ?, num_edits_last_month_by_newcomer_1y = ?, num_edits_last_month_by_newcomer_5y = ?, date_created = ?, date_last_edit = ?, date_last_discussion = ?, num_anonymous_edits = ?, num_bot_edits = ?, num_reverts = ?, total_months = ?, active_months = ?, max_active_months_row = ?, max_inactive_months_row = ?, percent_active_months = ?, editing_days = ?, percent_editing_days = ?, days_last_50_edits = ?, days_last_5_edits = ?, days_last_edit = ? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,parameters_qitems)
        conn.commit()

        query = 'UPDATE '+languagecode+'wiki_pages SET num_edits = ?, num_discussions = ?, num_edits_last_month = ?, num_edits_last_month_by_admin = ?, num_edits_last_month_by_anonymous = ?, num_edits_last_month_by_newcomer_90d = ?, num_edits_last_month_by_newcomer_1y = ?, num_edits_last_month_by_newcomer_5y = ?, date_created = ?, date_last_edit = ?, date_last_discussion = ?, num_anonymous_edits = ?, num_bot_edits = ?, num_reverts = ?, total_months = ?, active_months = ?, max_active_months_row = ?, max_inactive_months_row = ?, percent_active_months = ?, editing_days = ?, percent_editing_days = ?, days_last_50_edits = ?, days_last_5_edits = ?, days_last_edit = ? WHERE page_id = ? AND qitem IS NULL;'
        cursor.executemany(query,parameters)
        conn.commit()



        print ('Articles introduced: '+str(len(parameters)+len(parameters_qitems))+'. Articles left for next files or not in the database: '+str(len(num_edits))+'.')
        print ('This file took: '+str(datetime.timedelta(seconds=time.time() - iterTime)))
        parameters = []
        parameters_qitems = []
        conn.commit()

        # AFTER THIS, WE START A NEW DUMP.



    # IF IT GETS TOO BIG, WE COULD CLEAN THE TABLE WITH THE PAGES ALREADY INSERTED.
    parameters = []
    parameters_qitems = []
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + languagecode+'wiki_editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT count(*), page_id FROM editors GROUP BY page_id ORDER BY page_id;'
    for row in cursor2.execute(query):
        try:
            value = row[0]
            page_id = row[1]

            # if page_id == '1732' or page_id == 1732:
            #     print (row)
            #     input('')

            qitem = page_ids_qitems[page_id]
            if qitem == None:
                parameters.append((value,page_id))
            else:
                parameters_qitems.append((value,page_id,qitem))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki_pages SET num_editors = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters_qitems)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_editors = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor.executemany(query, parameters)
    conn.commit()


    # median human editor edits
    parameters = []
    parameters_qitems = []
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + languagecode+'wiki_editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT editors_history.edit_count, page_id FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor ORDER BY page_id;'

    count_values = []
    old_page_id = ''
    for row in cursor2.execute(query):
        page_id = row[1]

        if page_id != old_page_id and old_page_id != '':

            qitem = page_ids_qitems[old_page_id]
            if qitem == None:
                try:
                    parameters.append((median(count_values),old_page_id))
                except:
                    pass
            else:
                try:
                    parameters_qitems.append((median(count_values),old_page_id,qitem))
                except:
                    pass

            count_values = []

        try:
            count_values.append(int(row[0]))
        except:
            pass

        old_page_id = page_id


    # last round
    qitem = page_ids_qitems[old_page_id]
    if qitem == None:
        try:
            parameters.append((median(count_values),old_page_id))
        except:
            pass
    else:
        try:
            parameters_qitems.append((median(count_values),old_page_id,qitem))
        except:
            pass
    count_values = []


    query = 'UPDATE '+languagecode+'wiki_pages SET median_editors_edits = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor.executemany(query, parameters)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET median_editors_edits = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters_qitems)
    conn.commit()



    # median year first edit
    parameters = []
    parameters_qitems = []
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + languagecode+'wiki_editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT editors_history.year_first_edit, page_id FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor ORDER BY page_id;'

    year_values = []
    old_page_id = ''
    for row in cursor2.execute(query):
        page_id = row[1]

        if page_id != old_page_id and old_page_id != '':
            qitem = page_ids_qitems[old_page_id]

            if qitem == None:
                try:
                    parameters.append((median(year_values),old_page_id))
                except:
                    pass
            else:
                try:
                    parameters_qitems.append((median(year_values),old_page_id,qitem))
                except:
                    pass
            year_values = []

        try:
            year_values.append(int(row[0]))
        except:
            pass
        old_page_id = page_id

    # last round
    qitem = page_ids_qitems[old_page_id]
    if qitem == None:
        try:
            parameters.append((median(year_values),page_id))
        except:
            pass
    else:
        try:
            parameters_qitems.append((median(year_values),page_id,qitem))
        except:
            pass
    year_values = []


    query = 'UPDATE '+languagecode+'wiki_pages SET median_year_first_edit = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor.executemany(query, parameters)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET median_year_first_edit = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters_qitems)
    conn.commit()




    # number of admin editors
    parameters = []
    parameters_qitems = []    
    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + languagecode+'wiki_editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT count(*), page_id, flag FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor WHERE editors_history.flag != "" GROUP BY page_id ORDER BY page_id;'
    for row in cursor2.execute(query):
        page_id = row[1]
        flag = row[2]

        for x in ["sysop","bureaucrat","oversight","checkuser","steward"]:
            if x in flag:

                qitem = page_ids_qitems[page_id]
                if qitem == None:
                    try:
                        parameters.append((row[0],page_id))
                    except:
                        pass   
                else:
                    try:
                        parameters_qitems.append((row[0],page_id,qitem))
                    except:
                        pass                       
        # ("sysop","bureaucrat","oversight","checkuser","steward") 

    query = 'UPDATE '+languagecode+'wiki_pages SET num_admin_editors = ? WHERE page_id = ? AND qitem IS NULL;'
    cursor.executemany(query, parameters)
    conn.commit()
    query = 'UPDATE '+languagecode+'wiki_pages SET num_admin_editors = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters_qitems)
    conn.commit()

    os.remove(databases_path+languagecode+'wiki_editors_pages.db')
    print(languagecode+' history parsed and stored')
    parameters = []
    parameters_qitems = []    





    """

    # STATS 
    # final part. computing extra stats.
    print(languagecode+' extra stats.')

    query = 'SELECT admin_page_type, count(*) FROM '+languagecode+'wiki_pages GROUP BY 1;'
    admin_pages_type_articles = {}

    sum_articles = 0
    for row in cursor.execute(query):
        art = row[1]
        admin_pages_type_edits[row[0]]=art
        sum_articles += art

    for admin_page_type, edits in admin_pages_type_edits.items():
        insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages','admin_pages',admin_page_type,edits, sum_art, cycle_year_month)


    query = 'SELECT admin_page_type, SUM(num_edits) FROM '+languagecode+'wiki_pages GROUP BY 1;'
    admin_pages_type_edits = {}

    sum_edits = 0
    for row in cursor.execute(query):
        pv = row[1]
        admin_pages_type_edits[row[0]]=pv
        sum_edits += pv

    for admin_page_type, edits in admin_pages_type_edits.items():
        insert_stats_values(time_range,cursor2,'edits',None,languagecode,'admin_pages','admin_pages',admin_page_type,edits, sum_pv, cycle_year_month)


    query = 'SELECT admin_page_type, SUM(num_discussion_edits) FROM '+languagecode+'wiki_pages GROUP BY 1;'
    admin_pages_type_discussion_edits = {}

    sum_discussion_edits = 0
    for row in cursor.execute(query):
        discussion_edits = row[1]
        admin_pages_type_discussion_edits[row[0]]=discussion_edits
        sum_discussion_edits += discussion_edits

    for admin_page_type, discussion_edits in admin_pages_type_discussion_edits.items():
        insert_stats_values(time_range,cursor2,'discussion_edits',None,languagecode,'admin_pages','admin_pages',admin_page_type,discussion_edits, sum_discussion_edits, cycle_year_month)


    query = 'SELECT admin_page_type, SUM(num_reverts) FROM '+languagecode+'wiki_pages GROUP BY 1;'
    admin_pages_type_reverts = {}

    sum_reverts = 0
    for row in cursor.execute(query):
        reverts = row[1]
        admin_pages_type_reverts[row[0]]=reverts
        sum_reverts += reverts

    for admin_page_type, reverts in admin_pages_type_reverts.items():
        insert_stats_values(time_range,cursor2,'reverts',None,languagecode,'admin_pages','admin_pages',admin_page_type,reverts, sum_reverts, cycle_year_month)

    

    # zero edits last month
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE admin_page_type IS NOT NULL;'
    cursor.execute(query)
    all_articles_count = cursor.fetchone()[0]

    query = 'SELECT admin_page_type, COUNT(*) FROM '+languagecode+'wiki_pages WHERE num_edits_last_month = 0 GROUP BY 1;'
    admin_pages_type_zero_edits_last_month = {}
    count_articles = 0
    for row in cursor.execute(query):
        count = row[1]
        admin_pages_type_zero_Inlinks[row[0]]=count
        count_articles += count
    
    for admin_page_type, count in admin_pages_type_zero_edits_last_month.items():
        insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_edits_last_month', count, count_articles, cycle_year_month)

    insert_stats_values(time_range,cursor2,'articles',None,languagecode,'admin_pages',admin_page_type,'zero_edits_last_month', count_articles, all_articles_count, cycle_year_month)



    print ('stats were introduced.')
    print ('end of extend_editing_history')
    """

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




"""

df extend_editing_history_cumulative():

    # SELECT set1, set1descriptor, set2, set2descriptor, period, abs_value, rel_value, 100*abs_value/rel_value AS base, SUM(100*abs_value/rel_value) OVER (rows unbounded preceding) AS new_base, SUM(abs_value) OVER (rows unbounded preceding) AS new_abs_value FROM wdo_intersections_monthly WHERE set1 = 'en' and set1descriptor='wp' and set2='en' and set2descriptor ='ccc' ORDER BY period ASC;

    # potser faig la query directament a l'app.

    # potser aquí només hi poso el last, que computo directament. igual com faig amb el pageviews... en aquest cas, esborraria això i computaria el last_accumulated dels tipus a 
    # assign_types_of_admin.


        query = ('CREATE table if not exists wapa_incremental ('+
    'content text not null, '+
    'editor text not null, '+

    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')


    query = ('CREATE table if not exists wapa_cumulative ('+
    'content text not null, '+
    'editor text not null, '+

    'set1 text not null, '+
    'set1descriptor not null, '+

    'set2 not null, '+
    'set2descriptor not null, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

"""


def extend_first_timestamp_lang():
    function_name = 'extend_first_timestamp_lang'
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()
    
    lang_qitems_is_first_timestamp = {}
    qitems_timestamp_lang = {}
    for languagecode in wikilanguagecodes:
        print (languagecode)
        lang_qitems_is_first_timestamp[languagecode]=[]

        query = 'SELECT qitem, date_created, page_id FROM '+languagecode+'wiki_pages;'
        for row in cursor.execute(query):
            qitem = row[0]
            first_timestamp = str(row[1])
            if first_timestamp == 'None': continue
            first_timestamp = datetime.datetime.strptime(first_timestamp[:len(first_timestamp)-2],'%Y%m%d%H%M')
            page_id = row[2]

            try:
                lang_timestamp = qitems_timestamp_lang[qitem]
                stored_timestamp = lang_timestamp[0]

                if stored_timestamp > first_timestamp:
                    qitems_timestamp_lang[qitem] = [first_timestamp,languagecode,page_id]

            except:
                qitems_timestamp_lang[qitem] = [first_timestamp,languagecode,page_id]


    parameters = []
    for qitem,lang_timestamp in qitems_timestamp_lang.items():
        parameters.append((str(lang_timestamp[1]),str(qitem)))#,lang_timestamp[2]))


    print (len(parameters))
    print ('now introducing...')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        query = 'UPDATE '+languagecode+'wiki_pages SET first_timestamp_lang = ? WHERE qitem = ?'# AND page_id = ?;'
        cursor.executemany(query,parameters)
        conn.commit()

        query = 'UPDATE '+languagecode+'wiki_pages SET first_timestamp_lang = "'+languagecode+'" WHERE first_timestamp_lang is null;'
        cursor.execute(query)
        conn.commit()




    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def insert_stats_values(time_range, cursor2, content, editor, set1, set1descriptor, set2, set2descriptor, abs_value, base, period):

    if time_range == 'monthly increment' or  time_range == 'last month':
        table_value = 'incremental'
    else:
        table_value = 'cumulative'    

    if abs_value == None: abs_value = 0

    if base == None or base == 0: rel_value = 0
    else: rel_value = 100*abs_value/base

    if 'avg' in set1 or 'avg' in set2: rel_value = base # exception for calculations in generate_langs_ccc_intersections()

    if rel_value != 0.0 or abs_value != 0:
        values = (abs_value, rel_value, content, editor, set1, set1descriptor, set2, set2descriptor, period)

        query_insert = 'INSERT OR IGNORE INTO wapa_'+table_value+' (abs_value, rel_value, content, editor, set1, set1descriptor, set2, set2descriptor, period) VALUES (?,?,?,?,?,?,?,?,?)'
        cursor2.execute(query_insert,values);

        query_update = 'UPDATE wapa_'+table_value+' SET abs_value = ?, rel_value = ? WHERE content = ? AND editor = ? AND set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ? AND period = ?'
        cursor2.execute(query_update,values);




# def stats():
#     print('wikipedia_administrative_pages_analytics_index')

# # function    running set12   set1descriptor  set2    set2descriptor  content app_use
# # stats (new function)    A, C    languagecode    wp  languagecode    admin_pages articles    categorization, over time
# # stats   A, C    languagecode    admin_pages admin_pages types   articles    categorization, over time
# # stats   A, C    languagecode    admin_pages admin_pages types   categories  categorization, over time
# # stats   B   languagecode    admin_pages admin_pages types   pageviews   categorization
# # stats   B   languagecode    admin_pages languagecode    zero_ill    articles    categorization
# # stats   B   languagecode    admin_pages languagecode    zero_inlinks    articles    categorization



def export_data_tsv_for_analysis():
    functionstartTime = time.time()

    wikilanguagecodes_ordered_by_number_categories, language_admin_categories = retrieve_admin_categories_local()

    # admin_categories = language_admin_categories['en']
    # admin_categories_all = []
    # for x in admin_categories.values():
    #     admin_categories_all.append(x[0].replace(' ','_')+'_level')

    all_admin_categories = ['Stubs_level', 'Wikipedia_essays_level', 'Wikipedia_policies_and_guidelines_level', 'Wikipedia_help_level', 'Wikipedia_deletion_level', 'Wikipedia_copyright_level', 'Wikipedia_maintenance_level', 'WikiProjects_level', 'Wikipedia_tools_level', 'Wikipedia_disclaimers_level', 'Wikipedia_village_pump_level'] # DO NOT USE!


    admin_categories_all = ['Wikipedia_essays_level', 'Wikipedia_policies_and_guidelines_level', 'Wikipedia_help_level']


    instance_of_all = ['instance_of_Wikimedia_project_policies_guidelines', 'instance_of_Wikimedia_help_page', 'instance_of_Wikimedia_wikiproject', 'instance_of_Wikimedia_wikimedia_portal']


    important_features = ['num_pageviews', 'num_interwiki']


    admin_categories_instance_of_all = admin_categories_all + instance_of_all 
    try:
        os.remove(databases_path+'admin_pages_tableau.tsv')
    except:
        pass


    m = 0
    for languagecode in ['ca','en','it','ja','de']:


        columns = ['langwiki', 'qitem', 'page_title', 'page_id', 'page_namespace']+all_admin_categories+instance_of_all+important_features+['all_categories_unified','all_categories_unified_4levelmax','all_instance_of_unified','all_categories_instance_of_unified','choosen_category','choosen_category_no_help_policies','category_count','instance_of_count','category_instance_of_count','categories_smallest_to_largest']

        admin_categories = language_admin_categories[languagecode]
        admin_categories_string = ''
        existing_categories = []
        for x in admin_categories.values():
            title = x[0].replace(' ','_')+'_level'
            existing_categories.append(title)
            admin_categories_string+=title+', '


#        admin_categories_string = admin_categories_string[:len(admin_categories_string)-1]
        # print (admin_categories_string)

        conn = sqlite3.connect(databases_path + wikipedia_administrative_pages_analytics_db); cursor = conn.cursor()


        # OBTAINING THE DATA
        query = 'SELECT "'+languagecode+'wiki" as langwiki, qitem, page_title, page_id, page_namespace, '+admin_categories_string+' instance_of_Wikimedia_project_page, instance_of_Wikimedia_internal_item, instance_of_Wikimedia_project_policies_guidelines, instance_of_Wikimedia_help_page, instance_of_Wikimedia_wikiproject, instance_of_Wikimedia_wikimedia_portal, num_pageviews, num_interwiki FROM '+languagecode+'wiki_pages;'
        print (query)
        # input('')


        # CREATE THE MISSING COLUMNS
        df1 = pd.read_sql_query(query, conn)
        for x in all_admin_categories:
            if x not in existing_categories:
                df1[x] = None

#        df1 = df1.set_index('page_id')


        # print (df.columns.tolist())
        # print (df.head(10))


        # CREATE NEW COLUMNS BASED ON CONDITIONS
        # * all_categories_unified / category_count
        all_categories_unified_dict = {}
        category_count_dict = {}
        for i, j in df1.iterrows():

            k = 0
            page_id = j['page_id']
            all_categories_unified = ''
            for x in admin_categories_all:
                value = j[x]
                if value is not None and value > 0: 
                    all_categories_unified+=x+';'
                    k+=1
            all_categories_unified = all_categories_unified[:len(all_categories_unified)-1]
            all_categories_unified_dict[page_id] = all_categories_unified
            category_count_dict[page_id] = k
        df1['all_categories_unified']=df1['page_id'].map(all_categories_unified_dict)
        df1['category_count']=df1['page_id'].map(category_count_dict)
        all_categories_unified_dict = {}
        category_count_dict = {}
        print ('* all_categories_unified / category_count')

        # * all_categories_unified 4 level max
        all_categories_unified_dict = {}
        for i, j in df1.iterrows():
            page_id = j['page_id']
            all_categories_unified = ''
            for x in admin_categories_all:
                value = j[x]
                if value is not None and value > 0 and value < 5: 
                    all_categories_unified+=x+';'
                    k+=1
            all_categories_unified = all_categories_unified[:len(all_categories_unified)-1]
            all_categories_unified_dict[page_id] = all_categories_unified
        df1['all_categories_unified_4levelmax']=df1['page_id'].map(all_categories_unified_dict)
        all_categories_unified_dict = {}
        print ('* all_categories_unified 4 level max')


        # * all_instance_of_unified / instance_of_count
        all_instance_of_unified_dict = {}
        instance_of_count_dict = {}
        for i, j in df1.iterrows():

            k = 0
            page_id = j['page_id']
            all_instance_of_unified = ''
            for x in instance_of_all:
                value = j[x]
                if value is not None and value > 0: 
                    all_instance_of_unified+=x+';'
                    k+=1
            all_instance_of_unified = all_instance_of_unified[:len(all_instance_of_unified)-1]
            all_instance_of_unified_dict[page_id] = all_instance_of_unified
            instance_of_count_dict[page_id] = k
        df1['all_instance_of_unified']=df1['page_id'].map(all_instance_of_unified_dict)
        df1['instance_of_count']=df1['page_id'].map(instance_of_count_dict)
        all_instance_of_unified_dict = {}
        instance_of_count_dict = {}
        print ('* all_instance_of_unified / instance_of_count')

        # * all_categories_instance_of_unified / category_instance_of_count
        all_categories_instance_of_unified_dict = {}
        categories_instance_of_count_dict = {}
        for i, j in df1.iterrows():

            k = 0
            page_id = j['page_id']
            all_categories_instance_of_unified = ''
            for x in admin_categories_instance_of_all:
                value = j[x]
                if value is not None and value > 0: 
                    all_categories_instance_of_unified+=x+';'
                    k+=1
            all_categories_instance_of_unified = all_categories_instance_of_unified[:len(all_categories_instance_of_unified)-1]
            all_categories_instance_of_unified_dict[page_id] = all_categories_instance_of_unified
            categories_instance_of_count_dict[page_id] = k
        df1['all_categories_instance_of_unified']=df1['page_id'].map(all_categories_instance_of_unified_dict)        
        df1['category_instance_of_count']=df1['page_id'].map(categories_instance_of_count_dict)
        all_categories_instance_of_unified_dict = {}
        categories_instance_of_count_dict = {}
        print ('* all_categories_instance_of_unified / category_instance_of_count')

        # * choosen_category
        choosen_category_dict = {}
        choosen_category_no_help_policies_dict = {}
        categories_smallest_to_largest_dict = {}
        for i, j in df1.iterrows():

            page_id = j['page_id']
            values_dict_except = {}
            values_dict = {}
            for x in admin_categories_all:
                value = j[x]
                if value is not None and value > 0: 
                    values_dict[x]=value
                    if x not in ['Wikipedia_policies_and_guidelines_level','Wikipedia_help_level']:
                        values_dict_except[x]=value

            try: min_cat = min(values_dict_except, key=values_dict_except.get)
            except: min_cat = ''
            choosen_category_no_help_policies_dict[x]=min_cat
            try: min_cat = min(values_dict, key=values_dict.get)
            except: min_cat = ''
            choosen_category_dict[page_id] = min_cat

            values_dict_sorted = {k: v for k, v in sorted(values_dict.items(), key=lambda item: item[1])}
            categories_smallest_to_largest = ''
            for cat,level in values_dict_sorted.items():
                categories_smallest_to_largest+=cat+';'

            categories_smallest_to_largest = categories_smallest_to_largest[:len(categories_smallest_to_largest)-1]
            categories_smallest_to_largest_dict[page_id] = categories_smallest_to_largest

        df1['choosen_category']=df1['page_id'].map(choosen_category_dict)
        df1['choosen_category_no_help_policies']=df1['page_id'].map(choosen_category_dict)
        df1['categories_smallest_to_largest']=df1['page_id'].map(categories_smallest_to_largest_dict)
        choosen_category_dict = {}
        choosen_category_no_help_policies_dict = {}
        categories_smallest_to_largest_dict = {}
        print ('* choosen_category')



        # Getting to the end... 
        df1 = df1.fillna(0)
        df1 = df1.reset_index()

        # REORDER COLUMNS
        df1 = df1[columns]
        # df = df.reindex(sorted(df.columns), axis=1)


        # EXPORTING INTO TSV
        if m == 0:
            df1.to_csv(databases_path + 'admin_pages_tableau.tsv', sep='\t')
        else:
            df1.to_csv(databases_path + 'admin_pages_tableau.tsv', mode='a', header=False, sep='\t')


        m+=1
        print ('out.')
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        print (duration)




#######################################################################################

def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WDO - WIKIPEDIA ADMINISTRATIVE PAGES ANALYTICS ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/wikipedia_administrative_pages_analytics.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WDO - WIKIPEDIA ADMINISTRATIVE PAGES ANALYTICS ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue






#######################################################################################
class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("wikipedia_administrative_pages_analytics"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("wikipedia_administrative_pages_analytics"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass




### MAIN:
if __name__ == '__main__':
    startTime = time.time()

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'wikipedia_administrative_pages_analytics.py'


    # cycle_year_month = '2022-07'
    cycle_year_month = wikilanguages_utils.get_new_cycle_year_month() 
#    check_time_for_script_run(script_name, cycle_year_month)

    # Verify whether there is a new language or not
    wikilanguages_utils.extract_check_new_wiki_projects();

    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = sorted(languages.index.tolist())

    admin_categories = {'Q4582366':'Wikipedia policies and guidelines', 'Q4588883':'Wikipedia help', 'Q5324375':'Wikipedia maintenance', 'Q6192227':'Wikipedia tools', 'Q2954058':'Wikipedia essays', 'Q7216441':'Wikipedia disclaimers', 'Q4852393':'Wikipedia copyright', 'Q9118779':'Wikipedia village pump', 'Q4615845':'Wikipedia deletion', 'Q5492333':'WikiProjects', 'Q2944440':'Stubs'}


    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)


    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

#    if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()

    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))

#    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)
    

    print ('* Done with the WIKIPEDIA ADMINISTRATIVE PAGES ANALYTICS CYCLE completed successfuly after: ' + str(duration))
    wikilanguages_utils.finish_email(startTime,'wikipedia_administrative_pages_analytics.out', 'WIKIPEDIA ADMINISTRATIVE PAGES ANALYTICS')


