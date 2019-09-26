## NOTE - this has an over-reliance on using list indicies.
## Lists should really be reformated into dictionaries at a later date.

import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from pprint import pprint
from collections import Counter
from datetime import datetime

import calendar

import sys
import csv

import requests

csv.field_size_limit(sys.maxsize)

## Fetch the project tags from dataclips
url ="https://data.heroku.com/dataclips/rtlwjzxzdjksnvimkzrsitfaczcm.csv?access-token=308a0659-def6-4a36-b5aa-0c6577672cbd"
#url = "https://dataclips.heroku.com/gselhclcwirgagjzhpbxioglalbg.csv"

CC_CORE = ['rock-band', 'lost-in-space', 'ghostbusters', 'chatbot', 'paint-box', 'boat-race',
           'memory', 'dodgeball', 'brain-game', 'catch-the-dots', 'clone-wars', 'create-your-own-world',
           'cats', 'flower-generator', 'guess-the-flag', 'lineup', 'flappy-parrot', 'binary-hero',
           'happy-birthday', 'tell-a-story', 'wanted', 'recipe', 'mystery-letter', 'project-showcase',
           'build-a-robot', 'stickers', 'sunrise', 'linked-rooms', 'magazine', 'pixel-art',
           'about-me', 'rock-paper-scissors', 'turtle-race', 'team-chooser', 'colourful-creations', 'secret-messages',
           'modern-art', 'popular-pets', 'rpg', 'where-is-the-space-station', 'robo-trumps', 'codecraft']

CD_CORE = ['cd-beginner-html-css-sushi',
           'cd-intermediate-html-css-sushi',
           'cd-advanced-html-css-sushi',
           'cd-beginner-scratch-sushi',
           'cd-intermediate-scratch-sushi',
           'cd-advanced-scratch-sushi',
           'cd-beginner-wearables-sushi',
           'cd-intermediate-wearables-sushi'
           'cd-sebento-appinv-1',
           'cd-sebento-appinv-2',
           'cd-sebento-appinv-3',
           'cd-sebento-scratch-1',
           'cd-sebento-scratch-2',
           'cd-sebento-scratch-3',
           'cd-sebento-htmlcss-1',
           'cd-sebento-htmlcss-2',
           'cd-sebento-htmlcss-3']

def fetch_csv_projects():
    print('Fetching CSV of projects meta data from dataclips')
    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        csv_projects = [row for row in cr]

    return csv_projects


## Global MONTH
MONTH = ''

## GOOGLE APIs being used
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/spreadsheets']

## Discovery URI for APIs
ANALYTICS_DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
SHEETS_DISCOVERY_URI = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')

## Creds for APIs (should update to json at some point)
KEY_FILE_LOCATION = 'mycreds.p12'
SERVICE_ACCOUNT_EMAIL = 'id-018-analytics@ancient-sandbox-191211.iam.gserviceaccount.com'


def fetch_date_range():
    '''User enters Year and Month, return start and end dates as date time objects'''
    global MONTH
    '''Fetch year and month for analytics and get the YYYY,MM,DD date range
    Return a start and end date for the fiven month'''
    year = input('Enter the year you are interested in ')
    month = input('Enter the short name of the month ')
    MONTH = month
    start_date = datetime.strptime(month + ' ' + year, '%b %Y')
    days_in_month = calendar.monthrange(start_date.year, start_date.month)
    end_date = datetime.strptime(str(days_in_month[1]) + ' ' + month + ' ' + year , '%d %b %Y')
    print('Processing analytics from', start_date.strftime('%Y-%m-%d'), 'to', end_date.strftime('%Y-%m-%d'))
    return start_date, end_date


def initialize_apis():
    """Initializes analytics and sheets reporting object.
    Returns:
      analytics and sheets reporting service object.
    """

    credentials = ServiceAccountCredentials.from_p12_keyfile(
      SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)

    http = credentials.authorize(httplib2.Http())

    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=ANALYTICS_DISCOVERY_URI)
    sheets = build('sheets', 'v4', http=http, discoveryServiceUrl=SHEETS_DISCOVERY_URI)

    return analytics, sheets


def get_satisfaction_report(analytics, start_date, end_date):
    """
    Fetch raw satisfaction data from analytics
    """
    ## View ID for Analytics - https://ga-dev-tools.appspot.com/query-explorer/
    VIEW_ID = '157729614'
    start = start_date.strftime('%Y-%m-%d')
    end = end_date.strftime('%Y-%m-%d')
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                ## This report gets total views and views for first project page
                {
                    'viewId': VIEW_ID,
                    'pageSize': 10000,
                    'dateRanges': [{'startDate': start, 'endDate': end}],
                    'dimensions': [{'name': 'ga:eventCategory'},
                                   {'name': 'ga:eventAction'},
                                   {'name': 'ga:eventLabel'},],
                    'metrics': [{'expression': 'ga:totalEvents'}]
                }]
        }
    ).execute()

def build_clean_satisfaction(satisfaction_data):
    ## Grab the project dimesnions (names, satisfaction, language) and append the metrics to give a list of items such as ['balloon-pi-tay-popper', 'like', 'en', '2']
    all_data = [project['dimensions'] + [project['metrics'][0]['values'][0]] for project in satisfaction_data['reports'][0]['data']['rows']]
    all_events = {}
    ## Iterate over all data and add to a dictionary a dictionary with the project name as a key and the metric (like, dislike etc) along with the value of that metric
    for project in all_data:
        if project[0] not in all_events:
            all_events[project[0]] = {project[2]:{project[1]:project[3]}}
        elif project[2] not in all_events[project[0]]:
            all_events[project[0]][project[2]] = {project[1]:project[3]}
        else:
            all_events[project[0]][project[2]][project[1]] = project[3]
    return all_events

def get_analytics_report(analytics, start_date, end_date):
    ## View ID for Analytics - https://ga-dev-tools.appspot.com/query-explorer/
    VIEW_ID = '157729614'
    start = start_date.strftime('%Y-%m-%d')
    end = end_date.strftime('%Y-%m-%d')
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                ## This report gets total views and views for first project page
                {
                    'viewId': VIEW_ID,
                    'pageSize': 10000,
                    'dateRanges': [{'startDate': start, 'endDate': end}],
                    'metrics': [{'expression': 'ga:pageviews'},
                                {'expression': 'ga:uniquePageviews'},
                                {'expression': 'ga:avgTimeOnPage'},],
                    'dimensions': [{'name': 'ga:pagePathLevel2'},
                                   {'name': 'ga:pagePathLevel3'},

                    ],
                    "orderBys":[{
                        "fieldName": "ga:uniquePageviews",
                        "sortOrder": "DESCENDING"}],
                    'metricFilterClauses':[{
                        'filters':[
                            {'metricName': 'ga:pageviews',
                             'operator': 'GREATER_THAN',
                             'comparisonValue': '5'}]}]

                },
                ## This report gets data on page views after first page
                {
                    'viewId': VIEW_ID,
                    'pageSize': 10000,
                    'dateRanges': [{'startDate': start, 'endDate': end}],
                    'metrics': [{'expression': 'ga:pageviews'},
                                {'expression': 'ga:uniquePageviews'},
                                {'expression': 'ga:avgTimeOnPage'},],
                    'dimensions': [{'name': 'ga:pagePathLevel2'},
                                   {'name': 'ga:pagePathLevel3'},
                                   {'name': 'ga:pagePathLevel4'},
                    ],
                    "orderBys":[{
                        "fieldName": "ga:uniquePageviews",
                        "sortOrder": "DESCENDING"}],
                                        'metricFilterClauses':[{
                        'filters':[
                            {'metricName': 'ga:pageviews',
                             'operator': 'GREATER_THAN',
                             'comparisonValue': '5'}]}]
                }]
        }
    ).execute()


def read_sheets(sheets, range_name):
    '''Read a given Google sheet and name and return the values'''
    spreadsheetId = '1VdqfhNMM66rwBk7VsDoVWeLQbochGRf4S9BsQqH_9is'
    result = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheetId,
        range=range_name).execute()
    values = result.get('values', [])
    return values


def write_sheets(sheets, values, end):
    '''Write a 2d list to a google sheet. Use the month for the tab, unless as tring provided and then use that string name for the tab'''
    if type(end) == str:
        range_name = end
    else:
        range_name = calendar.month_name[end.month][0:3]
    '''Write to a specific range and tab, with values represented by a 2D list'''
    spreadsheetId = '1VdqfhNMM66rwBk7VsDoVWeLQbochGRf4S9BsQqH_9is'
    body = {'value_input_option': 'USER_ENTERED',
            'data': {'range' : range_name,
                     'values' : values}
            }

    result = sheets.spreadsheets().values().batchUpdate(
       spreadsheetId=spreadsheetId,
       body=body).execute() 

def make_projects_dict(csv_projects):
    '''Creates a dictionary using the repo name as a key. Values are a dictionary containing values for title, duration, version, listed and tags'''
    ## Get index of csv fields incase these change in the database in the future
    id_key = csv_projects[0].index('id')
    project_name = csv_projects[0].index('repository_name')
    title = csv_projects[0].index('name')
    duration = csv_projects[0].index('duration')
    version = csv_projects[0].index('version')
    listed = csv_projects[0].index('listed')
    tag_context = csv_projects[0].index('tag_context')
    tag_name = csv_projects[0].index('tag_name')
    seen_before = []
    projects_dict = {}
    ## Iterate over csv data and build the dictionary
    for project in csv_projects:
        ## If project not in the dictionary already, add an entry
        if project[id_key] not in seen_before:
            seen_before.append(project[id_key])
            projects_dict[project[project_name]] = {'title':project[title],
                                                    'duration':project[duration],
                                                    'version':project[version],
                                                    'listed':project[listed],
                                                    ## create a tag_context key with an empty list of values to store tags
                                                    project[tag_context]:[]}
            ## add the tag into the list
            projects_dict[project[project_name]][project[tag_context]].append(project[tag_name])
        ## If there's already that tag_context (hardware for instance) then append the new tag to the context list
        elif project[tag_context] in projects_dict[project[project_name]]:
            projects_dict[project[project_name]][project[tag_context]].append(project[tag_name])
        ## If the tag_context is not there already, create it's key with the tag name in a list as the value
        else:
            projects_dict[project[project_name]][project[tag_context]] = [project[tag_name]]
    return projects_dict

    
def process_analytics(start_date, end_date):
    analytics_data = get_analytics_report(analytics, start_date, end_date)
    parent_pages = analytics_data['reports'][0]['data']['rows']
    child_pages = analytics_data['reports'][1]['data']['rows']

    ## Get all parent pages and ignore mistyped urls such as /project/rockband as a dictionary
    all_pages_dict = {page['dimensions'][1][1:] : {'1': page['metrics'][0]['values']} for page in parent_pages if page['dimensions'][1][-1] != '/' and page['dimensions'][0] == '/projects/' }

    ## Get all child pages and store in dictionary
    for page in child_pages:
        ## Remove slashes from name and number
        page_name = page['dimensions'][1][1:-1]
        ## Get the page number if it's not 1, so not to overwrite parent
        if page['dimensions'][2][1:] != '1':
            page_number = page['dimensions'][2][1:]
        page_metrics = page['metrics'][0]['values']
        ## Add child pages data to dictionary
        if page_name in all_pages_dict.keys():
            all_pages_dict[page_name][page_number] = page_metrics


    return all_pages_dict


def compile_meta_analytics(projects_analytics, satisfaction_dict):
    '''Bring together analytics, projects meta data'''
    print('fetching analytics')
    print('fetching meta data')
    projects_meta = make_projects_dict(fetch_csv_projects())
    print('processing')
    
    ##Turn curriculum list into a dict
    for project in projects_meta:
        try:
            curric_data = projects_meta[project]['curriculum']
            curric_dict = {level_name[0:-2]:level_name[-1] for level_name in curric_data}

            curric_dict['level'] = curric_dict['']
            del curric_dict['']
            projects_meta[project]['curriculum'] = curric_dict
        except KeyError:

            projects_meta[project]['curriculum'] = {'manufacture':'0', 'community':'0', 'design':'0', 'programming':'0', 'phys-comp':'0'}

    ## Remove projects with no site_areas tag
    learning_projects = {project:meta for project, meta in projects_meta.items() if 'site_areas' in meta}
    ## Remove projects where site areas is not "projects"
    projects = {project:meta for project, meta in learning_projects.items() if meta['site_areas'][0] == 'projects'}
    ## Remove projects that are not listed
    projects = {project:meta for project, meta in projects.items() if meta['listed'] == 'true'}
    ## Add the analytics data for each project
    for project in projects.keys():
        if project in projects_analytics.keys():
            projects[project]['analytics'] = projects_analytics[project]
    for project in projects.keys():
        try:
            ## Add in missing key values if not present
            if 'like' not in satisfaction_dict[project]['en']:
                satisfaction_dict[project]['en']['like'] = '0'
            if 'dislike' not in satisfaction_dict[project]['en']:
                satisfaction_dict[project]['en']['dislike'] = '0'
            if 'ok' not in satisfaction_dict[project]['en']:
                satisfaction_dict[project]['en']['ok'] = '0'
            projects[project]['satisfaction'] = satisfaction_dict[project]['en']
        except KeyError:
            projects[project]['satisfaction'] = {'dislike': '0', 'like': '0', 'ok': '0'}
    return projects

def calculate_total_views(projects):
    ## Add zero rated analytics data if it's missing. This occurs if a project is published after date searched for
    for project in projects:
        if 'analytics' not in projects[project]:
            projects[project]['analytics'] = {'1':['0','0','0']}
    for project in projects:
        try:
            x = projects[project]['analytics']
        except:
            print(project)
    return sum([int(projects[project]['analytics']['1'][1]) for project in projects])


def calculate_total_ratings(projects):
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects])
    return likes + okays + dislikes


def calculate_average_satisfaction(projects):
    ##debug        
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects])
    try:
        return ((likes + okays)/sum([likes,okays,dislikes]) * 100)
    except ZeroDivisionError:
        return 0
    

def calculate_core_satisfaction(projects, core):
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects if project in core])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects if project in core])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects if project in core])
    try:
        return ((likes + okays)/sum([likes,okays,dislikes]) * 100)
    except ZeroDivisionError:
        return 0


def calculate_language_satisfaction(projects, language):
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects if 'technologies' in projects[project].keys() if language in projects[project]['technologies']])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects if 'technologies' in projects[project].keys() if language in projects[project]['technologies']])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects if 'technologies' in projects[project].keys() if language in projects[project]['technologies']])
    try:
        return ((likes + okays)/sum([likes,okays,dislikes]) * 100)
    except ZeroDivisionError:
        return 0
    
def calculate_curriculum_satisfaction(projects, domain):
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects if 'curriculum' in projects[project].keys() if int(projects[project]['curriculum'][domain]) > 0])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects if 'curriculum' in projects[project].keys() if int(projects[project]['curriculum'][domain]) > 0])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects if 'curriculum' in projects[project].keys() if int(projects[project]['curriculum'][domain]) > 0])
    try:
        return ((likes + okays)/sum([likes,okays,dislikes]) * 100)
    except ZeroDivisionError:
        return 0

def calculate_level_satisfaction(projects, level):
    likes = sum([int(projects[project]['satisfaction']['like']) for project in projects if 'curriculum' in projects[project].keys() if 'level' in projects[project]['curriculum'] if projects[project]['curriculum']['level'] == level])
    okays = sum([int(projects[project]['satisfaction']['ok']) for project in projects if 'curriculum' in projects[project].keys() if 'level' in projects[project]['curriculum'] if projects[project]['curriculum']['level'] == level])
    dislikes = sum([int(projects[project]['satisfaction']['dislike']) for project in projects if 'curriculum' in projects[project].keys() if 'level' in projects[project]['curriculum'] if projects[project]['curriculum']['level'] == level])    
    try:
        return ((likes + okays)/sum([likes,okays,dislikes]) * 100)
    except ZeroDivisionError:
        return 0

def calculate_top_five(projects):
    likes_dict = {project:int(projects[project]['satisfaction']['like']) for project in projects}
    counted = Counter(likes_dict)
    return counted.most_common(5)

def calculate_bottom_five(projects):
    likes_dict = {project:int(projects[project]['satisfaction']['dislike']) for project in projects}
    counted = Counter(likes_dict)
    return counted.most_common(5)

def build_sheet_data(projects):
    ## gather core data as list
    data = [MONTH]
    data.append(calculate_total_ratings(projects))
    data.append(calculate_average_satisfaction(projects))
    data.append(calculate_core_satisfaction(projects, CC_CORE))
    data.append(calculate_core_satisfaction(projects, CD_CORE))
    top_five = calculate_top_five(projects)
    data.extend([i for j in top_five for i in j])
    bottom_five = calculate_bottom_five(projects)
    data.extend([i for j in bottom_five for i in j])
    data.append(calculate_language_satisfaction(projects, 'scratch'))
    data.append(calculate_language_satisfaction(projects, 'python'))
    data.append(calculate_language_satisfaction(projects, 'html-css-javascript'))
    data.append(calculate_curriculum_satisfaction(projects, 'design'))
    data.append(calculate_curriculum_satisfaction(projects, 'programming'))
    data.append(calculate_curriculum_satisfaction(projects, 'phys-comp'))
    data.append(calculate_curriculum_satisfaction(projects, 'manufacture'))
    data.append(calculate_curriculum_satisfaction(projects, 'community'))
    data.append(calculate_level_satisfaction(projects, '1'))
    data.append(calculate_level_satisfaction(projects, '2'))
    data.append(calculate_level_satisfaction(projects, '3'))
    data.append(calculate_level_satisfaction(projects, '4'))
    return data

def add_to_sheet(sheets, range_name):
    existing_data = read_sheets(sheets, range_name)
    new_data = build_sheet_data(projects)
    for index, month in enumerate(existing_data):
        if month[0] == MONTH:
            existing_data[index] = new_data
        write_sheets(sheets, existing_data, range_name)
    


    
start, end = fetch_date_range()
analytics, sheets = initialize_apis()
projects_analytics = process_analytics(start, end)
satisfaction_data = get_satisfaction_report(analytics, start, end)
satisfaction_dict = build_clean_satisfaction(satisfaction_data)
projects = compile_meta_analytics(projects_analytics, satisfaction_dict)
add_to_sheet(sheets, 'Satisfaction')
