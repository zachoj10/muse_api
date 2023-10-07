import pandas as pd
import duckdb 
import requests 
import os
import argparse

BASE_URL = 'https://www.themuse.com/api/public/jobs?page={0}&api_key={1}'
DB_FILENAME = 'muse_jobs.duckdb'

api_key = os.environ['MUSE_API_KEY']

def connect_to_db(): 
    con = duckdb.connect(DB_FILENAME)

    return con 

def save_to_table(df, con, table_name): 
    con.execute(f"CREATE OR REPLACE TABLE { table_name } AS SELECT * FROM df")


def query_api(num_pages_to_query = None):
    num_pages_to_query = num_pages_to_query if num_pages_to_query is not None else float('inf')

    print(num_pages_to_query)

    running = True
    page_num = 0
    jobs_list = []
    companies = {}
    
    while running: 
        print(f'Querying page {page_num}')

        to_query = BASE_URL.format(page_num, api_key)

        response = requests.get(to_query)

        requests_remaining = response.headers.get('X-Ratelimit-Remaining')
        # request_reset_seconds = response.headers.get('X-Ratelimit-Reset')

        if response.status_code != 200: 
            print(f'Exiting API due to error. Response code: {response.status_code}')
            break

        output = response.json() 

        total_pages = output['page_count']

        results = output['results']

        for result in results:

            location_list = []
            category_list = []

            for location in result['locations']:
                location_list.append(location['name'])

            for category in result['categories']:
                category_list.append(category['name'])

            # print(result['levels'])

            company_object = {'company_id': result['company']['id'], 
                            'short_name': result['company']['short_name'], 
                            'name': result['company']['name']
            } 

            # print(result['levels']) ### list of level discts
            # print(result['refs']) ### dict ### TODO are there non_landingpage keys

            print(result['refs'].keys())

            job_object = {'job_id': result['id'], 
                        'description': result['contents'], 
                        'type': result['type'], 
                        'publication_date': result['publication_date'], 
                        'name': result['name'],
                        'short_name': result['short_name'], 
                        'model_type': result['model_type'],
                        'locations': location_list,
                        'categories': category_list,
                        'landing_page': result['refs']['landing_page'], 
                        'company_id': company_object['company_id'], 
                        'levels': result['levels']
            }

            jobs_list.append(job_object)
            companies[company_object['company_id']] = company_object

        page_num += 1

        if page_num > min(total_pages, num_pages_to_query):
            running = False

    con = connect_to_db()

    jobs = pd.DataFrame(jobs_list)
    company_df = pd.DataFrame(companies.values())


    save_to_table(jobs, con, 'jobs') 
    save_to_table(company_df, con, 'companies')


def main(pages_to_load): 

    query_api(pages_to_load)    

if __name__ == '__main__': 
    parser = argparse.ArgumentParser()
    parser.add_argument('--pages', metavar='P', type=int, nargs='?', required=False, action='store')

    pages_to_load = parser.parse_args().pages
    print(pages_to_load)
    main(pages_to_load) 