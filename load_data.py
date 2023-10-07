import pandas as pd
import duckdb
import requests
import os
import argparse
import time
import warnings

BASE_URL = "https://www.themuse.com/api/public/jobs?page={0}"
DB_FILENAME = "muse_jobs.duckdb"

API_KEY_ARG = "&api_key={0}"
api_key = os.environ.get("MUSE_API_KEY")


def connect_to_db():
    con = duckdb.connect(DB_FILENAME)

    return con


def save_to_table(df, con, table_name):
    con.execute(f"CREATE OR REPLACE TABLE { table_name } AS SELECT * FROM df")


def test_expected_response(field_name, job_id, data):
    if field_name == "refs":
        if list(data.keys()) != ["landing_page"]:
            warnings.warn(f"Job ID {job_id} has unexpected keys in the `refs` field")

    elif field_name == "levels":
        if len(data) > 1:
            warnings.warn(
                f"Job ID {job_id} has more than the expected number of values in the `levels` field"
            )


def query_api(num_pages_to_query=None):
    api_key_string = API_KEY_ARG.format(api_key)

    num_pages_to_query = (
        num_pages_to_query if num_pages_to_query is not None else float("inf")
    )

    print(f"Will query {num_pages_to_query} pages")

    running = True
    page_num = 0
    jobs_list = []
    companies = {}

    while running:
        print(f"Querying page {page_num}...")

        to_query = BASE_URL.format(page_num) + (
            api_key_string if api_key is not None else ""
        )

        response = requests.get(to_query)

        requests_remaining = response.headers.get("X-Ratelimit-Remaining")
        request_reset_seconds = response.headers.get("X-Ratelimit-Reset")

        if response.status_code != 200:
            print(f"Exiting API due to error. Response code: {response.status_code}")
            break

        output = response.json()

        total_pages = output["page_count"]

        results = output["results"]

        for result in results:
            location_list = []
            is_remote_eligible = False
            category_list = []

            for location in result["locations"]:
                ### Pop `Flexible / Remote` from the list of locations and set as a bool
                if location['name'] == 'Flexible / Remote':
                    is_remote_eligible = True
                    continue
                location_list.append(location["name"])

            for category in result["categories"]:
                category_list.append(category["name"])

            company_object = {
                "company_id": result["company"]["id"],
                "short_name": result["company"]["short_name"],
                "name": result["company"]["name"],
            }

            job_id = result["id"]

            ### Some expectations were made about the structure in the `refs` and `levels` fields 
            ### added tests to insure assumptions hold
            test_expected_response("refs", job_id, result["refs"])
            test_expected_response("levels", job_id, result["levels"])

            job_object = {
                "job_id": job_id,
                "description": result["contents"],
                "type": result["type"],
                "publication_at": result["publication_date"],
                "name": result["name"],
                "short_name": result["short_name"],
                "model_type": result["model_type"],
                "locations": location_list,
                'is_remote_eligible': is_remote_eligible,
                "categories": category_list,
                "landing_page": result["refs"]["landing_page"],
                "company_id": company_object["company_id"],
                "levels": result["levels"][0],
            }

            jobs_list.append(job_object)
            companies[company_object["company_id"]] = company_object

        page_num += 1

        if requests_remaining == 0:
            time.sleep(request_reset_seconds)

        if page_num >= min(total_pages, num_pages_to_query):
            running = False

    jobs = pd.DataFrame(jobs_list)
    company_df = pd.DataFrame(companies.values())

    return jobs, company_df


def main(pages_to_load):
    jobs, company_df = query_api(pages_to_load)

    con = connect_to_db()

    save_to_table(jobs, con, "jobs")
    save_to_table(company_df, con, "companies")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--pages",
        type=int,
        nargs="?",
        required=False,
        action="store",
        help="""The number of pages to query from the API. If omitted, the script 
                will continue to query until all available pages are exhausted. Note, 
                the script will always start with page 0""",
    )

    pages_to_load = parser.parse_args().pages
    main(pages_to_load)
