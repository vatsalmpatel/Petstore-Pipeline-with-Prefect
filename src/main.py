#!/usr/bin/env python3
import httpx
import psycopg2
from prefect import flow, task, get_run_logger

@task
def get_petstore_inventory(
    base_url:str = "petstore.swagger.io",
    path:str = "/v2/store/inventory",
    secure:bool = True
):
    # logger = get_run_logger()
    # logger.info("In the get_petstore_inventory task")

    if path[0] != "/":
        path = f"/{path}"
    if secure:
        url = f"https://{base_url}{path}"
    else:
        url = f"http://{base_url}{path}"

    response = httpx.get(url)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.exception("Failed when getting the API data from the Petstore API")
        raise e
    return response.json() 

@task
def clean_stats(data:dict):
    # logger = get_run_logger()
    # logger.info("In the clean_stats task")
    return {
        "sold": data.get("sold", 0) + data.get("Sold", 0),
        "pending": data.get("pending", 0) + data.get("Pending", 0),
        "available": data.get("available", 0) + data.get("Available", 0),
        "unavailable": data.get("not available", 0) + data.get("Unavailable", 0)
    }

@task
def insert_results(
    data: dict,
    db_user,
    db_password,
    db_name,
    db_host
):
    # logger = get_run_logger()
    # logger.info("In the insert_results task")
    with psycopg2.connect(
        user=db_user, password=db_password, dbname=db_name, host=db_host
    ) as conn:
        logger.info("Connection to Postgres Database Opened!!")
        with conn.cursor() as cursor:
            logger.info("Writing data to the inventory_history in the petstore database in Postgres")
            cursor.execute(
                """
                insert into inventory_history (
                                        fetch_timestamp,
                                        sold,
                                        pending,
                                        available,
                                        unavailable
                ) values (now(), %(sold)s, %(pending)s, %(available)s, %(unavailable)s)
                           """,
                data,
            )

@flow
def collect_petstore_info(
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
    db_user: str = "root",
    db_password: str = "root",
    db_name: str = "petstore",
    db_host: str = "localhost",
):
    raw_data = get_petstore_inventory(base_url, path, secure)
    cleaned_data = clean_stats(raw_data)
    insert_results(
        cleaned_data,
        db_user,
        db_password,
        db_name,
        db_host,
    )
    # logger = get_run_logger()
    # logger.info("All the tasks have been completed!!")

def main():
    collect_petstore_info.serve(name = 'petstore-inventory-check')

if __name__ == "__main__":
    main()
