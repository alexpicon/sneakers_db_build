"""Builds a database of sneakers."""

# Copyright (c) 2021 Alexander Picon
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import concurrent.futures
import functools
import json
import logging
import math
import os
import pathlib
import re
import sqlite3
import sys
from urllib.parse import urljoin

import requests


class Api:
    """A nicer way to use the APIs"""

    def __init__(self, host: str, key: str, cache_dir: pathlib.Path) -> None:
        self.url = f"https://{host}"
        self.session = requests.Session()
        self.session.headers.update({"x-rapidapi-key": key, "x-rapidapi-host": host})
        self.cache_dir = cache_dir

    def cache_path(self, endpoint: str, querystring: dict) -> pathlib.Path:
        """Returns a path to cache a unique combination of endpoint and querystring

        Sorts the querystring by key to make sure the files are not duplicated

        :param endpoint: the api endpoint
        :param querystring: a querystring dictionary to serialize
        :return: a path to a json file that caches the request that may or may not exist yet!
        """
        fname = [endpoint]
        for key in sorted(querystring.keys()):
            fname.append(f"_{key}_{querystring[key]}")
        return self.cache_dir / f"{''.join(fname)}.json"

    def call(self, endpoint: str, querystring: dict) -> dict:
        """Makes an API call. Uses json files as a cache to avoid making extra calls.

        :param endpoint: the api endpoint
        :param querystring: a querystring dictionary to serialize
        :return: the returned json object coming as a response
        """
        cache = self.cache_path(endpoint, querystring)
        if cache.is_file():
            logging.info(f"cache hit: {cache.name}")
            return json.loads(cache.read_text())
        resp = self.session.get(urljoin(self.url, endpoint), params=querystring)
        resp.raise_for_status()
        data = resp.json()
        with open(cache, "w", newline="\n") as fh:
            json.dump(data, fh, sort_keys=True, indent=4)
        return data

    @functools.cached_property
    def brands(self) -> list:
        """Hits the brands API

        :return: A list of brands
        """
        resp = self.call("brands", {})
        return sorted(resp["results"])

    @functools.cached_property
    def genders(self) -> list:
        """Hits the genders API

        :return: A list of genders
        """
        resp = self.call("genders", {})
        return sorted(resp["results"])

    @functools.cache
    def sneakers(self, **parameters) -> dict:
        """Hits the sneakers API

        :param parameters:
        :return: A dictionary with the count that matches the parameters, and a page of results
        """
        parameters.setdefault("limit", 100)
        parameters.setdefault("page", 0)
        resp = self.call("sneakers", parameters)
        return resp


class Database:
    """Wrapper for the sqlite DB"""

    def __init__(self, api: Api, base_path: pathlib.Path):
        self.api = api
        self.brands = []
        self.genders = []
        self.page_limit = 100
        self.base_path = base_path
        self.db = sqlite3.connect(":memory:")

    def process(self):
        """Entry point to load all data, leveraging the API"""
        self.create_tables()
        self.insert_brands()
        self.insert_genders()
        self.load_sneakers()
        self.load_fts()
        self.savedb()

    def insert_brands(self):
        """Inserts all known brands into the brands table."""
        self.brands = self.api.brands
        self.db.cursor().executemany(
            f"""INSERT INTO brands(brand) VALUES(:brand)""",
            [{"brand": brand} for brand in self.api.brands],
        )
        self.db.commit()

    def insert_genders(self):
        """Inserts all known genders into the genders table."""
        self.genders = self.api.genders
        self.db.cursor().executemany(
            f"""INSERT INTO genders(gender) VALUES(:gender)""",
            [
                {"gender": gender}
                for gender in self.api.genders
                # normalize: WOMENS->WOMEN, MENS->MEN
                if gender not in ["MENS", "WOMENS"]
            ],
        )
        self.db.commit()

    def load_sneakers(self):
        """Load all sneakers"""
        concurrency = 1
        # The filtering in the API is very bad, so we need to pull all sneakers in sequence.
        # They don't seem to update frequently so the result should be consistent.
        # In order to iterate, first we load the first page of sneakers
        first_page = self.api.sneakers(limit=self.page_limit, page=0)
        # we get the total of sneakers, we don't really care about the results right now
        # because we hit a cache when called again
        count = first_page["count"]
        # now that we have the number sneakers, we know how many pages we need to request
        last_page_num = math.ceil(count / self.page_limit) - 1
        if concurrency > 1:
            # we use 4 as maximum concurrency because they have a limit of 5 per second 🤦
            self.load_sneakers_concurrently(last_page_num, concurrency)
        else:
            # use this when not using proxies because they don't like concurrency
            self.load_sneakers_safely(last_page_num)

    def load_sneakers_concurrently(self, last_page_num: int, max_workers: int):
        """Parallelize loading sneakers with a thread pool.

        Only use this with proxies.

        :param last_page_num: Stop at this page.
        :param max_workers: How many parallel workers to use
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = {
                executor.submit(self.load_sneakers_page, page): page
                for page in range(last_page_num + 1)
            }
            for future in concurrent.futures.as_completed(tasks):
                page = tasks[future]
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"ERROR page={page} error={exc}")

    def load_sneakers_safely(self, last_page_num: int):
        """Load sneakers sequentially. Safe and slow.

        :param last_page_num: Stop at this page.
        """
        for page in range(last_page_num + 1):
            logging.info(f"Processing page {page} of {last_page_num}")
            try:
                self.load_sneakers_page(page)
            except Exception as exc:
                logging.error(f"ERROR page={page} error={exc}")

    def load_sneakers_page(self, page: int):
        """Load a page of info from the API and stores it in the DB

        :param page: the page number to load
        :return:
        """
        cursor = self.db.cursor()
        data = self.api.sneakers(limit=self.page_limit, page=page)
        for sneaker in data["results"]:
            self.insert_sneaker(sneaker, cursor)
        self.db.commit()

    def create_tables(self):
        """Creates the database schema."""
        logging.info("creating sneakers sql table")
        cursor = self.db.cursor()
        with (self.base_path / "create_tables.sql").open("r") as file_handle:
            cursor.executescript(file_handle.read())

    def savedb(self):
        """Dumps the in-memory DB to a file."""
        dbfile = self.base_path / "sneakers.db"
        logging.info(f"Saving sneakers database to {dbfile}")
        fdb = sqlite3.connect(dbfile)
        with fdb:
            self.db.backup(fdb, pages=0)
        self.db.close()
        fdb.close()

    def load_fts(self):
        """Updates the full text search table after the sneakers are loaded."""
        logging.info("INSERTing into search table")
        cursor = self.db.cursor()
        query = f"""
        INSERT INTO sneakers_fts(brand, name, silhouette, colorway, sku)
        SELECT brand, name, silhouette, colorway, sku FROM sneakers
        """
        cursor.execute(query)
        logging.info("optimizing search index")
        # noinspection SqlResolve
        query = r"INSERT INTO sneakers_fts(sneakers_fts) VALUES ('optimize')"
        cursor.execute(query)
        self.db.commit()

    def insert_sneaker(self, sneaker, cursor):
        """Inserts a sneaker object to the database

        :param sneaker: A sneaker object (a dictionary containing data)
        :param cursor: The cursor coming from the main handler
        """
        self.assert_sneaker_keys(sneaker)

        # we use the SKU as a key, ignore the internal API uuid
        sneaker.pop("id")
        sku = sneaker.get("sku")
        assert len(sku) > 0

        images = sneaker.pop("image")
        links = sneaker.pop("links")
        data = self.get_basically_sanitized_sneaker_data(sneaker)
        self.fix_date_and_year_in_sneaker_data(data)
        data.update(self.sqlize_sneakers_images_fields(images))
        data.update(self.sqlize_sneakers_links_fields(links))
        query = f"INSERT INTO sneakers ({', '.join(data.keys())}) VALUES ({':' + ', :'.join(data.keys())})"
        cursor.execute(query, data)
        self.insert_360_images(cursor, sku, images)

    @classmethod
    def insert_360_images(cls, cursor, sku, images):
        """Inserts associated 360 images of a sneaker to their own table.

        :param cursor: The cursor coming from the main handler
        :param sku: SKU (pkey) of the sneaker
        :param images: A list of urls to the 360 image of a sneaker
        """
        if images["360"]:
            for pos, img in enumerate(images["360"]):
                query = (
                    f"INSERT INTO images_360 (sku, position, image) VALUES (?, ?, ?)"
                )
                cursor.execute(query, (sku, pos, img))

    def get_basically_sanitized_sneaker_data(self, sneaker):
        """Sanitizes and normalizes a sneaker object.

        :param sneaker: The sneaker to sanitize
        :return: The sanitized/normalized sneaker object.
        """
        data = {}
        for key, value in sneaker.items():
            if key in ["releaseYear", "estimatedMarketValue", "retailPrice"]:
                # integer fields
                if value > 0:
                    data[key] = value
            else:
                value = value.strip()
                if key == "gender":
                    value = value.upper()
                    if value.endswith("MENS"):
                        # normalize: WOMENS->WOMEN, MENS->MEN
                        value = value.rstrip("S")
                    if value not in self.genders:
                        raise "API gave us an unknown gender"
                if key == "brand":
                    value = value.upper()
                    if value not in self.brands:
                        raise "API gave us an unknown brand"
                data[key] = value
        return data

    @staticmethod
    def sqlize_sneakers_links_fields(links):
        """Sanitizes fields that are links to other sites.

        :param links: List of links
        :return: A dictionary of columns and data to be inserted
        """
        return {
            f"link_{link}": links.get(link).strip()
            for link in ["flightClub", "goat", "stadiumGoods", "stockX"]
            if links.get(link).strip() != ""
        }

    @staticmethod
    def sqlize_sneakers_images_fields(images):
        """Sanitizes fields that are images of a sneaker.

        :param images: List of image URLs
        :return: A dictionary of columns and data to be inserted
        """
        return {
            f"image_{size}": images.get(size).strip()
            for size in ["original", "small", "thumbnail"]
            if images.get(size).strip() != ""
        }

    @staticmethod
    def assert_sneaker_keys(sneaker):
        """Assert that the sneaker object has all the data we need.

        :param sneaker: The sneaker object
        """
        assert set(sneaker.keys()) == {
            "brand",
            "gender",
            "estimatedMarketValue",
            "releaseYear",
            "story",
            "id",
            "colorway",
            "silhouette",
            "sku",
            "image",
            "retailPrice",
            "links",
            "name",
            "releaseDate",
        }
        assert set(sneaker["image"].keys()) == {"360", "original", "small", "thumbnail"}
        assert set(sneaker["links"].keys()) == {
            "flightClub",
            "goat",
            "stadiumGoods",
            "stockX",
        }

    @staticmethod
    def fix_date_and_year_in_sneaker_data(data):
        """Sanitizes date fields, filling missing data if possible.

        :param data: The sneaker data
        """
        rd = data.pop("releaseDate", "").strip()
        ry = data.pop("releaseYear", None)
        if (
            rd and rd != "0001-01-01"
        ):  # 1-1-1 is so weird, I just want to leave it there 🤣
            assert re.match(r"^\d\d\d\d-\d\d-\d\d$", rd)
            if rd.startswith("00"):
                # millenium and century digits truncated
                rd = "20" + rd[2:]
            potential_year = int(rd.split("-")[0])
            if ry is None or ry == 0:
                logging.info(
                    f"{data.get('sku')} Filling releaseYear from year found on releaseDate"
                )
                ry = potential_year
            else:
                if ry < 99:
                    # something truncated the millenium and century digits
                    ry += 2000
                if ry != potential_year:
                    # this should NEVER happen!
                    logging.error(
                        f"{data.get('sku')} releaseYear ({ry}) and releaseDate ({rd}) do not match!"
                    )
        if rd:
            data["releaseDate"] = rd
        if ry:
            data["releaseYear"] = ry


def main() -> int:
    """The sneaker_db_build application entry point."""

    logging.basicConfig(
        level=logging.DEBUG,
        format="👟 %(relativeCreated)8d 👟 %(threadName)s 👟 %(message)s",
    )
    base_dir = pathlib.Path(__file__).resolve().parent
    cache_dir = base_dir / "_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    api = Api(os.environ["RAPIDAPI_HOST"], os.environ["RAPIDAPI_KEY"], cache_dir)
    db = Database(api, base_dir)
    db.process()
    return 0


if __name__ == "__main__":
    sys.exit(main())
