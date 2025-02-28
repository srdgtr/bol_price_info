import asyncio
import configparser
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict
import dropbox
import numpy as np
import pandas as pd
import httpx
from httpx_limiter import AsyncRateLimitedTransport
from sqlalchemy import (
    TEXT,
    URL,
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    text,
)

date_now = datetime.now().strftime("%c").replace(":", "-")
ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")

try:
    dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))
except Exception:
    dbx = dropbox.Dropbox(ini_config.get("dropbox", "api_dropbox"))

config_db = dict(
    drivername="mariadb",
    username=ini_config.get("database odin", "user"),
    password=ini_config.get("database odin", "password"),
    host=ini_config.get("database odin", "host"),
    port=ini_config.get("database odin", "port"),
    database=ini_config.get("database odin", "database"),
)
engine = create_engine(URL.create(**config_db))

conn = engine.connect()
metadata = MetaData()

logger = logging.getLogger("price_info")
logging.basicConfig(
    filename=f"price_info_bol_" + date.today().strftime("%V") + ".log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)  # nieuwe log elke week
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

price_info = Table(
    "bol_price_info",
    metadata,
    # basis kolomen
    Column("ean", BigInteger(), primary_key=True, autoincrement=False),
    Column("prijs_bol", Float(20, 2)),
    Column("retailerId", Integer()),
    Column("countryCode", String(3)),
    Column("bestOffer", Boolean()),
    Column("fulfilmentMethod", TEXT(3)),
    Column("ultimateOrderTime", TEXT(10)),
    Column("minDeliveryDate", TEXT(15)),
    Column("maxDeliveryDate", TEXT(15)),
    Column("bol_price_error", TEXT(70)),
    Column("not_found_ean", Boolean()),
    Column("invalid_ean", Boolean()),
    # * max 40 omdat er bij bol 40 prijzen kunnen zijn
    *[
        col
        for i in range(1, 25)
        for col in (
            Column(f"prijs_bol_{i}", Float(20, 2)),
            Column(f"retailerId_{i}", Integer()),
            Column(f"countryCode_{i}", String(3)),
            Column(f"bestOffer_{i}", Boolean()),
            Column(f"fulfilmentMethod_{i}", TEXT(3)),
            Column(f"ultimateOrderTime_{i}", TEXT(10)),
            Column(f"minDeliveryDate_{i}", TEXT(15)),
            Column(f"maxDeliveryDate_{i}", TEXT(15)),
        )
    ],
)
metadata.drop_all(engine)

metadata.create_all(engine)

class BOL_API:
    host = None
    key = None 
    secret = None
    access_token = None
    access_token_expiration = None

    def __init__(self, host: str, key: str, secret: str):
        # set init values on creation
        self.host = host
        self.key = key
        self.secret = secret

        try:
            self.access_token = self.getAccessToken()
            if self.access_token is None:
                raise Exception("Request for access token failed.")
        except Exception as e:
            logging.error(e)
            sys.exit()
        else:
            self.access_token_expiration = time.time() + 250

    def getAccessToken(self) -> Dict[str, str]:
        # request the JWT
        try:
            # request an access token
            init_request = httpx.post(self.host, auth=(self.key, self.secret))
            init_request.raise_for_status()
        except Exception as e:
            logging.error(e)
            return None
        else:
            token = json.loads(init_request.text)["access_token"]
            if token:  # add right headers
                post_header = {
                    "Accept": "application/vnd.retailer.v10+json",
                    "Content-Type": "application/vnd.retailer.v10+json",
                    "Authorization": f"Bearer {token}",
                    "Connection": "keep-alive",
                }
            self.access_token_expiration = time.time() + 250
            return post_header

    def refresh_token(self):
        self.access_token = self.getAccessToken()
        if self.access_token is None:
            raise Exception("Failed to refresh access token.")
        logging.info("Token refreshed successfully")

    class Decorators:
        @staticmethod
        def refreshToken(decorated):
            # check the JWT and refresh if necessary
            def wrapper(api, *args, **kwargs):
                if time.time() > api.access_token_expiration:
                    api.access_token = api.getAccessToken()
                return decorated(api, *args, **kwargs)
            return wrapper

    @Decorators.refreshToken
    async def fetch_prices_others(self, client, ean):
        try:
            response = await client.get(
                f"{ini_config.get('bol_api_urls','base_url')}/products/{ean}/offers?condition=NEW",
                headers=self.access_token,
            )
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 401:
                logging.info("Token expired, refreshing token...")
                self.refresh_token()
                client.headers = self.access_token
                return await self.fetch_prices_others(client, ean)  # Retry
            logging.error(f"Error for EAN {ean}: {exc.response}")
            if exc.response.status_code == 429:
                time_to_sleep = int(response.headers.get("retry-after", 60))
                await asyncio.sleep(time_to_sleep*2)
                logging.info("to much requests at same time, try again")
                return await self.fetch_prices_others(client, ean)
            return response
        except httpx.ReadError as exc:
            logging.error(f"read error for EAN {ean}: {exc}")


    @Decorators.refreshToken
    async def get_prices_others_bol(self):
        with engine.connect() as connection:
            all_eans = (
                connection.execute(text(f"SELECT CAST(ean AS INTEGER) AS ean FROM unieke_eans")).mappings().all()
            )#[7500:8000]
        ean_to_response = {}
        timeout = httpx.Timeout(240)
        async with httpx.AsyncClient(timeout=timeout, transport=AsyncRateLimitedTransport.create(rate=900 / 60, capacity=50)) as client:
            ean_to_response = {}
            for row in all_eans:
                ean = str(row.get("ean", "")).zfill(13)
                response = await self.fetch_prices_others(client, ean)
                ean_to_response[ean] = response

            data_list = []
            for ean, response in ean_to_response.items():
                offer_nr = 1 #start from 2 as the first shop should be without number
                data = {"ean": int(ean)}
                response_data = response.json()
                if response_data.get("offers"):
                    sorted_prices = sorted(
                        sorted(response_data["offers"], key=lambda x: x["price"]),
                        key=lambda x: not x["bestOffer"],
                    )
                    for bol_pricing in sorted_prices:
                        if offer_nr == 24:
                            break
                        base_info = {
                            "prijs_bol": bol_pricing.get("price", 0.0),
                            "retailerId": bol_pricing.get("retailerId",""),
                            "countryCode": bol_pricing.get("countryCode",""),
                            "bestOffer": bol_pricing.get("bestOffer",False),
                            "fulfilmentMethod": bol_pricing.get("fulfilmentMethod"),
                            "ultimateOrderTime": bol_pricing.get("ultimateOrderTime"),
                            "minDeliveryDate": bol_pricing.get("minDeliveryDate"),
                            "maxDeliveryDate": bol_pricing.get("maxDeliveryDate"),
                        }
                        if bol_pricing.get("bestOffer") != True:
                            data.update({f"{key}_{offer_nr}": value for key, value in base_info.items()})
                        else:
                            data.update({f"{key}": value for key, value in base_info.items()})
                        offer_nr += 1

                elif response.status_code in (400, 404):
                    status_message = "Invalid EAN" if response.status_code == 400 else "Product not found"
                    logging.info(f"{status_message} for EAN {ean}")
                    data[f"{'invalid' if response.status_code == 400 else 'not_found'}_ean"] = True  # Dynamic key
                else:
                    logging.info(f"Failed to receive data for EAN {ean}: {response.status_code} - {response.text}")

                data_list.append(data)
                
                with engine.connect() as conn:
                    conn.execute(insert(price_info).values(**data))
                    conn.commit()
            return data_list

ean_basis_path = Path.home() / "ean_numbers_basisfiles"
first_shop = list(ini_config["bol_winkels_api"].keys())[2]
client_id, client_secret, _, _ = [x.strip() for x in ini_config.get("bol_winkels_api", first_shop).split(",")]
bol_api = BOL_API(ini_config["bol_api_urls"]["authorize_url"], client_id, client_secret)
get_bol_allowable_price = asyncio.run(bol_api.get_prices_others_bol())

get_bol_competition_price_totaal = pd.DataFrame(get_bol_allowable_price).assign(ean=lambda x: x["ean"].astype(str))
get_bol_competition_price_totaal.to_csv(f"bol_competition_prices_{date_now}.csv", index=False)

ean_basis = (
    pd.read_excel(
        max(
            ean_basis_path.glob("basis_BasisBestand_*.xlsm"),
            key=os.path.getctime,
        ),
        usecols=[
            "Product ID eigen",
            "EAN",
            "EAN (handmatig)",
            "Inkoopprijs (excl. BTW)",
            "Verkoopprijs BOL (excl. comissie)",
            "Verkoopprijs BOL (incl. comissie)",
            "Subcategorie",
            "Levertijd (in tekst)",
            "Exclusief",
            "Exclusief-prijs",
            "Richtprijs handmatig",
            "Fabrikant",
            "economic_operators",
        ],
        engine="openpyxl",
    )
    .assign(ean=lambda x: x["EAN"].where(x["EAN (handmatig)"] != np.nan, x["EAN (handmatig)"]))
    .dropna(subset=["Product ID eigen"])
    .astype("str")
)

# get_bol_competition_price_totaal = pd.read_sql('bol_price_info',engine).astype("str") # if something gose wrong use sql table
# get_bol_competition_price_totaal = pd.read_csv(max(Path(".").glob("bol_competition_prices_*.csv"),key=os.path.getctime,),low_memory=False).assign(ean = lambda x : x['ean'].astype(str))

bol_prices_basis_merged = pd.merge(ean_basis, get_bol_competition_price_totaal, on="ean", how="left")
with open("basis_sorted_PriceList_" + date_now + ".csv", mode="w", newline="\n") as f:
    bol_prices_basis_merged.to_csv(f, sep=",", index=False)

with open("is_bol_ftp_uitgevoerd.txt", "w") as f:
    f.write(f"to_check_if_bol_ftp_file_has_run {date_now}")

ftp_has_run = Path.cwd() / "is_bol_ftp_uitgevoerd.txt"

with open(ftp_has_run, "rb") as f:
    dbx.files_upload(
        f.read(),
        "/macro/datafiles/PriceList/" + ftp_has_run.name,
        mode=dropbox.files.WriteMode("overwrite", None),
    )
