import asyncio
import configparser
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from asynciolimiter import StrictLimiter
import dropbox
import numpy as np
import pandas as pd
import httpx
from sqlalchemy import TEXT, URL, BigInteger, Boolean, Column, Float, Integer, MetaData, String, Table, create_engine, insert

date_now = datetime.now().strftime("%c").replace(":", "-")
ini_config = configparser.ConfigParser()
ini_config.read(Path.home() / "bol_export_files.ini")
dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))

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
    Column("condition", TEXT(10)),
    Column("ultimateOrderTime", TEXT(10)),
    Column("minDeliveryDate", TEXT(15)),
    Column("maxDeliveryDate", TEXT(15)),
    Column("bol_price_error", TEXT(70)),
    # * max 40 omdat er bij bol 40 prijzen kunnen zijn
    *[
        col
        for i in range(2, 40)
        for col in (
            Column(f"ean_{i}", BigInteger(), autoincrement=False),
            Column(f"prijs_bol_{i}", Float(20, 2)),
            Column(f"retailerId_{i}", Integer()),
            Column(f"countryCode_{i}", String(3)),
            Column(f"bestOffer_{i}", Boolean()),
            Column(f"fulfilmentMethod_{i}", TEXT(3)),
            Column(f"condition_{i}", TEXT(10)),
            Column(f"ultimateOrderTime_{i}", TEXT(10)),
            Column(f"minDeliveryDate_{i}", TEXT(15)),
            Column(f"maxDeliveryDate_{i}", TEXT(15))
        )
    ]
)
metadata.drop_all(engine)

metadata.create_all(engine)

class BOL_API:
    host = None
    key = None
    secret = None
    access_token = None
    access_token_expiration = None
    total_requested = 0
    

    def __init__(self, host, key, secret):
        # set init values on creation
        self.host = host
        self.key = key
        self.secret = secret

        try:
            self.access_token = self.getAccessToken()
            if self.access_token is None:
                raise Exception("Request for access token failed.")
        except Exception as e:
            print(e)
            sys.exit()
        else:
            self.access_token_expiration = time.time() + 250

    def getAccessToken(self):
        # request the JWT
        try:
            # request an access token
            # print(f"{datetime.now().strftime('%c')} token_refreshing")
            init_request = httpx.post(self.host, auth=(self.key, self.secret))
            init_request.raise_for_status()
        except Exception as e:
            print(e)
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
    async def make_api_get_requests_bol(self,session, url,rate_limiter):
        await rate_limiter.wait()
        response = await session.get(url, headers=self.access_token)
        # remaining_requests = int(response.headers.get("x-ratelimit-remaining", 1))
        time_to_sleep = 60
        if int(response.headers.get("x-ratelimit-remaining")) < 50:
            # print(response.headers.get("x-ratelimit-reset"))
            # print(f"{response.headers.get('x-ratelimit-remaining')} Remaining")
            limit_left = (int(response.headers.get("x-ratelimit-remaining")))
            time_to_sleep = (int(response.headers.get("x-ratelimit-reset")))
            if limit_left < 10:
                print(f"to fast {time_to_sleep} seconds...")
                await asyncio.sleep(time_to_sleep*2)
        if response.status_code in (429,):
            print(f"Rate limit exceeded. Retrying in {time_to_sleep} seconds...")
            await asyncio.sleep(time_to_sleep)
            return await self.make_api_get_requests_bol(session, url)
        return response.json()

    async def fetch_items_bol(self,session,url,product_id,rate_limiter):
        # print(f"{datetime.now().strftime('%c')} new second")
        bol_result = await self.make_api_get_requests_bol(session, url,rate_limiter)
        return product_id, bol_result


    async def process_bol_competing_prices_prices(self,eans):
        timeout = httpx.Timeout(250)
        async with httpx.AsyncClient(timeout=timeout) as client:
            rate_limiter = StrictLimiter(890/60) # Limit to MAX 900 a minute concurrent requests
            tasks = [asyncio.create_task(self.fetch_items_bol(client,f"{ini_config.get('bol_api_urls','base_url')}/products/{str(ean['ean']).zfill(13)}/offers?condition=NEW",ean,rate_limiter)) for ean in eans]
            product_prices_bol = await asyncio.gather(*tasks, return_exceptions=True)
            respose_list_bol_info = []
            for ean, bol_offers in product_prices_bol:
                offer_nr = 1
                response_bol_info = {}
                if bol_offers.get("offers"):
                    sorted_prices = sorted(sorted(bol_offers["offers"], key=lambda x: x['price']), key=lambda x: not x['bestOffer'])
                    for bol_pricing in sorted_prices:
                        if offer_nr == 40:
                            break
                        base_info = {
                            "ean": int(ean.get("ean")),
                            "prijs_bol": bol_pricing.get("price"),
                            "retailerId": bol_pricing.get("retailerId"),
                            "countryCode": bol_pricing.get("countryCode"),
                            "bestOffer": bol_pricing.get("bestOffer"),
                            "fulfilmentMethod": bol_pricing.get("fulfilmentMethod"),
                            "condition": bol_pricing.get("condition"),
                            "ultimateOrderTime": bol_pricing.get("ultimateOrderTime"),
                            "minDeliveryDate": bol_pricing.get("minDeliveryDate"),
                            "maxDeliveryDate": bol_pricing.get("maxDeliveryDate"),
                        }
                        if bol_pricing.get("bestOffer") != True:
                            response_bol_info.update({f"{key}_{offer_nr}": value for key, value in base_info.items()})
                        else:
                            response_bol_info.update({f"{key}": value for key, value in base_info.items()})
                        offer_nr += 1
                    
                elif bol_offers.get('status') in (400, 404):
                    response_bol_info["ean"] = int(ean.get("ean"))
                    try: # 99% of cases it will be invalid ean
                        response_bol_info["bol_price_error"] = f"Invalid bol ean: {re.search(r'[0-9]+', bol_offers['violations'][0]['reason']).group(0)}"
                    except AttributeError:
                        response_bol_info["bol_price_error"] = bol_offers['violations'][0]["reason"]
                else:
                    response_bol_info["ean"] = int(ean.get("ean"))
                    response_bol_info["bol_price_error"] = "geen kook"
                respose_list_bol_info.append(response_bol_info)
                with engine.connect() as conn:
                    conn.execute(insert(price_info).values(**response_bol_info))
                    conn.commit()
            return respose_list_bol_info

# verkrijgen alle ean nummers
ean_basis_path = Path.home() / "ean_numbers_basisfiles"
alle_eans_uit_basisbestand = pd.read_csv(
    max(ean_basis_path.glob("ean_numbers_basis_*.csv"), key=os.path.getctime,),
    usecols=["ean"],
)

ean_from_basis_file_unique = alle_eans_uit_basisbestand.astype(str).query('ean.str.len() > 8').query("~ean.str.contains('-')").drop_duplicates()

# voor testen, klein aantal nummers
# ean_from_basis_file_unique = ean_from_basis_file_unique[:200]

# ean_from_basis_file_unique = pd.DataFrame(["7702018311422","4012074002541","5025232952700"], columns=['ean'])

# print(ean_from_basis_file_unique.query('ean == "7702018311422"'))

first_shop= list(ini_config["bol_winkels_api"].keys())[1]
client_id, client_secret,_,_ = [x.strip() for x in ini_config.get("bol_winkels_api", first_shop).split(",")]

bol_call_upload = BOL_API(ini_config["bol_api_urls"]["authorize_url"], client_id, client_secret)

get_bol_allowable_price = asyncio.run(bol_call_upload.process_bol_competing_prices_prices(ean_from_basis_file_unique.to_dict("records")))

get_bol_competition_price_totaal = pd.DataFrame(get_bol_allowable_price).assign(ean = lambda x : x['ean'].astype(str))
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
            "Richtprijs Exclusief-prijs",
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
        f.read(), "/macro/datafiles/PriceList/" + ftp_has_run.name, mode=dropbox.files.WriteMode("overwrite", None)
    )

