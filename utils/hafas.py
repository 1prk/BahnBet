import re
import pandas as pd
from datetime import datetime, timezone
import requests
import aiohttp
import asyncio
import json

import logging

logging.basicConfig(level=logging.INFO)

class Hafas():
    def __init__(self):
        self.stop_url = "http://reiseauskunft.insa.de/bin/ajax-getstop.exe/dn"
        self.url = 'https://reiseauskunft.insa.de/bin/mgate.exe'

    def get_station(self, string, results):
        payload = {
            "start": "1",
            "tpl": "suggest2json",
            "REQ0JourneyStopsF": "selectStationAttribute;MP",
            "REQ0JourneyStopsS0A": "7",
            "REQ0JourneyStopsB": results,
            "S": f"{string}?",
            "js": "true"
        }
        response = requests.get(self.stop_url, params=payload)
        json_match = re.search(r'SLs.sls=(\{.*?\});SLs.showSuggestion', response.text)
        if json_match:
            json_text = json_match.group(1)
            data = json.loads(json_text)
            return data['suggestions']

    def get_stations_map(self, query, dist, maxloc):
        payload = {
            "id": "dkw29zk6w2gqh64s",
            "ver": "1.48",
            "lang": "deu",
            "auth": {"type": "AID", "aid": "kAL6ULet"},
            "client": {
                "id": "NASA",
                "type": "WEB",
                "name": "webapp",
                "l": "vs_webapp_lvb",
                "v": ""
            },
            "formatted": False,
            "svcReqL": [
                {
                    "req": {
                        "input": {
                            "field": "S",
                            "loc": {
                                "type": "S",
                                "dist": dist,
                                "name": f"{query}?"
                            },
                            "maxLoc": maxloc
                        }
                    },
                    "meth": "LocMatch",
                    "id": "1|1|"
                }
            ]
        }
        response = requests.post(self.url, json=payload)
        return response.json()['svcResL'][0]['res']['match']['locL']

    def _process_stops(self, stations, starts_with):
        info = []
        for s in stations:
            name = s['name']
            type = s['type']
            #globalId = s['globalIdL'][0]['id']
            extId = s['extId']
            x = str(s['crd']['x'])
            y = str(s['crd']['y'])

            x = x[:2] + '.' + x[2:]
            y = y[:2] + '.' + y[2:]

            stop_info = {'name': name,
                         'type': type,
                         #'globalId': globalId,
                         'extId': extId,
                         'x': x,
                         'y': y}

            info.append(stop_info)

        df = pd.DataFrame(info)
        return df[df['name'].str.startswith(starts_with)]
    async def get_departures_async(self, session, extId):
        headers = {
            'Host': 'reiseauskunft.insa.de',
            'User-Agent': '<User-Agent>',  # Placeholder
            'Accept': '*/*',
            'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Content-Type': 'application/json',
            'Origin': 'https://reiseauskunft.insa.de',
            'Referer': 'https://reiseauskunft.insa.de/lvb/index.html?showPanCakeMenu=false&antiZoomHandling=yes&language=de_DE&P=SQ&cHash=915f38fe9afc66f66b080395443d88d7',  # Placeholder
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }

        payload = {
            "id": "dkw29zk6w2gqh64s",
            "ver": "1.48",
            "lang": "deu",
            "auth": {"type": "AID", "aid": "kAL6ULet"},
            "client": {
                "id": "NASA",
                "type": "WEB",
                "name": "webapp",
                "l": "vs_webapp_lvb",
                "v": ""
            },
            "formatted": False,
            "svcReqL": [
                {
                    "req": {
                        "stbLoc": {
                            "extId": extId
                            #"name": station_dict['value'],
                            #"lid": station_dict['id']
                            # "name": "Leipzig, Lützner",
                            # "lid": "A=1@O=Leipzig, Henriettenstr.@X=12324580@Y=51333737@U=80@L=12316@B=1@p=1720770855@i=A×de:14713:12316@",
                        },
                        "jnyFltrL": [{"type": "PROD", "mode": "INC", "value": 1023}],
                        "type": "DEP",
                        "sort": "PT",
                        "dur": 10
                        #"maxJny": 25
                    },
                    "meth": "StationBoard",
                    "id": "1|18|"
                }
            ]
        }

        try:
            async with session.post(self.url, headers=headers, json=payload) as response:
                return await response.json()
        except Exception as e:
            print(f"Error processing station {extId}: {str(e)}")
            return None

    def process_departures(self, departure_response):

        if 'svcResL' not in departure_response:
            return None

        departure_times = []
        station = departure_response['svcResL'][0]['res']['common']['locL'][0]['name']
        local_tz = pytz.timezone('Europe/Berlin')

        if 'jnyL' in departure_response['svcResL'][0]['res']:
            for journey in departure_response['svcResL'][0]['res']['jnyL']:
                line = re.search(r'#ZE#(.*?)#ZB#', journey['jid']).group(1)
                direction = journey['dirTxt']
                query_time = datetime.now(timezone.utc)
                date = datetime.strptime(journey['date'], '%Y%m%d').date()
                raw_departure_time_r = journey['stbStop'].get('dTimeR') if journey['stbStop'].get('dTimeR') and len(
                    journey['stbStop'].get('dTimeR')) <= 6 else None
                raw_departure_time_s = journey['stbStop'].get('dTimeS') if journey['stbStop'].get('dTimeS') and len(
                    journey['stbStop'].get('dTimeS')) <= 6 else None
                departure_prog_type = journey['stbStop'].get('dProgType')

                if raw_departure_time_r:
                    time_r = datetime.strptime(raw_departure_time_r, '%H%M%S').time()
                    formatted_departure_time_r = local_tz.localize(datetime.combine(date, time_r)).astimezone(
                        timezone.utc)
                else:
                    formatted_departure_time_r = None

                if raw_departure_time_s:
                    time_s = datetime.strptime(raw_departure_time_s, '%H%M%S').time()
                    formatted_departure_time_s = local_tz.localize(datetime.combine(date, time_s)).astimezone(
                        timezone.utc)
                else:
                    formatted_departure_time_s = None

                departure_times.append((
                    station,
                    line,
                    direction,
                    formatted_departure_time_r.isoformat() if formatted_departure_time_r else None,
                    formatted_departure_time_s.isoformat() if formatted_departure_time_s else None,
                    query_time.isoformat(),
                    departure_prog_type
                ))

            df = pd.DataFrame(departure_times,
                              columns=['stop', 'line', 'direction', 'departure_real', 'departure_scheduled', 'query_time',
                                       'departure_prog_type'])

            # Convert string timestamps to datetime objects
            df['departure_real'] = pd.to_datetime(df['departure_real'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
            df['departure_scheduled'] = pd.to_datetime(df['departure_scheduled'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
            df['query_time'] = pd.to_datetime(df['query_time'], format='%Y-%m-%dT%H:%M:%S.%f%z', utc=True)

            # Calculate delay
            df['delay'] = (df['departure_real'] - df['departure_scheduled']).dt.total_seconds() / 60

            return df
        else:
            return None


    async def process_station(self, session, id, semaphore):
        async with semaphore:
            response = await self.get_departures_async(session, id)
            if response:
                return self.process_departures(response)
        return None


    async def process_stations(self, stations, max_concurrent=50):
        semaphore = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            tasks = [self.process_station(session, id, semaphore) for id in stations.extId]
            results = await asyncio.gather(*tasks)

        return pd.concat([df for df in results if df is not None], ignore_index=True)


    def run_async_loop(self, stations):
        return asyncio.run(self.process_stations(stations))

    def get_delays(self, results):
        delays = results[(results['departure_prog_type'] == 'PROGNOSED') & (results['delay'].notna())].groupby(
            ['line', 'direction']).mean('delay').reset_index()
        return delays
