"""Three independent sources, provenance, cross-source dedup and quality branching."""
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import psycopg2
import requests
import pycountry
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

SOURCES = ['geonames', 'worldometer', 'worldbank']
AGENT = 'CountryComparisonStudentProject/1.0'


def connection():
    return psycopg2.connect(host=os.environ['DB_HOST'], user=os.environ['DB_USER'],
                            password=os.environ['DB_PASSWORD'], dbname=os.environ['DB_NAME'])


def initialize():
    with connection() as conn, conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS country_observations (
            source text NOT NULL, iso3 text NOT NULL, name text NOT NULL,
            population bigint, area_km2 double precision, region text,
            population_year integer, source_url text NOT NULL,
            fetched_at timestamptz NOT NULL, PRIMARY KEY(source, iso3));
        CREATE TABLE IF NOT EXISTS countries (
            iso3 text PRIMARY KEY, name text NOT NULL, population bigint,
            area_km2 double precision, region text, population_year integer,
            population_source text, area_source text, sources jsonb NOT NULL,
            updated_at timestamptz NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_sources (
            run_id text, source text, status text, row_count integer,
            detail text, checked_at timestamptz DEFAULT now(), PRIMARY KEY(run_id,source));
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id text PRIMARY KEY, status text, row_count integer,
            detail text, checked_at timestamptz DEFAULT now());''')


def get_json(url):
    return fetch(url).json()


def fetch(url):
    origin = '{0.scheme}://{0.netloc}'.format(urlsplit(url))
    robots = requests.get(origin + '/robots.txt', headers={'User-Agent': AGENT}, timeout=30)
    if robots.status_code == 200:
        if '<html' in robots.text[:500].lower():
            raise RuntimeError('robots.txt returned HTML; cannot verify permission')
        parser = RobotFileParser()
        parser.parse(robots.text.splitlines())
        if not parser.can_fetch(AGENT, url):
            raise RuntimeError('robots.txt disallows this URL')
        time.sleep(max(1, parser.crawl_delay(AGENT) or parser.crawl_delay('*') or 0))
    elif robots.status_code not in (404, 410):
        raise RuntimeError(f'Cannot verify robots.txt: HTTP {robots.status_code}')
    response = requests.get(url, headers={'User-Agent': AGENT}, timeout=60)
    response.raise_for_status()
    if 'text/html' in response.headers.get('Content-Type', ''):
        response.encoding = 'utf-8'
    return response


def number(value):
    if value is None or value == '':
        return None
    return float(str(value).replace(',', '').strip())


def scrape_geonames():
    url = 'https://www.geonames.org/countries/'
    soup = BeautifulSoup(fetch(url).text, 'html.parser')
    rows = []
    for tr in soup.select('tr'):
        cells = [c.get_text(' ', strip=True) for c in tr.find_all('td', recursive=False)]
        if len(cells) >= 9 and re.fullmatch(r'[A-Z]{3}', cells[1]):
            rows.append(dict(iso3=cells[1], name=cells[4], area_km2=number(cells[6]),
                             population=int(number(cells[7])), region={'AS':'Asia','EU':'Europe',
                             'AF':'Africa','NA':'North America','SA':'South America',
                             'OC':'Oceania','AN':'Antarctica'}.get(cells[8], cells[8]),
                             population_year=None, source_url=url))
    if len(rows) < 100:
        raise ValueError(f'Unexpected GeoNames HTML: only {len(rows)} countries')
    return rows


def worldometer():
    url = 'https://www.worldometers.info/world-population/population-by-country/'
    soup = BeautifulSoup(fetch(url).text, 'html.parser')
    aliases = {'DR Congo':'COD','South Korea':'KOR','North Korea':'PRK','Laos':'LAO',
               'State of Palestine':'PSE','Holy See':'VAT','Taiwan':'TWN',
               'Micronesia':'FSM','Congo':'COG','Réunion':'REU',
               'Saint Helena':'SHN','Channel Islands':None,
               'Russia':'RUS','Turkey':'TUR',"Côte d'Ivoire":'CIV',
               'Czech Republic (Czechia)':'CZE','Brunei':'BRN',
               'Sao Tome & Principe':'STP','St. Vincent & Grenadines':'VCT',
               'U.S. Virgin Islands':'VIR','Faeroe Islands':'FRO',
               'Turks and Caicos':'TCA','Saint Kitts & Nevis':'KNA',
               'Sint Maarten':'SXM','Caribbean Netherlands':'BES','Saint Martin':'MAF',
               'Saint Barthelemy':'BLM','Wallis & Futuna':'WLF',
               'Saint Pierre & Miquelon':'SPM','Falkland Islands':'FLK'}
    rows = []
    for table in soup.select('table'):
        headers = [th.get_text(' ',strip=True) for th in table.select('thead th')]
        if not headers:
            headers = [th.get_text(' ',strip=True) for th in table.select('tr')[0].select('th')]
        population_header = next((h for h in headers if 'Population' in h), '')
        year_match = re.search(r'20\d{2}', population_header)
        for tr in table.select('tbody tr'):
            cells = [td.get_text(' ',strip=True) for td in tr.select('td')]
            if len(cells) < 4:
                continue
            name = cells[1]
            try:
                code = aliases[name] if name in aliases else pycountry.countries.lookup(name).alpha_3
            except LookupError:
                raise ValueError(f'Unmapped Worldometer country: {name}')
            if code is None:
                # Channel Islands combines two ISO territories, so do not invent an ISO3.
                continue
            rows.append(dict(iso3=code,name=name,population=int(number(cells[2])),
                             area_km2=None,region=None,
                             population_year=int(year_match[0]) if year_match else None,source_url=url))
    if len(rows) < 100:
        raise ValueError(f'Unexpected Worldometer HTML: {len(rows)} rows')
    return rows


def world_bank():
    metadata = get_json('https://api.worldbank.org/v2/country?format=json&per_page=400')
    countries = {r['id']: r for r in metadata[1] if r['region']['id'] != 'NA'}
    url = 'https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=20000&mrnev=1'
    data = get_json(url)
    if int(data[0]['pages']) != 1:
        raise ValueError('World Bank response requires pagination')
    rows = []
    for r in data[1]:
        code = r.get('countryiso3code')
        if code in countries and r['value'] is not None:
            rows.append(dict(iso3=code, name=countries[code]['name'], population=int(r['value']),
                             area_km2=None, region=countries[code]['region']['value'],
                             population_year=int(r['date']), source_url=url))
    return rows


def source_status(run_id, source, status, count, detail):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('''INSERT INTO pipeline_sources(run_id,source,status,row_count,detail)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT(run_id,source) DO UPDATE
            SET status=excluded.status,row_count=excluded.row_count,detail=excluded.detail,
            checked_at=now()''', (run_id, source, status, count, detail))


def extract(source, **context):
    # XCom always describes this run; failed sources never supply stale rows.
    try:
        config = context['dag_run'].conf or {}
        if os.getenv('SIMULATE_FAILED_SOURCE') == source or config.get('simulate_failed_source') == source:
            raise RuntimeError('Explicit failure simulation')
        rows = {'geonames': scrape_geonames, 'worldometer': worldometer,
                'worldbank': world_bank}[source]()
        source_status(context['run_id'], source, 'extracted', len(rows), '')
        return dict(ok=True, rows=rows)
    except Exception as exc:
        source_status(context['run_id'], source, 'failed', 0, str(exc)[:1500])
        return dict(ok=False, rows=[], error=str(exc))


def transform(source, **context):
    result = context['ti'].xcom_pull(task_ids=f'{source}.extract')
    if not result['ok']:
        return result
    valid, rejected = {}, 0
    for row in result['rows']:
        code = str(row.get('iso3', '')).strip().upper()
        pop, area = row.get('population'), row.get('area_km2')
        if (not re.fullmatch('[A-Z]{3}', code) or not row.get('name') or
                pop is None or pop < 0 or (area is not None and area < 0)):
            rejected += 1
            continue
        row['iso3'] = code
        row['population'] = int(pop)
        row['source'] = source
        valid[code] = row
    return dict(ok=True, rows=list(valid.values()), rejected=rejected)


def collect(**context):
    results = {s: context['ti'].xcom_pull(task_ids=f'{s}.transform') for s in SOURCES}
    rows = [r for result in results.values() if result and result['ok'] for r in result['rows']]
    if (context['dag_run'].conf or {}).get('simulate_empty_merge'):
        rows = []
    rejected = sum(r.get('rejected', 0) for r in results.values() if r)
    return dict(rows=rows, unique_count=len({r['iso3'] for r in rows}), rejected=rejected,
                successful=[s for s,r in results.items() if r and r['ok'] and r['rows']])


def quality(**context):
    data = context['ti'].xcom_pull(task_ids='merge')
    total = len(data['rows']) + data['rejected']
    passed = data['unique_count'] >= 100 and data['rejected'] / max(total, 1) <= .05
    return 'load' if passed else 'quarantine'


def run_status(run_id, status, count, detail):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('''INSERT INTO pipeline_runs(run_id,status,row_count,detail)
            VALUES(%s,%s,%s,%s) ON CONFLICT(run_id) DO UPDATE SET
            status=excluded.status,row_count=excluded.row_count,detail=excluded.detail,
            checked_at=now()''', (run_id,status,count,detail))


def load(**context):
    data = context['ti'].xcom_pull(task_ids='merge')
    now = datetime.now(timezone.utc)
    grouped = {}
    with connection() as conn, conn.cursor() as cur:
        for row in data['rows']:
            cur.execute('''INSERT INTO country_observations
                (source,iso3,name,population,area_km2,region,population_year,source_url,fetched_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source,iso3)
                DO UPDATE SET name=excluded.name,population=excluded.population,
                area_km2=excluded.area_km2,region=excluded.region,
                population_year=excluded.population_year,source_url=excluded.source_url,
                fetched_at=excluded.fetched_at''',
                tuple(row.get(k) for k in ['source','iso3','name','population','area_km2','region',
                                          'population_year','source_url']) + (now,))
            grouped.setdefault(row['iso3'], []).append(row)
        for code, records in grouped.items():
            priority = {'worldbank':0,'worldometer':1,'geonames':2}
            records.sort(key=lambda r: priority[r['source']])
            pop = records[0]
            area = next((r for r in records if r.get('area_km2') is not None), {})
            region = next((r['region'] for r in records if r.get('region')), 'Unknown')
            cur.execute('''INSERT INTO countries VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(iso3) DO UPDATE SET name=excluded.name,population=excluded.population,
                area_km2=excluded.area_km2,region=excluded.region,population_year=excluded.population_year,
                population_source=excluded.population_source,area_source=excluded.area_source,
                sources=excluded.sources,updated_at=excluded.updated_at''',
                (code,pop['name'],pop['population'],area.get('area_km2'),region,
                 pop.get('population_year'),pop['source'],area.get('source'),
                 json.dumps([r['source'] for r in records]),now))
    for source in data['successful']:
        source_status(context['run_id'],source,'loaded',
                      sum(r['source']==source for r in data['rows']), '')
    run_status(context['run_id'], 'success' if len(data['successful'])==3 else 'degraded',
               data['unique_count'], json.dumps({'successful': data['successful'], 'rejected':data['rejected']}))


def quarantine(**context):
    data = context['ti'].xcom_pull(task_ids='merge')
    run_status(context['run_id'],'quality_failed',data['unique_count'],
               json.dumps({'rejected':data['rejected'],'successful':data['successful']}))
    raise ValueError('Quality gate failed: require >=100 countries and <=5% invalid rows; no data loaded')


with DAG('country_comparison', start_date=datetime(2026,1,1), schedule='@daily',
         catchup=False, max_active_runs=1, default_args={'retries':1,'retry_delay':timedelta(seconds=10)},
         tags=['countries','multi-source']) as dag:
    init = PythonOperator(task_id='initialize', python_callable=initialize)
    merge = PythonOperator(task_id='merge', python_callable=collect)
    for source in SOURCES:
        with TaskGroup(group_id=source) as group:
            e = PythonOperator(task_id='extract',python_callable=extract,op_kwargs={'source':source},show_return_value_in_logs=False)
            t = PythonOperator(task_id='transform',python_callable=transform,op_kwargs={'source':source},show_return_value_in_logs=False)
            e >> t
        init >> group >> merge
    gate = BranchPythonOperator(task_id='quality_gate',python_callable=quality)
    loader = PythonOperator(task_id='load',python_callable=load)
    reject = PythonOperator(task_id='quarantine',python_callable=quarantine,retries=0)
    done = EmptyOperator(task_id='done',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    merge >> gate >> [loader,reject]
    [loader,reject] >> done
