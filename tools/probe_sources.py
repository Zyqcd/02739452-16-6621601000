"""Read-only network diagnostics; does not bypass robots restrictions."""
import urllib.request

urls = [
    'https://www.worldometers.info/robots.txt',
    'https://www.worldometers.info/world-population/population-by-country/',
    'https://en.wikipedia.org/robots.txt',
    'https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population',
    'https://www.geonames.org/robots.txt',
    'https://www.geonames.org/countries/',
    'https://restcountries.com/robots.txt',
    'https://restcountries.com/v3.1/all?fields=name,cca3,population,area,region',
    'https://api.worldbank.org/robots.txt',
    'https://api.worldbank.org/v2/country?format=json&per_page=400',
    'https://api.restcountries.com/robots.txt',
    'https://api.restcountries.com/countries/v5?fields=codes,names,population,area,region',
]
for url in urls:
    try:
        request = urllib.request.Request(url, headers={'User-Agent':'CountryComparisonStudentProject/1.0'})
        with urllib.request.urlopen(request, timeout=30) as response:
            print(url, response.status, response.read(500).decode())
    except Exception as exc:
        print(url, type(exc).__name__, str(exc))
