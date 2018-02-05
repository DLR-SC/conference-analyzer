from bs4 import BeautifulSoup
from googletrans import Translator
import urllib.request
from urllib.parse import urljoin, urlsplit, urlparse
import datetime
import lxml
from multiprocessing import Pool
import inspect
import os.path
import re
import DataModels, DBHandler, parseTable
from itertools import product
def get_translator():
    return Translator()
def check_language_of_string(string, translator):
    return translator.detect(string).lang
def check_language_for_series_of_strings(strings, n, translator):
    strings = strings[:n]
    if all(string == strings[0] for string in strings): return check_language_of_string(strings[0], translator)
def check_website_for_language(soup, translator, n=5, tags={'span', 'h2', 'h3', 'a', 'b', 'p'}):
    return check_language_for_series_of_strings(soup.find_all(tags), n, translator)
def get_location_dict(path="data/iso_codes.csv"):
    locations = dict()
    with open(path, "r") as f:
        for line in f:
            locations[line.split(",")[1]] = line.split(",")[0]
        return locations
def get_net_dir(link):
    return urlparse(link).netloc
def get_location_shortcut(string, locations):
    for shortcut in locations:
        if (shortcut.lower().strip() == string.lower()): 
            return shortcut
def getLocation(link, locations):
    locs = list()
    [locs.append(get_location_shortcut(string, get_location_dict())) for string in get_net_dir(link).split(".")]
    locs = filter(None, locs)
    return "".join(locs)
def getRelocatedLink(link):
    headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'}
    request = urllib.request.Request(link, None, headers)
    return urllib.request.urlopen(request).geturl()
def get_soup_for_link(link, parser='lxml'):
        if(link is not None) and (type(link) is str):
            headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'}
            request = urllib.request.Request(link, None, headers)
            return BeautifulSoup(urllib.request.urlopen(request).read(), parser)

def search_website_for_date(link, tags={'span', 'h2', 'h3', 'a', 'b', 'p'}):
    for tag in get_soup_for_link(link).find_all(tags):
        if(tag.string is not None) and (re.search("\d{4}", tag.string) is not None):
            return re.search("\d{4}", tag.string).group(0)
def search_website_for_english_version(soup, link): 
    for i in soup.find_all('a'):
        if(i.string is not None):
            if(i.string.lower() == 'english') or (i.string.lower() == 'en'):
                href = i.get('href').split("/")
                href.remove(href[0])
                link = getRelocatedLink(link).replace("\n", "")
                if(not link.endswith("/")):
                    link = link+"/"
                return get_soup_for_link(link+"/".join(href))
    return get_soup_for_link(link)
#Saves a value to a file at path
def rememberValue(value, path='data/sym.csv'):
    try:
        f = open(path, "r+")
    except (IOError, OSError) as e:
        print(inspect.currentframe().f_code.co_name+"////"+e)
        return False
    if(value is not None) and (f is not None):
        value = value.strip()
        for l in f:
            ar = l.split(",")
            for w in ar:
                if(value.lower() == w.lower()):
                    return None
        f.write(value.lower()+",")
    else:
        raise Exception('Value or File is None')
def rememberValues(values, path):
    try:
        with open(path, "a+") as f:
            f.seek(0, os.SEEK_SET)
            for line in f:
                if(line.split(",")[0].lower() == values[0].lower()):
                    return False
            else:
                f.write(",".join(values)+"\n")
    except (IOError, OSError) as e:
        print(inspect.currentframe().f_code.co_name+"////"+e)
        return False

def get_older_version(soup, link, years = 1):
    if(years > 1):
        soups = list()
    now = datetime.datetime.now()
    yearN = now.year-1
    if(soup == False):
        return False
    while yearN >= now.year-years:
        for i in soup.find_all('a'):
            if(i.string is None) or (i.get('href') is None):
                continue
            if(str(yearN) in i.string) or (str(yearN) in i.get('href')):
                if(years > 1):
                    soups.append(i.get('href'))
                else:
                    href = i.get('href')
                    url_split = urlsplit(urljoin(link, href))
                    url_year_split = url_split.path.split("/")[1]+"/"
                    if not (url_year_split in link):
                        return_link = urljoin(link, url_year_split)
                    else:
                        return_link = urljoin(urlsplit(link).netloc, url_split.path.split("/")[1]+"/")
                    return return_link
        yearN -= 1
    else:
        if(years > 1) and (len(soups) > 0):
            return soups
        try:
            r = urllib.request.urlopen(link)
            fUrl = r.geturl()
            if("8" in fUrl):
                return fUrl.replace("8", "7")
        except (AttributeError, urllib.error.HTTPError) as e:
            print(inspect.currentframe().f_code.co_name)
            print(e)
            return False
    return False

def get_search_words(soup, link, translator, language="en", path="data/en.csv"):
    with open(path, "r+") as f:
        fileWords = f.read().split(",")
        search_terms = list()
        if(language != "en"):
            for word in fileWords:
                search_terms.append(word)
                search_terms.append(translator.translate(text=word, dest=language).text)
        else:
            search_terms = fileWords
        return search_terms
def get_not_empty_a_tags(soup):
    valid_tags = list()
    for tag in soup.find_all('a'):
        if(tag.string is not None) and (tag.get('href')):   valid_tags.append(tag)
    return valid_tags

def check_if_tag_contains_schedule_link(tag, search_terms):
    for search_term in search_terms:
        if(search_term in tag.string) or (search_term in tag.get('href')):
            return True

def getSchedule(link, translator = Translator(), language='en'):
    soup = get_soup_for_link(link)
    language = check_website_for_language(soup, translator)
    if(language != 'en'):
        soup = search_website_for_english_version(soup, link)
    language = check_website_for_language(soup, translator)
    search_terms = get_search_words(soup, link, translator)
    for tag in get_not_empty_a_tags(soup):
        if(check_if_tag_contains_schedule_link(tag, search_terms)):
            return urljoin(link, tag.get('href'))
    else:
        older_link = get_older_version(soup, link)
        if(type(older_link) is str):
            return getSchedule(older_link, translator)
'''translator = Translator()
urls = open("urls.txt", "r")'''
def test(a):
    a = a.replace("\n", "")
    schedule = getSchedule(a)
    if(schedule is None):
        schedule = ""
    return a.replace("\n", "")+"####"+schedule.replace("\n", "")
def part_of_series(name):
    words = name.split()
    for word in words:
        if (word == get_location_shortcut(name, get_location_dict())) or (re.search("\d{4}", name) is not None):
            words.remove(word)
    with open("data/conferences.csv", "a+") as f:
        for line in f:
            if(line == "".join(words)):
                return True, "".join(words)
        else:
            f.write("".join(words)+"\n")
            return False, "".join(words)

def multiprocessing_test(titles, link, insertData):
    poola = Pool(processes=4)
    for title in titles:
        poola.apply_async(scrape_and_insert_data, args=(title, link, insertData, titles))
        print("Thread Openend")
    poola.close()
    poola.join()

def scrape_and_insert_data(title, link, db, titles):
    print("bitte")
    c = DataModels.Conference()
    c.extract_conference_informations(link)
    partOf, conference_series_name = part_of_series(c.name)
    if(partOf):
        cs = DataModels.Conference_Series(conference_series_name, 1)
    else: cs = False
    person_name = title
    to = DataModels.Topic("", "en")
    db.insert_data(person_name, c, cs, DataModels.Talk().extract_information_from_title(title), to)
if __name__ == "__main__":
    insertData = DBHandler.insert_data("bolt://127.0.0.1:7687", "neo4j", "Gjekgi75")
    link = "http://de.pycon.org/"
    topic = parseTable.TableParser(getSchedule(link)).parse()
    #scrape_and_insert_data(topic['Alex Conway'], link, insertData, topic)
    multiprocessing_test(topic, link, insertData)