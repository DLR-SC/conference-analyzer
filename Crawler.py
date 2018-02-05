from bs4 import BeautifulSoup, element
from googletrans import Translator
import urllib.request
from urllib.parse import urljoin, urlsplit, urlparse
import datetime
import lxml
import multiprocessing
from multiprocessing import pool
import inspect
import os.path
import re
import parseTable
import DataModels
class Utilities(object):
    def check_language_of_string(str, translator):
        return translator.detect(i.string).lang
    def check_language_for_series_of_strings(strings, n, translator):
        strings = strings[:n]
        if all(string == string[0] for string in strings): return check_language_of_string(strings[0])
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
    @staticmethod
    def getLocationShortcut(string, locations):
        for shortcut in locations:
            if (shortcut.lower().strip() == string.lower()): 
                return shortcut
    @staticmethod
    def getLocation(link, locations):
        locs = list()
        [locs.append(Utilities.getLocationShortcut(string, Utilities.get_location_dict())) for string in Utilities.get_net_dir(link).split(".")]
        locs = filter(None, locs)
        return "".join(locs)
    def getRelocatedLink(link):
        headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'}
        request = urllib.request.Request(link, None, headers)
        return urllib.request.urlopen(request).geturl()

def getSoup(link, parser='lxml'):
    '''Gets a website by a Link and returns it as a BeautifulSoup object
    Parameters
    ----------
    link : str
        Link to the Site
    parser : str
        Which parser to use (Default 'lxml' for html)
    '''
    try:
        if(link is not None) and (type(link) is str):
            headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'}
            request = urllib.request.Request(link, None, headers)
            response = urllib.request.urlopen(request)
            return BeautifulSoup(response.read(), parser)
        else:
            raise Exception('Be sure to check if Link is Empty or not a string \n Link: '+str(link))
    except (urllib.error.URLError, UnicodeError) as e:
        print(e)
        print("at "+link)
        return False


def getEnglishVersion(soup, link):
    '''Searches a given Website for a link for a possible English instance of the Site. If nothing is found the original link is returned.
    Parameters
    ----------
    soup : BeautifulSoup
        BeautifulSoup object of the given site
    link : str
        Link to the Site
    '''
    for i in soup.find_all('a'):
        if(i.string is not None):
            if(i.string.lower() == 'english') or (i.string.lower() == 'en'):
                href = i.get('href').split("/")
                href.remove(href[0])
                link = getRelocatedLink(link).replace("\n", "")
                if(not link.endswith("/")):
                    link = link+"/"
                return link+"/".join(href)
    return link
#Saves a value to a file at path
def rememberValue(value, path='data/sym.csv'):
    '''Add value to a File in the following scheme: "value,"
    Parameters
    ----------
    value : str
        The value to be added to the File
    path : str
        Path of the File to be saved to (Default is 'data/sym.csv')
    '''
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
        return False
def rememberValues(values, path):
    '''Like rememberValue but for lists
    Parameters
    ----------
    values : list
        The values to be added to the File
    path : str
        Path of the File to be saved to (Default is 'data/sym.csv')
    '''
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

def getOlderVersion(soup, link, years = 1):
    '''Searches a given site for an older instance
    Parameters
    ----------
    soup = BeautifulSoup
        BeautifulSoup object of the given site
    link : str
        The Link to the Site
    years : int
        How many years you want to go back in time
    '''
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


def getSchedule(link, translator = Translator(), language='en'):
    '''Searches a given site for a schedule/program
    Parameters
    ----------
    link : str
        The Link to the Site
    translator : Translator
        googltrans API object required to detect and switch languages. (Default is Translator())
    language : str
        The language of the site. Pass shortcuts here. e.g. English -> en (Default is en)
    '''
    soup = getSoup(link)
    original_link = link
    language = identLanguage(soup, translator)
    f = open("data/en.csv", "r")
    if(language != 'en'):
        link = getEnglishVersion(soup, link)
        if(link is not None) and (link != original_link):
            soup = getSoup(link)
            language = 'en'
    fileWords = f.read().split(",")
    fileWords.remove(fileWords[len(fileWords)-1])
    search_terms = list()
    if(language != "en"):
        for word in fileWords:
            search_terms.append(word)
            try:
                search_terms.append(translator.translate(text=word, dest=language).text)
            except AttributeError as e:
                print(inspect.currentframe().f_code.co_name)
                print(e)
    else:
        search_terms = fileWords
    try:
        for a_tag in soup.find_all('a'):
            if(a_tag.string is not None) and (a_tag.get('href') is not None):
                for search_term in search_terms:
                    if(search_term in a_tag.string.lower()) and ('tutorial' not in a_tag.string.lower()) and ("#top" not in a_tag.get('href')):
                        href_array = a_tag.get('href').split("/")
                        if(href_array[len(href_array)-1] != "#"):
                            rememberValue(href_array[len(href_array)-1], "data/en.csv")
                        return urljoin(link, a_tag.get('href'))
                for search_term in search_terms:
                    if(search_term in a_tag.get('href').lower()) and ("tutorial" not in a_tag.get('href').lower()):
                        if(a_tag.string != "#"):
                            rememberValue(a_tag.string, "data/en.csv")
                        return urljoin(link, a_tag.get('href'))
    except AttributeError as e:
        print(inspect.currentframe().f_code.co_name)
        print(e)
    o = getOlderVersion(soup, link)
    if(type(o) is str):
        return getSchedule(o, translator)



    

'''translator = Translator()
urls = open("urls.txt", "r")'''
def test(a):
    a = a.replace("\n", "")
    schedule = getSchedule(a)
    if(schedule is None):
        schedule = ""
    return a.replace("\n", "")+"####"+schedule.replace("\n", "")
    rememberValues([a.replace("\n", ""), schedule.strip()], "data/schedules.csv")
def part_of_series(name):
    words = name.split()
    try:
        words.remove(getLocationShortcut(name))
    except ValueError as e:
        print(e)
    words.remove(re.search("\d{4}", name).group(0))
    with open("data/conferences.csv", "a+") as f:
        for line in f:
            if(line == "".join(words)):
                return True, "".join(words)
        else:
            f.write("".join(words)+"\n")
            return False, "".join(words)
def scrape_and_insert_data(title, link, db, titles):
    c = DataModels.Conference()
    c.extract_conference_informations(link)
    partOf, csName = part_of_series(c.name)
    if(partOf):
        cs = Conference_Series(c.name, 1)
    else: cs = False
    person_name = tit
    db.insert_data(person_name, c, cs, Talk.extract_information_from_title(topic[title]), to)
    print("="*30)
    return True
def multiprocessing_test(titles, link, insertData):
    poola = pool.Pool(processes=4)
    for title in titles:
        thread = poola.apply_async(scrape_and_insert_data, args=(title, link, insertData, titles))
        print("Thread Openend")
    poola.close()
    poola.join()


if __name__ == "__main__":
    insertData = insertData("bolt://127.0.0.1:7687", "neo4j", "Gjekgi75")
    link = "http://de.pycon.org/"
    topic = parseTable.TableParser(getSchedule(link)).parse()
    multiprocessing_test(topic, link, insertData)