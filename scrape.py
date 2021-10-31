import requests

from bs4 import BeautifulSoup
import pandas as pd
import re

import json
import uuid
import pickle
import time
import random
from collections import OrderedDict
from pathlib import Path

cached_filename = "./cache/cache"
cached_directory = "./cache"

def cached_request(root_url, headers, data, method=requests.post):

    hassh = "".join([json.dumps(d, sort_keys=True) for d in [root_url, data]])
    

    for line in open(cached_filename):
        if hassh in line:
            h, filename = line.split("|")
            filename = filename.strip()
            print("Using cached request!")
            return pickle.load(open(filename, 'br'))

    response = method(root_url, headers=headers, data=data)
    filename = cached_directory + "/" + str(uuid.uuid4())
    while (Path(filename).is_file()):
        filename = cached_directory + "/" + str(uuid.uuid4())
    pickle.dump(response, open(filename,'bw'))
    fp = open(cached_filename, 'a')
    fp.write(f"{hassh}|{filename}\n")

    sleep_time = 5 + random.random()
    print(f"Sleeping for {sleep_time}")
    time.sleep(sleep_time)
    
    return response
    

root_url = "https://www.brewersfriend.com/search/index.php"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'}
data = {'keyword': '',
        'method': '',
        'units': 'metric',
        'style': '',
        'oglow': '',
        'oghigh': '',
        'fglow': '',
        'fghigh': '',
        'ibulow': '',
        'ibuhigh': '',
        'abvlow': '',
        'abvhigh': '',
        'colorlow': '',
        'colorhigh': '',
        'batchsizelow': '',
        'batchsizehigh': '',
        'sort': '',
        'page': 2}

def dig(content):
    if type(content) in [int, str]:
        return str(content)
    if content.find("span") is not None:
        return dig(content.find("span"))
    elif content.find("a") is not None:
        return dig(content.find("a"))
    elif content.find("strong") is not None:
        return dig(content.find("strong"))
    if len(content.contents) > 0:
        return content.contents[0]
    return ""

def remove_excess(line):
    line = re.sub("[\t\s]+", " ", line)
    line = re.sub("[:]{1}", "", line)
    return line.strip()

def get_recipe_details(relative_url):
    url = f"https://www.brewersfriend.com{relative_url}"
    response = cached_request(url, headers, {}, method=requests.get)

    doc = BeautifulSoup(response.text, features="html.parser")

    tables = {}

    for match in doc.find_all("div", {'class': "brewpart", "id": ["water", "hops", "fermentables"]}):
        brewpart_id = match["id"]

        match = match.find("table")
        
        columns = []
        for col in match.find("tr").find_all("th"):
            column_name = remove_excess(dig(col))
            columns.append(column_name)

        rows = []
        for table in match.find_all("tr")[1:-1]:
            row = []
            for row_entry in table.find_all("td"):
                value = remove_excess(dig(row_entry))
                row.append(value)
            rows.append(row)

        df = pd.DataFrame(rows, columns=columns)
        tables[brewpart_id] = df

    for match in doc.find_all("div", {'class': "brewpart", "id": ["yeasts"]}):
        brewpart_id = match["id"]

        columns = []
        row = []
        for table in match.find_all("tr"):
            if table.find("div", "brewpartlabel") is None:
                continue

            if table.find("tr") is not None:
                continue

            for row_entry in table.find_all("td"):
                if row_entry.find("div", "brewpartlabel") is not None:
                    what = remove_excess(dig(row_entry.find("div", "brewpartlabel")))
                    columns.append(what)
                else:
                    value = remove_excess(dig(row_entry))
                    row.append(value)

        df = pd.DataFrame([row], columns=columns)    
        tables[brewpart_id] = df

    columns = []
    row = []
    match = doc.find("div", {'class':'description'})
    print(match.prettify())
    for item in match.find_all("span", {'class':'viewStats'}):
        key = remove_excess(dig(item.contents[1]))
        value = remove_excess(dig(item.contents[3]))
        
        if (len(item.contents) > 4):
            print(item.contents)
            value = value + " " + remove_excess(dig(item.contents[4]))
            #TODO
            
        columns.append(key);
        row.append(value);

    print(columns)
    print(row)
    tables["description"] = pd.DataFrame(row, columns=columns)
        
    print(url)
    #print(doc.prettify())
    print(tables)
    sys.exit(1)


def store_data(page):
    data['page'] = page
    response = cached_request(root_url, headers, data)

    doc = BeautifulSoup(response.text, features="html.parser")
    
    columns = []
    for match in doc.find_all("th"):
        column_name = match.find("span").contents[0]
        column_name = re.sub("[\t\s]+", " ", column_name)
        columns.append(column_name)
    columns.append("expand-id")
    columns.append("href")


    rows = []
    expanded_jsons = []
    for match in doc.find_all("tr", {'class': ['odd', 'even']}):
        row = []
        for i, td in enumerate(match.find_all("td")):
            
            if columns[i] == "Title":
                if len(td.find("a","recipetitle").contents) < 1:
                    content = ""
                else:
                    content = td.find("a","recipetitle").contents[0]
            elif columns[i] == "Style":
                link = td.find("a")
                if link is not None:
                    content = td.find("a").contents[0]
                else:
                    content = td.contents[0]
            else:
                content = td.contents[0]

            content = re.sub("[\t\s]+", " ", content)

            content = content.strip()
            row.append(content)
        
        expand_id = match.find("span", "expand-recipe")['data-expand-id']
        href = match.find("a","recipetitle")['href']
        
        expand_id = re.sub("[\t\s]+", " ", expand_id)
        href = re.sub("[\t\s]+", " ", href)

        df_details = get_recipe_details(href)
    
        row.append(expand_id)
        row.append(href)
        rows.append(row)


        expanded_info = doc.find("tr", {'id': expand_id})
        if (expanded_info):
            expanded_json = {}
            table = expanded_info.find("table")
            if table:
                for info in list(table.find_all("tr"))[2:]:
                    for td in info.find_all("td"):
                        what = td.find("b").contents[0]
                        content = td.contents[1].strip()
                        
                        what = what.replace("\n", " ")
                        content = content.replace("\n", " ")

                        what = re.sub("[\t\s:]+", " ", what)
                        content = re.sub("[\t\s]+", " ", content)
                        
                        what = what.strip()
                        content = content.strip()
                        
                        expanded_json[what] = content
                        
            expanded_json["expand-id"] = expand_id
            expanded_jsons.append(expanded_json)

    #print(doc)
    #print(pd.DataFrame(rows, columns=columns))
    #print(pd.DataFrame(expanded_jsons))
    df = pd.DataFrame(rows, columns=columns).set_index("expand-id").join(pd.DataFrame(expanded_jsons).set_index("expand-id"), lsuffix="_").reset_index()

    df.to_feather(f"./data/per-page/page_{data['page']}.feather")
    df.to_csv(f"./data/per-page/page_{data['page']}.csv")


nums = list(range(1, 4746))
random.shuffle(nums)
nums=[89]
for page in nums:
    print(page)
    store_data(page)
