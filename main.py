# json ๋ถํ 
'''
import ijson

SOURCE_FILE = "namuwiki210301/namuwiki_20210301.json"
capture_values = [
    ("item.namespace", "string"),
    ("item.title", "string"),
    ("item.text", "string")
]


def parse_namuwiki_json(limit=-1, debug=False):
    i = 0
    doc = {}
    with open(SOURCE_FILE,"rt",encoding="utf-8") as f:
        for prefix, event, value in ijson.parse(f):

            if debug:
                print(prefix, event, value)

            if (prefix, event) in capture_values:
                doc[prefix[5:]] = value
            if (prefix, event, value) == ("item", "end_map", None):
                yield doc
                doc = {}
                i += 1
                if limit > 0 and i >= limit:
                    break

i=0
for doc in parse_namuwiki_json(10000000, debug=False):
    i+=1
    if i%1000==0: print(i)
    #print(doc['text'])
    #print("===" * 10)
    #doc['text'] = extract_text(doc['text'])
    if not (doc['text'].startswith("#redirect")):
        doc['title']=doc['title'].replace(":","").replace("\\","").replace("/","โ").replace("*","๐").replace("?","๏ผ").replace("\"","โ").replace("<","๏นค").replace(">","๏นฅ").replace("|","โ")
        output = open(f"outputs/{doc['title']}.txt", "w", encoding="utf-8")
        doc['text'] = doc['text'].replace("[๋ชฉ์ฐจ]","")
        output.write(doc["title"]+"\n"+doc['text']+"\n")
        output.close()
'''
# ์ซ์ ์ถ์ถ
from math import log10
'''
import os, re
import time
from threading import Thread

output = open("output.txt", "w", encoding="utf-8")
file_path = "C:/Programming/namuwiki"
file_list = os.listdir(file_path)
file_len = len(file_list)
cnt = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def worker(id):
    i = id
    w_cnt = 0
    while i < file_len:
        w_cnt += 1
        file_name = file_list[i]
        file = open(file_path + "/" + file_name, "r", encoding="utf-8")
        filedata = file.read()
        filedata = re.sub(
            "\.|[0-9]{2,4}๋|[0-9]{1,2}์|[0-9]{1,2}[์.][^\s]{1,2}[0-9]{1,2}์ผ|[0-9]{4}\-[0-9]{2}-[0-9]{2}|[0-9]{1,2}์|[0-9]{1,2}์ [0-9]{1,2}๋ถ|[#][a-zA-Z0-9]{6}|\[youtube\([a-zA-Z0-9]+\)|\[\[http[^]]+|\[\[ํ์ผ:[^]]+|<[a-zA-Z0-9]+>|<[\-a-zA-Z0-9][a-zA-Z0-9]+>|1[sS][tT]|2[nN][dD]|3[rR][dD]|[4-9][tT][hH]|\{\{\{[+-][0-9]|[0-9.]+%|[0-9.]+[ ~][0-9.]+%",
            "", filedata, flags=re.MULTILINE)
        filedata = filedata.replace("0", "")
        results = re.findall(r'\d+', filedata)
        file.close()

        file_result = [0 for _ in range(10)]
        for result in results:
            file_result[int(result[0])] += 1
        for j in range(10):
            cnt[j] += file_result[j]
        # output.write(f"{file_name[:-4]} : {str(file_result)}\n")
        i += 20
        if w_cnt % 100 == 0: print(f"{id}: {w_cnt}")


start = time.time()
threads = []
for i in range(20):
    t = Thread(target=worker, args=[i])
    t.start()
    threads.append(t)
for thread in threads:
    thread.join()

print(f"{time.time() - start:.5f}sec")
print(str(cnt))
print(len(file_list))
'''
import pandas as pd
from scipy import stats
cnt = [14377845, 11271107, 6299994, 4149820, 3860590, 2433708, 2030557, 1853162, 1581374]
actual = []
for i in range(9):
    actual.append(cnt[i]/sum(cnt)*100)
print(actual)
ex = []
for i in range(1,10):
      ex.append(log10(1+1/i)*100)
print(ex)
print(sum(actual)-sum(ex))
chis = stats.chisquare(actual, ex)
print(chis)
