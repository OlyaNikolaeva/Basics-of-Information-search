from multiprocessing import Pool
import pandas as pd
import collections
import math


def termsIDF(folder, docsCount):
    indxf = open('{0}\windex.txt'.format(folder), "r", encoding='utf-8')
    lines = indxf.readlines()

    idf_terms = {}
    for line in lines:
        split = line.split()
        key = split[0]
        docs = split[1].split(',')
        idf_terms[key] = round(math.log10(docsCount / len(docs)), 5)

    return idf_terms


def dfLine(i, term, tf, idf):
    return {'Document': i, 'Term': term, 'tf': tf, 'idf': idf, 'tf-idf': round(tf * idf, 5)}


def docTermsTF(docName, terms):
    global lemmasFolder

    tf_terms = {}
    f = open('{0}\{1}.txt'.format(lemmasFolder, docName), "r", encoding='utf-8')
    docTerms = f.readlines()
    docTerms = [term.split('\n')[0] for term in docTerms]
    termsCount = len(docTerms)
    termsCounter = collections.Counter(docTerms)
    for term in terms:
        tf_terms[term] = round(termsCounter[term] / termsCount, 5)
    return tf_terms


def docDataFrame(i):
    global terms, idf_terms
    print(i)
    tf_terms = docTermsTF(i, terms)
    df = pd.DataFrame(columns=['Document', 'Term', 'tf', 'idf', 'tf-idf'])
    for term in tf_terms:
        tf = tf_terms[term]
        idf = idf_terms[term]
        df = df.append(dfLine(i, term, tf, idf), ignore_index=True)
    return df


folder = r'...\IS'
lemmasFolder = r'...\IS\Lemmas'

tdf = pd.DataFrame(columns=['Document', 'Term', 'tf', 'idf', 'tf-idf'])
idf_terms = termsIDF(folder, 100)
indxf = open('{0}\windex.txt'.format(folder), "r", encoding='utf-8')
lines = indxf.readlines()
terms = [line.split()[0] for line in lines]

if __name__ == '__main__':
    doc = [i for i in range(1, 101)]
    with Pool(4) as pool:
        res = pool.map(docDataFrame, doc)
        tdf = tdf.append(res, ignore_index=True)
    try:
        tdf.to_excel('tdf.xlsx')
    except ValueError as error:
        part1 = int(len(tdf) / 2)
        tdf.head(part1).to_excel('{0}\\tdf1.xlsx'.format(folder))
        tdf.tail(len(tdf) - part1).to_excel('{0}\\tdf2.xlsx'.format(folder))
    print('finish!')