import csv
import locale
import os
from pathlib import Path
locale.setlocale(locale.LC_ALL, '')

CONV_FILE = 'data/conversion.csv'
FREQ_FILE = 'data/frequency.csv'
SQL_FILE = 'khiin_test_db.sql'

PROJECT_FOLDER = Path(os.path.realpath(__file__)).parent.parent
OUTPUT_FOLDER = Path.joinpath(PROJECT_FOLDER, 'out')
CONV_FILE = Path.joinpath(PROJECT_FOLDER, CONV_FILE)
FREQ_FILE = Path.joinpath(PROJECT_FOLDER, FREQ_FILE)
SQL_FILE = Path.joinpath(OUTPUT_FOLDER, SQL_FILE)

##############################################################################
#
# Utilities
#
##############################################################################

def ensure_directory(path):
    if not path.exists():
        Path.mkdir(path)

def has_hanji(string):
    return any(ord(c) > 0x2e80 for c in string)

##############################################################################
#
# CSV parsing and data collection
#
##############################################################################

def parse_freq_csv(csv_file):
    data = []
    with open(csv_file) as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        data = list(reader)
    for x in data:
        x['freq'] = int(x['freq'])
    data = [x for x in data if x['freq'] != 0]
    return sorted(data, key=lambda x: locale.strxfrm(x['input']))

def parse_gisu_csv(csv_file):
    data = []
    with open(csv_file) as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        data = list(reader)
    for x in data:
        if has_hanji(x["output"]):
            x["weight"] = 1000
    return sorted(data, key=lambda x: locale.strxfrm(x['input']))

def find_common_inputs(freq, gisu):
    freq_has_gisu = []
    gisu_has_freq = []

    for word in freq:
        if any(x['input'] == word['input'] for x in gisu):
            freq_has_gisu.append(word)
    
    for word in gisu:
        if any(x['input'] == word['input'] for x in freq_has_gisu):
            gisu_has_freq.append(word)
    
    return [freq_has_gisu, gisu_has_freq]

def all_syllables(freq, gisu):
    ret = set()
    for x in freq:
        for syl in x["input"].split(' '):
            ret.add(syl)
    for x in gisu:
        for syl in x["input"].split(' '):
            ret.add(syl)
    return sorted(list(ret), key=locale.strxfrm)

##############################################################################
#
# SQL builder functions
#
##############################################################################

def freq_row_sql(row):
    return f'("{row["input"]}", {row["freq"]}, {row["chhan_id"]})'

def freq_sql(data):
    sql = """DROP TABLE IF EXISTS "frequency";
CREATE TABLE IF NOT EXISTS "frequency" (
	"id"        INTEGER PRIMARY KEY,
    "input"     TEXT NOT NULL,
	"freq"      INTEGER,
	"chhan_id"  INTEGER,
	UNIQUE("input")
);
INSERT INTO "frequency" ("input", "freq", "chhan_id") VALUES\n"""
    values = [freq_row_sql(x) for x in data]
    sql += ',\n'.join(values) + ';\n'
    return sql

def gisu_row_sql(row):
    return f'("{row["input"]}", "{row["output"]}", {row["weight"]})'

def gisu_sql(data):
    sql = """DROP TABLE IF EXISTS "dictionary";
CREATE TABLE "dictionary" (
	"id"           INTEGER PRIMARY KEY,
	"chhan_id"     INTEGER,
	"input"        TEXT NOT NULL,
	"output"       TEXT NOT NULL,
	"weight"       INTEGER,
	"input_length" INTEGER,
	"category"     INTEGER,
	"annotation"   TEXT,
	UNIQUE("input","output"),
    FOREIGN KEY("input") REFERENCES "frequency"("input")
);
INSERT INTO "dictionary" ("input", "output", "weight") VALUES\n"""
    values = [gisu_row_sql(row) for row in data]
    sql += ',\n'.join(values) + ';\n'
    return sql

def syls_sql(data):
    sql = """DROP TABLE IF EXISTS "syllables";
CREATE TABLE "syllables" (
    "id" INTEGER PRIMARY KEY,
    "input" TEXT NOT NULL,
    UNIQUE("input")
);
INSERT INTO "syllables" ("input") VALUES\n"""
    values = ',\n'.join([f'("{x}")' for x in data])
    sql += values + ';\n'
    return sql

def build_sql(freq, gisu, syls):
    sql = """BEGIN TRANSACTION;
DROP TABLE IF EXISTS "version";
CREATE TABLE IF NOT EXISTS "version" (
    "key"	TEXT,
    "value"	INTEGER
);
DROP TABLE IF EXISTS "unigram_freq";
CREATE TABLE IF NOT EXISTS "unigram_freq" (
    "id"	INTEGER,
    "gram"	TEXT NOT NULL UNIQUE,
    "n"	INTEGER NOT NULL,
    PRIMARY KEY("id")
);
DROP INDEX IF EXISTS "unigram_freq_gram_idx";
CREATE INDEX IF NOT EXISTS "unigram_freq_gram_idx" ON "unigram_freq" (
    "gram"
);
DROP TABLE IF EXISTS "bigram_freq";
CREATE TABLE IF NOT EXISTS "bigram_freq" (
    "id"	INTEGER,
    "lgram"	TEXT,
    "rgram"	TEXT,
    "n"	INTEGER NOT NULL,
    PRIMARY KEY("id"),
    UNIQUE("lgram","rgram")
);
DROP INDEX IF EXISTS "bigram_freq_gram_index";
CREATE INDEX IF NOT EXISTS "bigram_freq_gram_index" ON "bigram_freq" (
    "lgram",
    "rgram"
);\n"""
    sql += freq_sql(freq)
    sql += gisu_sql(gisu)
    sql += syls_sql(syls)
    sql += "COMMIT;\n"
    return sql

def write_sql(sql_file, sql):
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write(sql)

##############################################################################
#
# __main__
#
##############################################################################

if __name__ == "__main__":
    ensure_directory(OUTPUT_FOLDER)
    freq = parse_freq_csv(FREQ_FILE)
    gisu = parse_gisu_csv(CONV_FILE)
    
    [common_freq, common_gisu] = find_common_inputs(freq, gisu)
    syllables = all_syllables(freq, gisu)

    sql = build_sql(common_freq, common_gisu, syllables)
    write_sql(SQL_FILE, sql)

    print(f"""Output written to {SQL_FILE}:
 - {len(common_freq)} inputs ("frequency" table)
 - {len(common_gisu)} tokens ("conversion" table)
 - {len(syllables)} syllables ("syllables" table)""")
