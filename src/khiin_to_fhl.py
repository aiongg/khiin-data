import argparse
from datetime import datetime
import re
import sqlite3
import time
import unicodedata

ASCII_SUBS = [
    (r'\u207F', 'nn'),
    (r'\u0358', 'u'),
    (r'\u0301', '2'),
    (r'\u0300', '3'),
    (r'\u0302', '5'),
    (r'\u0304', '7'),
    (r'\u030D', '8'),
    (r'\u0306', '9'),
    (r'\u0324', 'r'),
]

KIP_SUBS = [
    (r'ch', 'ts'),
    (r'oa', 'ua'),
    (r'oe', 'ue'),
    (r'ou', 'oo'),
    (r'eng', 'ing'),
    (r'ek', 'ik'),
]

def get_cursor(file):
    con = sqlite3.connect(file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    return cur

def poj_to_ascii(text):
    text = unicodedata.normalize('NFD', text)
    for sub in ASCII_SUBS:
        text = re.sub(sub[0], sub[1], text)
    text = re.sub(r'([A-Za-z]+)(\d)([A-Za-z]+)', r'\1\3\2', text).lower()
    return text

def poj_to_qstring(text):
    text = poj_to_ascii(text)
    if re.search(r' ', text) is not None:
        text = re.sub(r'\d| +', '', text)
    return text

def poj_to_reading(text):
    text = poj_to_ascii(text)
    for sub in KIP_SUBS:
        text = re.sub(sub[0], sub[1], text)
    text = re.sub(r' ', '-', text)
    return text

def get_freq_data(cur):
    res = cur.execute("select * from frequency")
    freq = res.fetchall()
    return freq

def get_conv_data(cur, id):
    res = cur.execute("select * from conversions where input_id = ?", [id])
    conv = res.fetchall()
    return conv

def has_non_hanji(text):
    text = unicodedata.normalize('NFD', text)
    return re.search(r'[A-Za-z]', text) is not None

def get_wordlist(db_cur):
    word_list = []
    freq = get_freq_data(db_cur)
    for freq_row in freq:
        input = freq_row['input']
        conv = get_conv_data(db_cur, freq_row['id'])
        for conv_row in conv:
            output = conv_row['output']

            # if has_non_hanji(output):
            #     continue

            word_list.append({ 'reading': poj_to_reading(input),'qstring': poj_to_qstring(input), 'value': output, })

    return word_list

def build_db(file, word_list):
    now = datetime.now()
    con = sqlite3.connect(file)
    con.executescript(f'''
        DROP TABLE IF EXISTS words;
        DROP TABLE IF EXISTS qstring_word_mappings;
        DROP TABLE IF EXISTS cooked_information;
        CREATE TABLE words (id INTEGER PRIMARY KEY, reading, value, probability);
        CREATE TABLE qstring_word_mappings (qstring, word_id);
        CREATE TABLE cooked_information (key, value);
        INSERT INTO cooked_information VALUES
            ('version_timestamp', '{now.strftime("%Y%m%d")}'),
            ('cooked_timestamp_utc', '{round(time.time(), 1)}'),
            ('cooked_datetime_utc', '{now.strftime("%Y-%m-%d %H:%M UTC")}');

        CREATE INDEX words_index_key ON words (reading);
        CREATE INDEX qstring_word_mappings_index_qstring ON qstring_word_mappings (qstring);
    ''')

    words_table = []
    qstrings_table = []

    for i, row in enumerate(word_list):
        id = i + 1
        words_table.append((id, row['reading'], row['value'], 1))
        qstrings_table.append((row['qstring'], id))

    c = con.cursor()
    c.executemany('INSERT INTO words VALUES (?, ?, ?, ?)', words_table)
    c.executemany('INSERT INTO qstring_word_mappings VALUES (?, ?)', qstrings_table)
    con.commit()

cin_top = """%ename chailaiji:en;
%ename 台語
%selkey 123456789
%keyname begin
a a
b b
c c
d d
e e
f f
g g
h h
i i
j j
k k
l l
m m
n n
o o
p p
q q
r r
s s
t t
u u
v v
w w
x x
y y
z z
1 1
2 2
3 3
4 4
5 5
6 6
7 7
8 8
9 9
%keyname end
%chardef begin
"""

cin_bottom = "%chardef end"

def build_txt(file, word_list):
    seen = []
    with open(file, 'w', encoding='utf8') as out:
        out.write(cin_top)
        for row in word_list:
            qstr = row['qstring']
            val = row['value']
            if val.find(' ') > -1:
                val = val.replace(' ', '-')
            if (qstr, val) not in seen:
                out.write(qstr + ' ' + val + "\n")
                seen.append((qstr, val))
        out.write(cin_bottom)

##############################################################################
#
# __main__
#
##############################################################################

parser = argparse.ArgumentParser(
    description="""Convert the Khiin database to FHL format""",
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('-i', "--input", metavar='FILE', required=True, help='the khiin database file (khiin.db)')
parser.add_argument('-o', "--output", metavar='FILE', required=False, help='the output database file (TalmageOverride.db)')
parser.add_argument('-c', "--cin", metavar="FILE", required=False, help="The .cin output file (chailaiji.cin)")

if __name__ == '__main__':
    args = parser.parse_args()
    input_file = args.input
    output_file = args.output if args.output else 'out/TalmageOverride.db'
    cur = get_cursor(input_file)
    word_list = get_wordlist(cur)
    build_db(output_file, word_list)

    if args.cin:
        build_txt(args.cin, word_list)
