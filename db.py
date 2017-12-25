import os
import sqlite3
import psycopg2
from psycopg2 import sql

# Connect to an existing database
#conn = psycopg2.connect("dbname=test user=postgres")

def remake_db():

    #with contextlib.closing(sqlite3.connect(db_file)) as con:
    with psycopg2.connect("dbname=musicdb user=bxu") as con:

        with con: # auto-commits
                with con.cursor() as cur:
                    cur.execute('''DROP VIEW IF EXISTS all_joined_view''')
                    cur.execute('''DROP VIEW IF EXISTS all_view''')
                    cur.execute('''DROP VIEW IF EXISTS concordance_composition_joined_view''')
                    cur.execute('''DROP VIEW IF EXISTS concordance_composition_view''')
                    cur.execute('''DROP VIEW IF EXISTS composition_composer_view''')
                    cur.execute('''DROP TABLE IF EXISTS composition_composer''')
                    cur.execute('''DROP TABLE IF EXISTS composition_concordance_entry''')
                    cur.execute('''DROP TABLE IF EXISTS composer''')
                    cur.execute('''DROP TABLE IF EXISTS composition''')
                    cur.execute('''DROP TABLE IF EXISTS concordance''')



                    cur.execute('''CREATE TABLE IF NOT EXISTS composer(id SERIAL PRIMARY KEY, name text)''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition(id SERIAL PRIMARY KEY, 
                                                                    track_no text, 
                                                                    title text,
                                                                    remark text,
                                                                    mode text,
                                                                    printed_in text,  
                                                                    genre text,
                                                                    scribe text
                                                                        )''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition_composer(composition integer references composition,
                                                                                composer_src integer references composer, 
                                                                                composer_rism integer references composer, 
                                                                                composer_chr integer references composer
                                                                                )''')

                    cur.execute('''CREATE TABLE IF NOT EXISTS concordance (id SERIAL PRIMARY KEY, shorthand text, latitude float, longitude float)''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition_concordance_entry( concordance_id integer references concordance,
                                                                                  composition_id integer references composition,
                                                                                  concordance_index integer CHECK (concordance_index BETWEEN 0 AND 2 )
                                                                                    )''')
                    cur.execute('''CREATE VIEW composition_composer_view as
                    SELECT c1.name as composer_src, c2.name as composer_rism, c3.name as composer_chr, 
                    c.track_no, c.title, c.remark, c.mode, c.printed_in, c.genre, c.scribe, c.id as composition_id from
                    composition c join composition_composer cc on c.id = cc.composition
                    LEFT JOIN composer c1 on cc.composer_src = c1.id
                    LEFT JOIN composer c2 on cc.composer_rism = c2.id
                    LEFT JOIN composer c3 on cc.composer_chr = c3.id
                    ;
                    ''')

                    cur.execute('''CREATE VIEW concordance_composition_view as
                                SELECT concordance.shorthand as concordance_man, NULL as concordance_print, NULL as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join concordance on cce.concordance_id = concordance.id where cce.concordance_index=0
                                UNION
                                SELECT NULL as concordance_man, concordance.shorthand as concordance_print, NULL as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join concordance on cce.concordance_id = concordance.id where cce.concordance_index=1
                                UNION
                                SELECT NULL as concordance_man, NULL as concordance_print, concordance.shorthand as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join concordance on cce.concordance_id = concordance.id where cce.concordance_index=2
                                ;
                                ''')
                    cur.execute('''CREATE VIEW concordance_composition_joined_view as
                    SELECT
                    string_agg(concordance_man,', ') as concordance_man, string_agg(concordance_print, ', ') as concordance_print, 
                    string_agg(concordance_chr, ', ') as concordance_chr, composition_id from concordance_composition_view group by composition_id
                    ;
                    ''')
                    cur.execute('''CREATE VIEW all_joined_view as
                                                     SELECT composer_src, composer_rism, composer_chr,
                                                     track_no, title, remark, mode, printed_in, genre, scribe, 
                                                     concomv.concordance_man, concomv.concordance_print, concomv.concordance_chr
                                                     FROM
                                                     concordance_composition_joined_view concomv join composition_composer_view comcomv 
                                                     ON 
                                                     concomv.composition_id = comcomv.composition_id 
                                                     ;
                                                     ''')

                    cur.execute('''CREATE VIEW all_view as
                                  SELECT composer_src, composer_rism, composer_chr,
                                  track_no, title, remark, mode, printed_in, genre, scribe, 
                                  concomv.concordance_man, concomv.concordance_print, concomv.concordance_chr
                                  FROM
                                  concordance_composition_view concomv join composition_composer_view comcomv 
                                  ON 
                                  concomv.composition_id = comcomv.composition_id 
                                  ;
                                  ''')


def getId(conn, table, col, name):
    id = None
    # sanitize
    if name == "":
        return None
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("SELECT id from {} where {} = %s").format(sql.Identifier(table), sql.Identifier(col)), (name,))
        id = cursor.fetchone()
        if not id:
            cursor.execute(sql.SQL("INSERT INTO {} ({}) VALUES(%s) RETURNING ID").format(sql.Identifier(table), sql.Identifier(col)), (name,))
            id = cursor.fetchone()[0]
        else:
            id = id[0]
    return id
    # conn.commit()


def insert_song(conn, track_no, title, remark, mode, printed_in, genre, scribe):
    id = None
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO composition ({}) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING ID".
                       format("track_no, title, remark, mode, printed_in, genre, scribe"),
                       (track_no, title, remark, mode, printed_in, genre, scribe))
        id = cursor.fetchone()[0]
    return id


def insert_song_composers(conn, composition_id, comp_src_id, comp_rism_id, comp_chr_id):
    id = None
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO composition_composer ({}) VALUES(%s,%s,%s,%s)".
                       format("composition, composer_src, composer_rism, composer_chr"),
                       (composition_id, comp_src_id, comp_rism_id, comp_chr_id))

def insert_composition_concordance(conn, composition_id, cond_man, cond_print, cond_chr):
    with conn.cursor() as cursor:
        for index, cond_list in enumerate([cond_man, cond_print, cond_chr]):
            for cond in cond_list.split(','):
                if cond == "":
                    continue
                cond_id = getId(conn, "concordance", "shorthand", cond.strip())
                cursor.execute("INSERT INTO composition_concordance_entry ({}) VALUES(%s,%s,%s)".
                               format("concordance_id, composition_id, concordance_index"),
                               (cond_id, composition_id, index))




def entry_is_sane(entry):
    [composer_src, composer_rism, composer_chr, track_no, title, remark, mode, printed_in, genre, scribe, cond_man,
     cond_print, cond_chr] = entry
    if title != "":
        return True
    return False


def parse_data(input_file: object) -> object:
    columns = ["composer_src", "composer_rism", "composer_chr", "track_no", "title", "remark", "mode", "printed_in",
               "genre", "scribe", "cond_man", "cond_print", "cond_chr"]

    with open(input_file, encoding='utf-8') as f:
        input_lines = f.readlines()

    entries = []
    entry = []

    for line in input_lines[1:]:
        tokens = line.split("@")
        tokens = [t.strip("\n") for t in tokens]

        if len(entry) > 0:
            entry[-1] += tokens[0]
            entry.extend(tokens[1:])

        else:
            entry.extend(tokens)

        if len(entry) == len(columns):
            if entry_is_sane(entry):
                entries.append(entry)
            entry = []
        elif len(entry) > len(columns):
            raise RuntimeError("text too long")

    with psycopg2.connect("dbname=musicdb user=bxu") as con:
        with con:  # auto-commits
            for entry in entries:
                [composer_src, composer_rism, composer_chr,
                 track_no, title, remark, mode, printed_in, genre, scribe, cond_man, cond_print, cond_chr] = entry
                comp_src_id = getId(con, "composer", "name", composer_src)
                # print(comp_src_id)
                comp_rism_id = getId(con, "composer", "name", composer_rism)
                # print(comp_rism_id)
                comp_chr_id = getId(con, "composer", "name", composer_chr)
                # print(comp_chr_id)
                composition_id = insert_song(con, track_no, title, remark, mode, printed_in, genre, scribe)
                insert_song_composers(con, composition_id, comp_src_id, comp_rism_id, comp_chr_id)
                insert_composition_concordance(con, composition_id, cond_man, cond_print, cond_chr)
