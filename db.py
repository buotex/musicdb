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
                    cur.execute('''DROP MATERIALIZED VIEW IF EXISTS all_joined_view''')
                    cur.execute('''DROP MATERIALIZED VIEW IF EXISTS all_view''')
                    cur.execute('''DROP MATERIALIZED VIEW IF EXISTS concordance_composition_joined_view''')
                    cur.execute('''DROP MATERIALIZED VIEW IF EXISTS concordance_composition_view''')
                    cur.execute('''DROP MATERIALIZED VIEW IF EXISTS composition_composer_view''')
                    cur.execute('''DROP TABLE IF EXISTS composition_composer''')
                    cur.execute('''DROP TABLE IF EXISTS composition_concordance_entry''')
                    cur.execute('''DROP TABLE IF EXISTS composer''')
                    cur.execute('''DROP TABLE IF EXISTS composition''')
                    cur.execute('''DROP TABLE IF EXISTS concordance''')



                    cur.execute('''CREATE TABLE IF NOT EXISTS composer(name text PRIMARY KEY)''')
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
                                                                                composer_src text references composer, 
                                                                                composer_rism text references composer, 
                                                                                composer_chr text references composer
                                                                                )''')

                    cur.execute('''CREATE TABLE IF NOT EXISTS concordance (shorthand text PRIMARY KEY, latitude float, longitude float)''')

                    cur.execute('''CREATE TABLE IF NOT EXISTS composition_concordance_entry( concordance_shorthand text references concordance,
                                                                                  composition_id integer references composition,
                                                                                  concordance_index integer CHECK (concordance_index BETWEEN 0 AND 2 )
                                                                                    )''')
                    cur.execute('''CREATE MATERIALIZED VIEW composition_composer_view as
                    SELECT c1.name as composer_src, c2.name as composer_rism, c3.name as composer_chr, 
                    c.track_no, c.title, c.remark, c.mode, c.printed_in, c.genre, c.scribe, c.id as composition_id from
                    composition c join composition_composer cc on c.id = cc.composition
                    LEFT JOIN composer c1 on cc.composer_src = c1.name
                    LEFT JOIN composer c2 on cc.composer_rism = c2.name
                    LEFT JOIN composer c3 on cc.composer_chr = c3.name
                    ;
                    ''')

                    cur.execute('''CREATE MATERIALIZED VIEW concordance_composition_view as
                                SELECT concordance.shorthand as concordance_man, NULL as concordance_print, NULL as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join concordance on cce.concordance_shorthand = concordance.shorthand where cce.concordance_index=0
                                UNION
                                SELECT NULL as concordance_man, concordance.shorthand as concordance_print, NULL as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join 
                                concordance on cce.concordance_shorthand = concordance.shorthand where cce.concordance_index=1
                                UNION
                                SELECT NULL as concordance_man, NULL as concordance_print, concordance.shorthand as concordance_chr, composition_id
                                from composition_concordance_entry as cce join composition on cce.composition_id = composition.id join 
                                concordance on cce.concordance_shorthand = concordance.shorthand where cce.concordance_index=2
                                UNION
                                SELECT NULL as concordance_man, NULL as concordance_print, NULL as concordance_chr, composition.id
                                from composition
                                where composition.id not in (select composition_id from composition_concordance_entry)
                                ;
                                ''')
                    cur.execute('''CREATE MATERIALIZED VIEW concordance_composition_joined_view as
                    SELECT
                    string_agg(concordance_man,', ') as concordance_man, string_agg(concordance_print, ', ') as concordance_print, 
                    string_agg(concordance_chr, ', ') as concordance_chr, composition_id from concordance_composition_view group by composition_id
                    ;
                    ''')
                    cur.execute('''CREATE MATERIALIZED VIEW all_joined_view as
                                                     SELECT composer_src, composer_rism, composer_chr,
                                                     track_no, title, remark, mode, printed_in, genre, scribe, 
                                                     concomv.concordance_man, concomv.concordance_print, concomv.concordance_chr
                                                     FROM
                                                     concordance_composition_joined_view concomv join composition_composer_view comcomv 
                                                     ON 
                                                     concomv.composition_id = comcomv.composition_id 
                                                     ;
                                                     ''')

                    cur.execute('''CREATE MATERIALIZED VIEW all_view as
                                  SELECT comcomv.composer_src, comcomv.composer_rism, comcomv.composer_chr,
                                  comcomv.track_no, comcomv.title, comcomv.remark, comcomv.mode, comcomv.printed_in, comcomv.genre, comcomv.scribe, 
                                  concomv.concordance_man, concomv.concordance_print, concomv.concordance_chr, 
                                  concordance.latitude, concordance.longitude
                                  FROM
                                  concordance_composition_view concomv left join composition_composer_view comcomv
                                  ON 
                                  concomv.composition_id = comcomv.composition_id 
                                  LEFT JOIN
                                   concordance
                                  ON 
                                  concomv.concordance_man = concordance.shorthand 
                                  or concomv.concordance_print = concordance.shorthand 
                                  or concomv.concordance_chr = concordance.shorthand                                  
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


def insert_song_composers(conn, composition_id, comp_src_name, comp_rism_name, comp_chr_name):

    comp_src_name = comp_src_name.replace(',',';')
    comp_rism_name = comp_rism_name.replace(',',';')
    comp_chr_name = comp_chr_name.replace(',',';')

    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO composer (name) VALUES(%s) ON CONFLICT DO NOTHING", (comp_src_name,))
        cursor.execute("INSERT INTO composer (name) VALUES(%s) ON CONFLICT DO NOTHING", (comp_rism_name,))
        cursor.execute("INSERT INTO composer (name) VALUES(%s) ON CONFLICT DO NOTHING", (comp_chr_name,))

        cursor.execute("INSERT INTO composition_composer ({}) VALUES(%s,%s,%s,%s)".
                       format("composition, composer_src, composer_rism, composer_chr"),
                       (composition_id, comp_src_name, comp_rism_name, comp_chr_name))

def insert_composition_concordance(conn, composition_id, cond_man, cond_print, cond_chr):
    with conn.cursor() as cursor:
        for index, cond_list in enumerate([cond_man, cond_print, cond_chr]):
            for cond in cond_list.split(','):
                if cond == "":
                    continue
                cursor.execute("INSERT INTO concordance (shorthand) VALUES(%s) ON CONFLICT DO NOTHING", (cond.strip(),))
                cursor.execute("INSERT INTO composition_concordance_entry ({}) VALUES(%s,%s,%s)".
                               format("concordance_shorthand, composition_id, concordance_index"),
                               (cond.strip(), composition_id, index))




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

    with psycopg2.connect("dbname=musicdb user=bxu") as conn:
        with conn:  # auto-commits
            for entry in entries:
                [composer_src, composer_rism, composer_chr,
                 track_no, title, remark, mode, printed_in, genre, scribe, cond_man, cond_print, cond_chr] = entry
                composition_id = insert_song(conn, track_no, title, remark, mode, printed_in, genre, scribe)
                insert_song_composers(conn, composition_id, composer_src, composer_rism, composer_chr)
                insert_composition_concordance(conn, composition_id, cond_man, cond_print, cond_chr)

def cleanup_db():

    db_names = ["composition_composer","composition_concordance_entry","composer",
                "composition", "concordance"]
    with psycopg2.connect("dbname=musicdb user=bxu") as conn:
        with conn:  # auto-commits

            for db in db_names:
                with conn.cursor() as cursor:
                    cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(db)))


def refresh_views():
    VIEW_NAMES = ["composition_composer_view", "concordance_composition_view", "concordance_composition_joined_view",
                  "all_view", "all_joined_view"]
    with psycopg2.connect("dbname=musicdb user=bxu") as conn:
        with conn:  # auto-commits

            for view in VIEW_NAMES:
                with conn.cursor() as cursor:
                    cursor.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(view)))


def add_positions(input_file):
    with open(input_file, encoding='utf-8') as f:
        input_lines = f.readlines()

    with psycopg2.connect("dbname=musicdb user=bxu") as conn:
        with conn:  # auto-commits
            with conn.cursor() as cursor:
                for line in input_lines[:]:
                    shorthand, latitude, longitude = line.split(",")
                    cursor.execute("UPDATE concordance set latitude=%s, longitude=%s where shorthand=%s", (latitude, longitude,shorthand))


