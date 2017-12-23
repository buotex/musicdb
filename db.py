import os
import sqlite3
import contextlib


def remake_db(db_file):

    with contextlib.closing(sqlite3.connect(db_file)) as con:
            with con: # auto-commits
                with contextlib.closing(con.cursor()) as cur:

                    cur.execute('''DROP TABLE IF EXISTS composer''')
                    cur.execute('''DROP TABLE IF EXISTS composition''')
                    cur.execute('''DROP TABLE IF EXISTS composition_composer''')
                    cur.execute('''DROP TABLE IF EXISTS concordance''')
                    cur.execute('''DROP TABLE IF EXISTS composition_concordance''')
                    cur.execute('''DROP VIEW IF EXISTS composition_composer_view''')
                    cur.execute('''DROP VIEW IF EXISTS concordance_production''')


                    cur.execute('''CREATE TABLE IF NOT EXISTS composer(name string, birth date, death date)''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition(track_no integer, 
                                                                    title string,
                                                                    remark string,
                                                                    mode integer,
                                                                    printed_in string,  
                                                                    genre string,
                                                                    scribe string
                                                                        )''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition_composer(composition integer references composition(ROWID),
                                                                                composer_src integer references composer(ROWID), 
                                                                                composer_rism integer references composer(ROWID), 
                                                                                composer_chr integer references composer(ROWID)
                                                                                )''')

                    cur.execute('''CREATE TABLE IF NOT EXISTS concordance ( shorthand string)''')
                    cur.execute('''CREATE TABLE IF NOT EXISTS composition_concordance( composition integer references composition(ROWID),
                                                                                  concordance_man integer references concordance(ROWID),
                                                                                  concordance_print integer references concordance(ROWID),
                                                                                  concordance_chr integer references concordance(ROWID)
                                                                                    )''')
                    cur.execute('''CREATE VIEW composition_composer_view as
                    SELECT c1.name, c2.name, c3.name, c.track_no, c.title, c.remark from
                    composition c join composition_composer cc on c.ROWID == cc.composition
                    LEFT JOIN composer c1 on cc.composer_src == c1.ROWID
                    LEFT JOIN composer c2 on cc.composer_rism == c2.ROWID
                    LEFT JOIN composer c3 on cc.composer_chr == c3.ROWID
                    ;
                    ''')
                    cur.execute('''CREATE VIEW concordance_production as 
                    SELECT con.shorthand, composition.title from composition natural join composition_concordance 
                    , concordance where composition_concordance_man == concordance.ROWID OR
                                        composition_concordance_print == concordance.ROWID OR
                                        composition_concordance_chr == concordance.ROWID
                                        ''')


def getId(conn, table, col, name):
    id = None
    # sanitize
    if name == "":
        return None
    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute("SELECT ROWID from {} where {} == ?".format(table, col), (name,))
        id = cursor.fetchone()
        if not id:
            #print("INSERTING {}".format(name))
            cursor.execute("INSERT INTO {} ({}) VALUES(?)".format(table, col), (name,))
            id = cursor.lastrowid
        else:
            id = id[0]
    return id
    # conn.commit()


def insert_song(conn, track_no, title, remark, mode, printed_in, genre, scribe):
    id = None
    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute("INSERT INTO composition VALUES(?,?,?,?,?,?,?)",
                       (track_no, title, remark, mode, printed_in, genre, scribe))
        id = cursor.lastrowid
    return id


def insert_song_composers(conn, composition_id, comp_src_id, comp_rism_id, comp_chr_id):
    id = None
    #print(composition_id, comp_src_id, comp_rism_id, comp_chr_id)
    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute("INSERT INTO composition_composer VALUES(?,?,?,?)",
                       (composition_id, comp_src_id, comp_rism_id, comp_chr_id))
        id = cursor.lastrowid
    return id


def entry_is_sane(entry):
    [composer_src, composer_rism, composer_chr, track_no, title, remark, mode, printed_in, genre, scribe, cond_man,
     cond_print, cond_chr] = entry
    if title != "":
        return True
    return False


def parse_data(input_file: object, db_file: object) -> object:
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

    with contextlib.closing(sqlite3.connect(db_file)) as conn:
        with conn:  # auto-commits
            for entry in entries:
                [composer_src, composer_rism, composer_chr,
                 track_no, title, remark, mode, printed_in, genre, scribe, cond_man, cond_print, cond_chr] = entry
                comp_src_id = getId(conn, "composer", "name", composer_src)
                # print(comp_src_id)
                comp_rism_id = getId(conn, "composer", "name", composer_rism)
                # print(comp_rism_id)
                comp_chr_id = getId(conn, "composer", "name", composer_chr)
                # print(comp_chr_id)
                composition_id = insert_song(conn, track_no, title, remark, mode, printed_in, genre, scribe)
                insert_song_composers(conn, composition_id, comp_src_id, comp_rism_id, comp_chr_id)
