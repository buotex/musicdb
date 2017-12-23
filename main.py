import db
import remi


db_file = "music.db"
db.remake_db(db_file)

db.parse_data("database.csv", db_file)
