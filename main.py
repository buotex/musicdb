import db

remake = False
if remake:
    db.remake_db()
else:
    db.cleanup_db()


db.parse_data("database.csv")
db.add_positions("positions.csv")
db.refresh_views()