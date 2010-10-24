import sqlite3

db_src = sqlite3.connect('db')
db_dst = sqlite3.connect('db.copy')

c_src = db_src.cursor()
c_dst = db_dst.cursor()

c_src.execute('SELECT sha256, offset, length FROM chunk')
result = c_src.fetchone()
while result:
  sha256, offset, length = result
  c_dst.execute('INSERT INTO chunk (sha256, offset, length) VALUES (?, ?, ?)',
    (sha256, offset, length))
  result = c_src.fetchone()

db_dst.commit()
