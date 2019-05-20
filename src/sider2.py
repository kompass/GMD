import MySQLdb

if __name__ == '__main__':
    db = MySQLdb.connect(host='neptune.telecomnancy.univ-lorraine.fr', db='gmd', user='gmd-read', passwd='esial')
    c = db.cursor()

    c.execute('show tables')
    print(c.fetchall())
