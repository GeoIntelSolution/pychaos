import pymssql
src="SL_FLOWPIPE"

server ="192.168.1.53:1433"
user="sa"
password="sa123"
conn=pymssql.connect(
    server,
    user,
    password,
    database="SpatialDemo"
)

cursor = conn.cursor(as_dict=True)

cursor.execute("select top 1 * from "+src)
columrow=cursor.fetchone()
if columrow:
    print(columrow)
    for key in columrow.keys():
        print(key)

cursor=conn.cursor()

cursor.execute("select * from ")

