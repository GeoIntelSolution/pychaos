import pymssql
import psycopg2
from decimal import Decimal

src={
    "geom":"SHAPE",
    "table":"SL_FLOWPIPE",
    "server":"192.168.1.53:1433",
    "user":"sa",
    "password":"sa123",
    "database":"SpatialDemo"
}

dist={
    "geom":"geom",
    "table":"SL_FLOWPIPE",
    "server":"192.168.1.107",
    "port":"9216",
    "user":"postgres",
    "password":"gis3857",
    "database":"xiaoshan"
}


def GetType(value):
    return {
        int: "INTEGER",
        str:"VARCHAR(50)",
        Decimal:"double  precision",
        bytes:"bytes",
    }.get(type(value),"VARCHAR(50)")


def pgDDL(columnsRow,tableName):
    ddlStr="create table if not EXISTS "+tableName+"("
    insertStr="insert into table "+ tableName+"("
    keyList=[]
    keyType=[]
    srid=0
    geometryType=""
    for i,key in enumerate(columnsRow.keys()):
        print(key+" is "+GetType(columnsRow[key]))
        if key == "srid":
            srid=columnsRow["srid"]
        elif key == "geom":
            keyList.append("geom")
            typeIndex=columnsRow["geom"].find("(")
            geometryType=columnsRow["geom"][:typeIndex]
            print(geometryType)
        elif key == "SHAPE":
            pass
        else:
               keyList.append(key)
               keyType.append(key+"  "+GetType(columnsRow[key]))
    keyType.append(dist["geom"]+"  geometry("+geometryType+","+str(srid)+")")
        #set the primary key
    keyType.append("CONSTRAINT "+src["table"]+"_pk  PRIMARY KEY (OBJECTID)")
    ddlStr+=",".join(keyType)
    ddlStr+=")"
    insertStr+=",".join(keyList)
    insertStr+=") values"

    createIndex = "create UNIQUE INDEX "+dist["table"]+"_pkey ON "+dist["table"]  +" USING btree(OBJECTID)"
    createSpatialIndex="create INDEX idx_on_"+dist["table"]+"_geom on "+dist["table"]+" using gist(geom)"
    return ddlStr,insertStr,createIndex,createSpatialIndex,keyList



# connect to the
with pymssql.connect(src["server"],src["user"],src["password"],src["database"]) as srcCon:
    with srcCon.cursor(as_dict=True) as colCursor:
        colCursor.execute("select top 1  *,"+src["geom"]+".STAsText() as geom,"+src["geom"]+".STSrid as srid from "+src["table"])
        colRow=colCursor.fetchone()
        if  not colRow:
            print("无法获取列信息")
        else:
            print(colRow)
            print("begin to  ddl in the dist database")
            ddlStr,insertStr,pkIndx,spatialIndex,keysList=pgDDL(colRow,src["table"])
            print(ddlStr)
            print(insertStr)
            print(pkIndx)
            print(spatialIndex)
            with psycopg2.connect(database=dist["database"],host=dist["server"],port=dist["port"],user=dist["user"],password=dist["password"]) as distCon:
                with distCon.cursor() as distCursor:
                    distCursor.execute(ddlStr)
                    distCon.commit()

                    srcCursor=srcCon.cursor(as_dict=True)
                    srcCursor.execute("select * from "+src["table"])
                    currentRow=srcCursor.fetchone()
                    while currentRow:
                        print(currentRow)
                        #form the 100 pieces of  data
                        currentRow=srcCursor.fetchone()









