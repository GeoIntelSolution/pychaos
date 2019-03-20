import pymssql
import psycopg2
from decimal import Decimal

src={
    "geom":"SHAPE",
    "table":"SP_VALVE",
    "server":"192.168.1.53:1433",
    "user":"sa",
    "password":"sa123",
    "database":"SpatialDemo"
}

dist={
    "geom":"geom",
    "table":"SP_VALVE",
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
    insertStr="insert into  "+ tableName+"("
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

    createIndex = "create UNIQUE INDEX  IF NOT EXISTS "+dist["table"]+"_pkey ON "+dist["table"]  +" USING btree(OBJECTID)"
    createSpatialIndex="create INDEX IF NOT EXISTS idx_on_"+dist["table"]+"_geom on "+dist["table"]+" using gist(geom)"
    return ddlStr,insertStr,createIndex,createSpatialIndex,keyList,srid



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
            ddlStr,insertStr,pkIndx,spatialIndex,keysList,projectID=pgDDL(colRow,src["table"])
            print(ddlStr)
            print(pkIndx)
            print(spatialIndex)
            with psycopg2.connect(database=dist["database"],host=dist["server"],port=dist["port"],user=dist["user"],password=dist["password"]) as distCon:
                with distCon.cursor() as distCursor:
                    distCursor.execute(ddlStr)
                    distCon.commit()

                    srcCursor=srcCon.cursor(as_dict=True)
                    srcCursor.execute("select   *,"+src["geom"]+".STAsText() as geom from "+src["table"])
                    currentRow=srcCursor.fetchone()
                    counter=1
                    insertArr=[]
                    while currentRow:
                        # print(currentRow)
                        #form the 100 pieces of  data
                        item=[]
                        insertList=[]
                        for key in keysList:
                            if key != "geom":
                                if GetType(currentRow[key])==GetType(str):
                                    insertList.append(key)
                                    item.append("\'"+str(currentRow[key])+"\'" if currentRow[key] else "\'\'")
                                elif GetType(currentRow[key])==GetType(type(None)):
                                    continue
                                else:
                                    insertList.append(key)
                                    item.append(str(currentRow[key]) if currentRow[key] else "NULL")
                        # temp=""" %s (%s,ST_GeomFromText(\'%s\',%d))""" % (insertStr,",".join(item),currentRow["geom"],projectID)
                        temp="""(%s,ST_GeomFromText(\'%s\',%d))""" % (",".join(item),currentRow["geom"],projectID)
                        # print(temp)
                        insertArr.append(temp)
                        currentRow=srcCursor.fetchone()
                        if counter %100==0 or currentRow==None:
                            insertBatchStr = ",".join(insertArr)
                            insertBatchStr=insertStr+insertBatchStr
                            # print(insertBatchStr)
                            distCursor.execute(insertBatchStr)

                            distCon.commit()
                            print(counter)
                            item=[]
                            insertArr=[]

                            if currentRow == None:
                                  #no need ,自动建立的
                                  # distCursor.execute(pkIndx)

                                  distCursor.execute(spatialIndex)
                        counter+=1
                #create index






