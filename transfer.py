import pymssql
# src="SL_FLOWPIPE"
#
# server ="192.168.1.53:1433"
# user="sa"
# password="sa123"
# conn=pymssql.connect(
#     server,
#     user,
#     password,
#     database="SpatialDemo"
# )
#
# cursor = conn.cursor(as_dict=True)
#
# cursor.execute("select top 1 * from "+src)
# columrow=cursor.fetchone()
# if columrow:
#     print(columrow)
#     for key in columrow.keys():
#         print(key)
#
# cursor=conn.cursor()
#
# cursor.execute("select * from ")


text =''' insert into  SL_FLOWPIPE(PressureType,ServiceLife,FirstID,EndID,FirstOldID,
                          EndOldID,StartX,StartY,EndX,EndY,StartLon,StartLat,EndLon,EndLat,
                          PipeLength,StartElevation,EndElevation,Caliber,Material,EmbedMode,State,
                          DefDescription,Craft,Owner,ActionLevel,ProjectID,OwnerStruct,OLDID,Adress,
                          Location,locationArea,AdminName,MapNo,ClassCode,Remarks,Remarks2,Remarks3,Purpose,
                          Detecter,DataProvider,FileSource,PlanIdentify,DisuseIdentify,CreateIdentify,DeleteIdentify,
                          ComplexID,ComplexClassCode,CompletionDate,LifeFrom,FromIdentify,LifeTo,ToIdentify,ID,Display,
                          OBJECTID,geom) values
   ('',0,'f3865f2a-0d40-43a4-a2e3-f6948b626de1','c010c46f-43db-4346-b5eb-3323b8eb810c','','',
       483500.006,2941261.868,483581.913,2941263.411,'','','','',81.920,111.863,111.676,200,'铸铁','直埋','','','','','','','','','','金盆西路','','','',120101,'','','','','','','','','','','','','','1899-12-30 00:00:00.0000000','','ea73d738-1ba8-418f-84d2-89f947153a60','','','2fdb3b09-e196-443b-a49c-3dfa09f65357','供水管线',24012,'\202\011\000\000\001\0250\335$\006\260\202\035A\300\237\032\357\246pFA\320x\351\246\367\203\035A\344\245\233\264\247pFA\000\034Zd;\367[@\000\010\201\225C\353[@'::bytea)
'''
# print(text[770],text[771])
for i in range(770,780):
    print(text[989:1000])
