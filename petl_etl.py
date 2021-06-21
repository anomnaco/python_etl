import petl as etl

table_header = ["Fixed Acidity","Volatile Acidity","Citric Acid","Sugar","Chlorides","Free SO2","Total SO2","Density","pH","Sulfates","Alcohol","Quality"]

table1 = etl.addfield(etl.convertnumbers(etl.setheader(etl.fromcsv('winequality-red.csv'),table_header)), "Type", "Red") 
table2 = etl.addfield(etl.convertnumbers(etl.setheader(etl.fromcsv('winequality-white.csv'),table_header)), "Type", "White")

#print(etl.head(table1))
#print(etl.head(table2))

table1_filtered = etl.select(table1, "Quality", lambda v: v > 6)
table2_filtered = etl.select(table2, "Quality", lambda v: v > 4)

good_wines = etl.cat(table1_filtered,table2_filtered)

good_wines_enhanced = etl.addfields(good_wines, [   ("Max Acidity", lambda rec: rec["Fixed Acidity"]+rec["Volatile Acidity"]),
                                                    ("Locked SO2", lambda rec: rec["Total SO2"]-rec["Free SO2"])])
#print(etl.head(good_wines_enhanced))
#print(etl.tail(good_wines_enhanced))

gwe_sorted = etl.sort(good_wines_enhanced, key=["Quality","Sugar"])

#print(etl.head(gwe_sorted))
print(etl.lookall(etl.tail(gwe_sorted,500)))