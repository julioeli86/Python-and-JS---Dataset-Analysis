import csv
import json

importString = "\nvar MongoClient = require('mongodb').MongoClient;\nvar url = mongodb://localhost:27017/;"
importString += "\nMongoClient.connect(url, function(err, db) { \n if (err) throw err; \n var dbo = db.db('FinalProject2');\n var myobj = [\n" 

with open("BAN-707-008-cal_cities_lat_long.csv","r", encoding="utf-8") as infile:
    rdr = csv.reader(infile, delimiter=',')
    keys=[]
    firstRow = True
    
    
    for row in rdr:
        row_data = {}
        if firstRow:
            # This row contains the headings
            firstRow = False
            for col in row:
                keys.append(col)    
        
        else:
            
            for i,col in enumerate(row):
                row_data[keys[i]] = col
            
            importString += json.dumps(row_data)
            
            importString += ",\n"
            
    importString = importString[:-2]
    importString += "\n ];""dbo.collection('cityData2').insertMany(myobj, function(err, res) {  \n                if (err) throw err;"" \n            db.close(); \n  }); \n});\n"       

print(importString)
with open("CityData2.js", "w+") as outfile:
        outfile.write(importString)           
            
      
        
        