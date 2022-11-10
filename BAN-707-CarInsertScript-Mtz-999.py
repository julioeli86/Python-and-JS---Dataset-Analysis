import csv
import io
import json

importString = "\nvar MongoClient = require('mongodb').MongoClient;\nvar url = mongodb://localhost:27017/;"
rowCount = 0 

with open ("car2.js", "w") as outfile:
    outfile.write(importString)
    
importString = ""
    

with io.open("vehicles-mod.csv","r", encoding="utf-8") as infile:
    rdr = csv.reader(infile, delimiter= ',')
    firstRow = True
    keys=[]
    
    for row in rdr:
        row_data = {}
        # End the insertMany command every 1,000 documents
        if rowCount % 80000 == 0 and rowCount != 0:
            importString = importString[:-2]
            importString += ",{w:0}];\n ""dbo.collection('carData2').insertMany(myobj, function(err, res) {  \n                if (err) throw err;"" \n            db.close(); \n  }); \n});\n"
            with open ("car2.js","a") as outfile:
                outfile.write(importString)
            importString = ""
            print("\tBeginning row " + str(rowCount + 1))
        # Start a new insertMany command every 1,000 documents
        if rowCount % 80000 == 0:
            importString += "\nMongoClient.connect(url, function(err, db) { \n if (err) throw err; \n var dbo = db.db('FinalProject2');\n var myobj = [\n"
        # Identify and store column headers
               
        if firstRow:
            # This row contains the headings
            firstRow = False
            for col in row:
                keys.append(col)
                
        else:
            #Store each entry as a value in our dictionaries (columen headings are keys)
            for i,col in enumerate(row):
                if keys[i] in ["price", "year", "odometer", "lat", "long"] and col != "":
                    #if type[i] == int:
                    row_data[keys[i]] = float(col)
                elif keys[i] in ["posting_date"]:
                    row_data[keys[i]] = 'ISODate("{}")'.format(col)
                elif keys[i] not in ["url","region_url","image_url","description"] and i!=0:
                    row_data[keys[i]] = col
                
            importString += json.dumps(row_data).replace('"ISODate(\\"','ISODate("').replace('\\")"','")')
            
            #Without a newline between documents in the insertMany, MongoDB will throw abs
            #'literal not terminated before end of script' error for 3 or more documents
            importString += ",\n"
        
        if  rowCount % 80000 == 0:
            print("Completed row " + str(rowCount))
            with open ("car2.js","a") as outfile:
                outfile.write(importString)
            importString = ""
        # Increment row count for progress tracking
        rowCount = rowCount + 1
    # Final print statements
    importString = importString[:-2]
    importString += ",{w:0}];\n ""dbo.collection('carData2').insertMany(myobj, function(err, res) {  \n    if (err) throw err;"" \n    db.close(); \n  }); \n});\n"
               
#Remove comma and newline from last document, as no more documents will be added

    with open("car2.js", "a") as outfile:
        outfile.write(importString)
#Create JavaScript file
    print("END ROW COUNT:  " + str(rowCount - 1))
    
    
    
    
        
        
    
    

