    
# Copy Python list contents into SQLite database table
# assumes a 3-row header:  column names, SQLite datatypes, and units (this one is ignored)
def PyListoDB(list, dbfile, dbtable):
    import sqlite3 as lite
    dropquery = "DROP TABLE IF EXISTS "+dbtable
    headerquery = "CREATE TABLE "+dbtable+"("
    insertquery = "INSERT INTO "+dbtable+" VALUES("
    for e in range(len(list[0])):           #transform column names & datatypes to string
        headerquery = headerquery+list[0][e]+" "+list[1][e]+", "
    headerquery = headerquery[:-2]+')'
    insertquery = insertquery+e*"?, " + "?)" 
    #simplify python list, deleting initialization row and three header rows
    for i in range(3):
        del list[0]
    #copy python list contents to a DB table
    #SQL table write structure from http://zetcode.com/db/sqlitepythontutorial/
    con = lite.connect(dbfile)              #establish database connection
    with con:
        cur = con.cursor()
        cur.execute(dropquery)
        cur.execute(headerquery)
        cur.executemany(insertquery, list)
        
# Copy CSV file contents into a SQLite database table 
def CSVtoDB(csvfile, dbfile, delim):
#arguments- CSV and DB filenames (incl. extensions!), delimiter: c=comma, t=tab
    import csv
    #copy CSV file contents into a python list
    if delim == "c":
        Lines = csv.reader(open(csvfile, 'rU'))
    elif delim == "t":
        Lines = csv.reader(open(csvfile, 'rU'), delimiter="\t")
    List = [[]]
    for x in Lines:
        List.append(x)
    #define SQL-format strings to create & populate table
    dbtable = csvfile[:-4]
    del List[0]
    PyListoDB(List, dbfile, dbtable)



### HEADER
print
print
print "This script generates a GID lookup table for a full landscape analysis from"
print "a csv-format GIS attribute table in the format of hugoton_attributes.csv."  
print
print "It requires the following directory structure:"
print "    Main directory: 'hugoton.db', csv-format GIS attribute table (with appropriate"
print "        header), .dat-format CFARM site archive file"
print "    Subdirectory 'scripts'/subdirectory 'landscape': this script 'landtable00X.py'"
print


### RUNTABLE BULDER
import os
Database = "hugoton.db"
archfile = "runfile_cometfarm_spinup_current.dat"                   #CFARM site archive file
archtab = archfile[:-4]
treatfile = "treatments_d.csv"
treattab = treatfile[:-4]
countfile = "Counties.csv"
counttab = countfile[:-4]
viewname = "filtered"
abspath = os.path.abspath(__file__)     #get absolute path where script is located
dname = os.path.dirname(abspath)        #associated directory only
os.chdir(dname)
os.chdir('..')
os.chdir('..')                          #navigate TWO directories higher
dirmain = os.getcwd()
print "Please enter the name of the attribute table to use (no extension):"
attCSV = raw_input()
print
if attCSV != "":
    import glob
    import shutil
    gidtable = attCSV+"_gid_lookup.csv"
    attCSVfile = attCSV+".csv"
    if os.path.exists(gidtable): 
        print "** GID lookup table already exists; program terminated **"
        print
        print
    else:
        #copy all GID lookup table archive components to SQLite database     
        print "Copying GID lookup table-associated CSV files to SQLite database tables..."
        print
        CSVtoDB(attCSVfile, Database, "c")
        CSVtoDB(archfile, Database, "t")
        CSVtoDB(treatfile, Database, "c")
        CSVtoDB(countfile, Database, "c")
            
        #create a GID lookup table based on each entry in 'Yields.csv', using joins to bring in site 
        #data, FIPS codes, and CFARM equilibrium archive run numbers
        import csv
        import sqlite3 as lite
        print "Joining tables to generate GID lookup table..."
        print
        con = lite.connect(Database)              #establish database connection
        with con:
            cur = con.cursor()
            cur.execute("SELECT r.runno, a.* \
                        FROM %s a \
                        JOIN %s r ON a.fips=r.fips AND a.gridx=r.gridx AND a.gridy=r.gridy AND a.mukey_int=r.mukey \
                        JOIN %s c ON a.fips=c.fips \
                        WHERE (a.nlcd240_equiv=82 OR a.nlcd240_equiv=81 OR a.nlcd240_equiv=51) AND a.irr=0 AND r.irrig='N'" % (attCSV,archtab,counttab))
            rows = cur.fetchall()
            c = csv.writer(open(gidtable, "wb"))
            print "GID lookup table "+gidtable+" being saved..."
            for row in rows:
                c.writerow(row)
            print
            print
