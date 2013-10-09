    
def PyListoDB(list, dbfile, dbtable):
# Copy Python list contents into SQLite database table
# assumes a 3-row header:  column names, SQLite datatypes, and units (this one is ignored)
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
        

def CSVtoDB(csvfile, dbfile, delim):
# Copy CSV file contents into a SQLite database table 
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



### header ###############################################################################
print
print
print "This script generates a DayCent model runtable for a full landscape analysis from"
print "a csv-format GIS attribute table.  It requires the following directory structure:"
print "    Main directory: csv-format files (with SQLite-appropriate headers) for the"
print "        GIS attribute table, county restrictions, treatments, and a .dat-format"
print "        CFARM site archive file"
print "    Subdirectory 'scripts'/subdirectory 'landscape': this script 'landtable00X.py'"
print


### file/path definitions ################################################################
import os
Database = "hugoton.db"
attfile = "hugoton_attributes_Oct08.csv"
atttab = attfile[:-4]
archfile = "runfile_cometfarm_spinup_current.dat"
archtab = archfile[:-4]
treatfile = "treatments_d.csv"
treattab = treatfile[:-4]
countfile = "Counties.csv"
counttab = countfile[:-4]
spintab = "hugoton_attributes_Oct08_gid_lookup"
spinfile = spintab+".csv"
runtab = atttab+"_runtable"
runfile = runtab+".csv"
abspath = os.path.abspath(__file__)     #get absolute path where script is located
dname = os.path.dirname(abspath)        #associated directory only
os.chdir(dname)
os.chdir('..')
os.chdir('..')                          #navigate TWO directories higher
dirmain = os.getcwd()
print "Creating a landscape DayCent runtable based on the following inputs:"
print "    Attribute table:  ", attfile
print "    CFARM archive lookup table:  ", archfile
print "    Treatment table:  ", treatfile
print "    County restrictions:  ", countfile
print


### check for existing runtable, load inputs to SQLite ###################################
import glob
import shutil
if os.path.exists(spinfile) or os.path.exists(runfile): 
    print "** Derivate file(s) already exists; program terminated **"
    print
    print
else:
    #copy all runtable archive components to SQLite database     
    print "Copying runtable CSV files to SQLite database tables..."
    print
    CSVtoDB(attfile, Database, "c")
    CSVtoDB(archfile, Database, "t")
    CSVtoDB(treatfile, Database, "c")
    CSVtoDB(countfile, Database, "c")


### create & write spinup table (attribute table + CFARM spinup runno ####################
    import csv
    import sqlite3 as lite
    con = lite.connect(Database)              #establish database connection
    print "Joining SQLite tables to generate DayCent spinup table..."
    print
    with con:
        cur = con.cursor()
        cur.execute("CREATE TABLE %s AS \
                    SELECT r.runno, a.* \
                    FROM %s a \
                    JOIN %s r ON a.fips=r.fips AND a.gridx=r.gridx AND a.gridy=r.gridy AND a.mukey_int=r.mukey \
                    JOIN %s c ON a.fips=c.fips \
                    WHERE a.irr=0 AND r.irrig='N' AND ((a.nlcd240_equiv=51 AND r.grazetill='graze') OR ((a.nlcd240_equiv=81 OR a.nlcd240_equiv=82) AND r.grazetill='till'))" 
                    % (spintab,atttab,archtab,counttab))
        cur.execute("SELECT * FROM %s" % (spintab))
        rows = cur.fetchall()
        c = csv.writer(open(spinfile, "wb"))
        print "Spinup table "+spinfile+" being saved..."
        print
        for row in rows:
            c.writerow(row)


### create & write runtable (spinup table x treatments) ##################################
    print "Joining SQLite tables to generate DayCent runtable..."
    print
    con = lite.connect(Database)              #establish database connection
    with con:
        cur = con.cursor()
        cur.execute("CREATE TABLE %s AS \
                    SELECT DISTINCT s.runno, s.fips, s.gridx, s.gridy, s.mukey_int, s.irr, s.nlcd240_equiv, t.* \
                    FROM %s s \
                    JOIN %s t" % (runtab,spintab,treattab))
        cur.execute("SELECT * FROM %s" % (runtab))
        rows = cur.fetchall()
        c = csv.writer(open(runfile, "wb"))
        print "Runtable "+runfile+" being saved..."
        print
        for row in rows:
            c.writerow(row)
    print

