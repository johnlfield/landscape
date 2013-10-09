

def PyListoDB(list, dbfile, dbtable):
    # Copy Python list contents into SQLite database table
    # assumes a 3-row header:  column names, SQLite datatypes, and units (this one is ignored)
    import sqlite3 as lite
    dropquery = "DROP TABLE IF EXISTS "+dbtable
    headerquery = "CREATE TABLE "+dbtable+"("
#     print headerquery
    insertquery = "INSERT INTO "+dbtable+" VALUES("
    for e in range(len(list[0])):           #transform column names & datatypes to string
        headerquery = headerquery+list[0][e]+" "+list[1][e]+", "
    headerquery = headerquery[:-2]+')'
#     print headerquery
    insertquery = insertquery+e*"?, " + "?)" 
    #simplify python list, deleting initialization row and three header rows
    for i in range(3):
        del list[0]
    #copy python list contents to a DB table
    #SQL table write structure from http://zetcode.com/db/sqlitepythontutorial/
    os.chdir(dirmain)
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
    os.chdir(dirmain)
    PyListoDB(List, dbfile, dbtable)
    


### File and path definitions ###########################################################
import os
abspath = os.path.abspath(__file__)       #get absolute path where script is located
dname = os.path.dirname(abspath)          #associated directory only
os.chdir(dname)
os.chdir('..')
os.chdir('..')                            #navigate TWO directories higher
dirmain = os.getcwd()
dirres = dirmain+"/results/2013-10-08,01.01- first Hugoton landscape"
dbfile = "hugoton.db"
viewname1 = "Combo"
viewname2 = "Filtered"


### import, label, concatenate & copy .lis files to SQLite database #####################
# note that we're doing the change in SOM calculation here in Python rather than as a
# SQLite query later...
os.chdir(dirres)
import glob
import numpy as np
i = 1
print
print "Reading DayCent .lis output archived in "+dirres
print
for f in glob.glob(os.path.join(dirres, '*')):
    if f.endswith(".lis"):                       #for each .lis file in the archive 
        g = open(f)
        lines = g.readlines()
        labels = lines[1].split()
        base = os.path.basename(f)
        filename = os.path.splitext(base)[0]     #split off the filename       
        id = filename.split("_")
        npdata = np.genfromtxt(f, skip_header=3) #import .lis data as numpy array
        listdata = npdata.tolist()               #convert numpy array to Python list
        for row in listdata:
            year = row[0]
            year = int(year)-1
            row[0] = year
            row.insert(0, id[1])                 #add treatment ID to each entry
            row.insert(0, id[0])
        del listdata[0]                          #delete the first row (DDC repeat)
        for e in range(len(listdata)):
            if e == 0:
                dSOM = 0
            else:
                dSOM = listdata[e][9]-listdata[e-1][9]
            listdata[e].append(dSOM)
        if i == 1:
            DDClis = listdata                    #initialize with first .lis file
        else:
            DDClis = DDClis+listdata             #concatenate files
        i += 1
header = ['Site','Treatment'] + labels + ['dSOM']
columns = len(header)
header2 = ['TEXT','TEXT']
for i in range(columns-2+1):
    header2.append('FLOAT')
header3 = ['']
for i in range(columns-1+1):
    header3.append('')
DDClis.insert(0, header3)
DDClis.insert(0, header2)
DDClis.insert(0, header)
print "Copying DayCent results to SQLite database table..."
os.chdir(dirmain)
PyListoDB(DDClis, dbfile, 'Modeled')


### import, label, concatenate & copy .out files to SQLite database #####################
i = 1
print
print "Reading DayCent .out output archived in "+dirres
print
for f in glob.glob(os.path.join(dirres, '*')):
    if f.endswith(".out"):                       #for each .lis file in the archive 
        g = open(f)
        lines = g.readlines()
        labels = lines[0].split()
        base = os.path.basename(f)
        filename = os.path.splitext(base)[0]     #split off the filename       
        id = filename.split("_")
        npdata = np.genfromtxt(f, skip_header=1) #import .lis data as numpy array
        listdata = npdata.tolist()               #convert numpy array to Python list
        for row in listdata:
            year = row[0]
            year = int(year)-1
            row[0] = year
            row.insert(0, id[1])                 #add treatment ID to each entry
            row.insert(0, id[0])
        del listdata[0]                          #delete the first row (DDC repeat)
        if i == 1:
            DDCout = listdata                    #initialize with first .lis file
        else:
            DDCout = DDCout+listdata             #concatenate files
        i += 1
header = ['Site','Treatment'] + labels
columns = len(header)
header2 = ['TEXT','TEXT']
for i in range(columns-2):
    header2.append('FLOAT')
header3 = ['']
for i in range(columns-1):
    header3.append('')
DDCout.insert(0, header3)
DDCout.insert(0, header2)
DDCout.insert(0, header)
print "Copying DayCent results to SQLite database table..."
print
print
os.chdir(dirmain)
PyListoDB(DDCout, dbfile, 'N2O')


### upload runtable ##################
print "Copying runtable to SQLite database table..."
CSVtoDB("hugoton_runtable_upload.csv", dbfile, "c")


### convert crmvst to real units, add as a column, compute treatment avg ##################
print "Joining and querying SQLite tables to produce summarized output ..."
c_conc = 0.45                          #define biomass carbon concentration 
import sqlite3 as lite
con = lite.connect(dbfile)
with con:
    cur = con.cursor()
    cur.execute("ALTER TABLE Modeled ADD COLUMN DDC_yield FLOAT")
    cur.execute("UPDATE Modeled SET DDC_yield=((crmvst/%s)*.01)" % (c_conc))
    cur.execute("DROP TABLE IF EXISTS Summary")
    cur.execute("CREATE TABLE Summary AS \
                 SELECT m.Site AS Site, x.Treatment AS Treatment, m.Treatment AS DC_run, AVG(m.DDC_yield) AS yield, AVG(m.dSOM) AS dSOM, AVG(n.N2Oflux) as N2O \
                 FROM Modeled m \
                 JOIN N2O n ON m.Treatment=n.Treatment \
                 JOIN hugoton_runtable_upload x ON m.Treatment=x.DC_run \
                 WHERE m.DDC_yield>0.1 \
                 GROUP BY m.Treatment")
    cur.execute("ALTER TABLE Summary ADD COLUMN gwpSOM FLOAT")
    cur.execute("UPDATE Summary SET gwpSOM=(dSOM*(-44/12))*.01")
    cur.execute("ALTER TABLE Summary ADD COLUMN gwpN2O FLOAT")
    cur.execute("UPDATE Summary SET gwpN2O=(N2O*(44/28)*265)*.01")
    cur.execute("ALTER TABLE Summary ADD COLUMN gwpTOT FLOAT")
    cur.execute("UPDATE Summary SET gwpTOT=gwpSOM+gwpN2O")
    cur.execute("ALTER TABLE Summary ADD COLUMN ghg_int FLOAT")
    cur.execute("UPDATE Summary SET ghg_int=gwpTOT/yield")
                 
#                  
# con = lite.connect(dbfile)
# with con:
#     cur = con.cursor()
#     cur.execute("DROP VIEW IF EXISTS Final")
#     cur.execute("CREATE VIEW Final AS \
#                 SELECT x.fips, x.mukey, x.NARRx, x.NARRy, x.Treatment, s.'AVG(DDC_yield)' \
#                 FROM hugoton_attributes_runtable_pasture_test x \
#                 JOIN Summary s ON s.Treatment=x.run_id")
    import csv
    c = csv.writer(open("hugoton_results_combined.csv", "wb"))
    cur.execute("PRAGMA table_info(Summary)")
    labels = cur.fetchall()
    header = []
    for row in labels:
        header.append(row[1])
    c.writerow(header)
    cur.execute("SELECT * FROM Summary")
    rows = cur.fetchall()
    for row in rows:
        c.writerow(row)
print
print
