
# Copy CFARM archive site.100 file to main working directory
def CFARMarch(DDCarchpath, run, sitpath):
    import subprocess
    site100 = "%s -r %s %s ./site.100" % (DDCarchpath,run,sitpath)
    subprocess.call("%s" % (site100), shell=True)

# Copy the appropriate soils.in and site.wth files into the main working directory
def SoilWth(soilpath, mukey, wthpath, NARR, dirmain):
    import os
    import shutil
    soildir = mukey[:-3]
    os.chdir(soilpath+"/"+soildir)
    shutil.copy(mukey+".in", dirmain)
    os.chdir(wthpath)
    shutil.copy(NARR+".wth", dirmain)
    os.chdir(dirmain)
    os.rename(mukey+".in", "soils.in")
    os.rename(NARR+".wth", "site.wth")

# Update the standard switch.sch as per a runtable entry
def SCHbuild(Est_year, Plant_DOY, Harv_skip, SGN1_DOY, SGN2_DOY, Sene_DOY, Harv_DOY):
    #DayCent quirk- header block starting year must be indexed back by 1; keep this in
    #mind for @ESY, @02S, @12S, @22S, @23S 
    OSY1 = int(Est_year)-1 
    EOT = int(Est_year)+20                     #arbitrarily simulate post-estab. management for decade
    FRST = int(Plant_DOY)+1
    Cult = int(Plant_DOY)-1
    Harv_start = OSY1+int(Harv_skip)
    Harv_end = int(Harv_start)+20
    #different template .sch files depending on how many harvests are skipped
    if int(Harv_skip) == 0:
        sch = 'sg0.sch'
        End = EOT
    elif int(Harv_skip) == 1:
        sch = 'sg1.sch'
        End = EOT
    else:
        sch = 'sg2.sch'
        End = Harv_end
    infile = open(sch)
    outfile = open('sg.sch', 'w')
    replacements = {'@STY':str(Est_year), '@LSY':str(End), '@EEY':str(Est_year), '@ESY':str(OSY1), '@PEY':str(EOT), '@PSY':str(Est_year), \
                    '@22E':str(Harv_start), '@22S':str(Est_year), '@23E':str(Harv_end), \
                    '@23S':str(Harv_start), '@PD':str(Plant_DOY), '@FD':str(FRST), '@SN':str(Sene_DOY), \
                    '@N1':str(SGN1_DOY), '@N2':str(SGN2_DOY), '@HV':str(Harv_DOY), '@CD':str(Cult)}
    for line in infile:
        for src, target in replacements.iteritems():
            line = line.replace(src, target)
        outfile.write(line)
    infile.close()
    outfile.close()

# Update the standard fert.100 as per a runtable entry
# delete the main working directory fert.100 if exists
# copy a new fert.100 from the library archive manking string replacements
def FERTbuild(SGN1_rate, SGN2_rate, dirlib):
    if os.path.exists("fert.100"):
        os.remove("fert.100")
    infile = open(dirlib+"/fert.100")
    outfile = open('fert.100', 'w')
    replacements = {'@SR1':str(SGN1_rate), '@SR2':str(SGN2_rate)}
    for line in infile:
        for src, target in replacements.iteritems():
            line = line.replace(src, target)
        outfile.write(line)
    infile.close()
    outfile.close()

# Run DDcentEVI and DDClist100
def DDcentEVI(sch, id):
    import subprocess
    id = "Hugoton_"+id
    runexp = "./DDcentEVI32 -s %s -n %s" % (sch,id)
    subprocess.call("%s" % (runexp), shell=True)
    runlis = "./DDClist100sp %s %s outvars.txt" % (id,id)
    subprocess.call("%s" % (runlis), shell=True)         #execute DDClist100 on experiment .bin output
    os.rename("year_summary.out", str(id)+".out")






### HEADER
print
print
print "This script automates a set of DayCent model runs as specified in a user-selected"
print "csv-formatted runtable, and includes archiving of all .lis and year_summary.out"
print "file results."
print  
print "It requires the following directory structure:"
print "  Main directory: UNIX-exectutable DDcentEVI, DDClist100, and DDCsitarchive,"
print "    as well as the csv-formatted runtable and this script ('autoDDC00X-py')"
print "  Subdirectory 'library100': contains a series of archive folders containing"
print "    all necessary supporting DayCent .100 files, switch.sch (formatted for"
print "    automated modification), outfiles.in, and outvars.txt"
print 
print "  * Note that .wth, soils.in, and site.100 files will be downloaded from their"
print "    respective network repositories automatically; those paths are hard-"
print "    coded into this script and must be changed by hand"
print
print "Analysis results will automatically be archived along with a log file recording"
print "the runtable file and library100 archive version used, the paths specified for the"
print ".wth, soils.in, and site.100 file repositories, and the current version of the"
print "script code in order to track the provinence of all results generated."
print

#specification of all relevant input archives and network data repositories
import os
DDCarchpath = "/data/paustian/Century/bin/DDCsitarchive32"
wthpath = "/data/wcnr-network/Research/Paustian/AFRI/NARR_gridxy_wth/"
# wthpath = "/data/wcnr-network/Research/Paustian/AFRI/Validation/_archive/DayMet/"
sitpath = "/data/paustian/CFARM/daycentservice/newEqRuns/100files/runfile_cometfarm_spinup_current.dsa"
soilpath = "/data/paustian/ernie/SSURGO_master_script/soil_test2"
# d/i refers to dryland and irrigated, while c/r refers to cropland and rangeland
runtable_dc = "hugoton_attributes_Oct08_runtable.csv"
# runtable_dr = "hugoton_runtable_dr.csv"
# runtable_ic = "hugoton_test.csv"
# runtable_ir = "hugoton_test.csv"
print "Runtables:  ", runtable_dc
lib100_d = "025"
# lib100_i = "026"
print "Library100 archives:  ", lib100_d
over = "007"
print "Site.100 & schedule file override archive:  ", over
script = os.path.basename(__file__)
print "Code version:  ", script
    
#define main working, library, runtable, and results archive diretory paths
def DDC_runs(runtable, lib100, over):
    import time
    import datetime
    tstamp = datetime.datetime.now().strftime("%Y-%m-%d,%H.%M")   #timestamp for archive
    print "Results archive:  ", tstamp
    print
    abspath = os.path.abspath(__file__)     #get absolute path where script is located
    dname = os.path.dirname(abspath)        #associated directory only
    os.chdir(dname)
    dirmain = os.getcwd()
    dirlib = dirmain+"/library100/"+lib100
    dirarch = dirmain+"/"+tstamp
    dirover = dirmain+"/override/"+over
    start = time.time()                     #start analysis run timer
    print

    #move entire library100 and runtable archive contents to working directory
    import glob
    import shutil
    for f in glob.glob(os.path.join(dirlib, '*')):
        shutil.copy(f, dirmain)    

    #read and execute runtable line by line
    import csv
    Lines = csv.reader(open(runtable, 'rU'))
    tottreats = 0
    for row in Lines:
        tottreats +=1
    Lines = csv.reader(open(runtable, 'rU'))
    treatcount = -1
    for row in Lines:
        treatcount += 1
        run = row[0]
        fips = row[1]
        NARRx = row[2]
        NARRy = row[3]
        mukey = row[4]
        irr = row[5]
        nlcd = row[6]
        Study = row[7]
        Treat = row[8]
        Ecotype = row[9]
        Est_year = row[10]
        Plant_DOY = row[11]
        SGNE_rate = row[12]
        SGNE_DOY = row[13]
        Harv_skip = row[14]
        SGN1_rate = row[15]
        SGN1_DOY = row[16]
        SGN2_rate = row[17]
        SGN2_DOY = row[18]
        SGIR_rate = row[19]
        SGIR_DOY = row[20]
        Sene_DOY = row[21]
        Harv_DOY = row[22]
        id = row[23]
        print
        print
        comp = round((treatcount/float(tottreats))*100,3)
        NARR = "NARR_"+str(NARRx)+"_"+str(NARRy)
        print "Completion="+str(comp)+"% ("+str(treatcount)+"/"+str(tottreats)+" total runs)"
        print "Now running DayCent for treatment "+Treat+" using SSURGO mukey "+str(mukey)+","
        print NARR+".wth, and CFARM run index "+run
        print
        #define file
        os.chdir(dirmain)
        CFARMarch(DDCarchpath, run, sitpath)
        print "Using automatically-generated site.100 file from the CFARM archive."
        print
#           p = raw_input("Paused...")               #uncomment to manually extract template file
        SoilWth(soilpath, mukey, wthpath, NARR, dirmain)

        schfile = Treat+".sch"
        os.chdir(dirover)
        if os.path.exists(schfile):
            shutil.copy(schfile, dirmain)
            os.chdir(dirmain)
            os.rename(schfile, "sg.sch")
            print "Using manually-generated schedule file from override archive."
            print
#           p = raw_input("Paused...")               #uncomment to manually extract template file
        else:
            os.chdir(dirmain)
            SCHbuild(Est_year, Plant_DOY, Harv_skip, SGN1_DOY, SGN2_DOY, Sene_DOY, Harv_DOY)
            print "Using automatically-generated schedule file based on runtable."
            print
#           p = raw_input("Paused...")               #uncomment to manually extract template file
        FERTbuild(SGN1_rate, SGN2_rate, dirlib)
        DDcentEVI("sg.sch", id)
#       pause = raw_input("Pause...")
        #cleanup run-specific supporting files
        os.remove("site.100")
        os.remove("soils.in")
        os.remove("site.wth")
        os.remove("sg.sch")
        os.remove("fert.100")

    ### RUN SUMMARY
    sec = round((time.time() - start), 2)
    secpertreat = round(sec/tottreats, 2)
    min = round(sec/60.0, 2)
    sec = str(sec)
    secpertreat = str(secpertreat)
    min = str(min)
    tottreats = str(tottreats)
    print "Analysis complete."
    print "It took "+min+" minutes total to run the "+tottreats+" treatments ("+secpertreat+" sec/treatment)"
    print

    # log metadata, archive results, working directory cleanup
    print
    print
    logfile = tstamp+"_log.txt"
    print "All simulations complete, logging metadata as "+logfile
    c = open(logfile, "w")
    c.write("Analysis timstamp: "+tstamp+'\n')
    c.write("Runtable file: "+runtable+'\n')
    c.write("Library archive: "+lib100+'\n')
    c.write("Override archive: "+over+'\n')
    c.write("Model run automation code version: "+script+'\n')
    c.write("Weather file repository: "+wthpath+'\n')
    c.write("Soils.in file repository: "+soilpath+'\n')
    c.write("Site.100 file repository: "+sitpath+'\n')
    c.write("DDC version: DDcentEVI32 (17Nov2012)"+'\n')
    c.write("It took "+min+" minutes total to run the "+tottreats+" treatments ("+secpertreat+" sec/treatment)"+'\n')
    if not os.path.exists(dirarch):
        os.makedirs(dirarch)           #create archive directory if doesn't exist
        shutil.move(logfile, dirarch)
    for file in glob.glob(os.path.join(dirmain, '*')):
        if file.endswith(".in") or file.endswith(".txt") or file.endswith(".sch") or file.endswith(".100"):
            os.remove(file)            #delete remaining .in/.txt files from main directory
    for file in glob.glob(os.path.join(dirmain, '*')):
        if file.endswith(".bin") or file.endswith(".lis") or file.endswith(".out"):
            shutil.move(file, dirarch)      #archive all .bin/.lis/.out files together
    print
    print


DDC_runs(runtable_dc, lib100_d, over)
# DDC_runs(runtable_dr, lib100_d, over)
# DDC_runs(runtable_ic, lib100_i, over)
# DDC_runs(runtable_ir, lib100_i, over)