import io
import logging
import os
import pandas as pd
import sqlite3
import time

# Reference database with JWST data product descriptions
JW_PRODUCTS = "jwstfiles.db"

# Desired log file name
LOG = "scan_jwst_directory.log"

# Column names for association DataFrames
ASN_COLUMNS = ['file',
                'program_id',
                'ac_id',
                'pipeline',
                'datetime',
                'number'
               ]

# Column names for data product DataFrames
PRODUCT_COLUMNS = ['file',
                   'asn_number',
                   'program_id',
                   'instrument',
                   'detector',
                   'optical_elements',
                   'target_id',
                   'source_id',
                   'obs_number',
                   'visit_number',
                   'visit_group',
                   'parallel_seq',
                   'activity_number',
                   'exposure_number',
                   'suffix',
                   'description',
                   'units',
                   'stage'
                  ]

#--------------------

class JWSTAssociation():
    """ This class takes an association table filename upon init and digests
    that filename into relevant properties.

    :module __init__:  Initialize a JWSTAssociation class object.
    """

    def __init__(self, filename):
        """ Process an association filename into relevant information.  Expected: jw<prgid>-<asnid>_<dateTtime>_<pipeline>_<num>_asn.json

        :param filename:  The filename being read in.
        :type filename:  str
        """

        self.filename = filename

        # Cut off '.json'
        self.full = filename.split('.')[0]

        # Start with .formatted = True
        self.formatted = True

        # All major sections should be separated by '_'
        in_chunks = self.full.split('_')

        # Association filenames should have at least 5 major sections
        if len(in_chunks) < 5:
            self.formatted = False
            logging.error("{0} filename not formatted properly.".format(
                                                                 filename))
            return

        # The Program and Association Candidate ID's should be separated by a
        # '-'
        first = in_chunks[0][2:].split('-')
        if len(first) == 1:
            self.formatted = False
            logging.error("{0} filename not formatted properly.".format(
                                                                 filename))
            return
        self.program_id = first[0]
        self.ac_id = first[1]

        # Reformat the YYYYMMDDTHHMMSS datetime string
        second = in_chunks[1]
        #t = time.strptime(second, '%Y%m%dT%H%M%S')
        #self.datetime = time.strftime("%Y %b %d, %X", t)
        second = second.lower().split('t')[0]
        t = time.strptime(second, '%Y%m%d')
        self.datetime = time.strftime("%Y %b %d", t)

        # The pipeline and number sections are self-contained
        self.pipeline_element = in_chunks[2]
        self.number = in_chunks[3]

        # Package all properties into a tuple for easy access.  Make sure
        # ordering agrees with ASN_COLUMNS.
        self.tuple_ = (self.filename,
                       self.program_id,
                       self.ac_id,
                       self.pipeline_element,
                       self.datetime,
                       self.number
                      )

#--------------------

class JWSTProduct():
    """ This class takes a JWST data product filename upon init and digests
    that filename into relevant properties.

    :module __init__:  Initialize a JWSTProduct class object.
    """

    def __init__(self, filename):
        """ Process a data product filename into relevant information.

        :param filename:  The filename being read in.
        :type filename:  str
        """
        self.filename = filename
        self.full = filename.split(".")[0]

        in_chunks = self.full.split("_")
        print(in_chunks)
        if len(in_chunks) < 5:
            logging.error("{0} filename not formatted properly.".format(
                                                                 filename))
            self.formatted = False
            return
        else:
            self.formatted = True

        first = in_chunks[0].split("-")
        if len(first) == 1:
            first = first[0]
            self.program_id = first[2:7]
            self.obs_number = first[7:10]
            self.visit_number = first[10:13]
            self.asn_number = None
        else:
            self.program_id = first[0][2:]
            self.asn_number = first[1]
            self.obs_number = None
            self.visit_number = None

        second = in_chunks[1]
        if second.startswith('t'):
            self.target_id = second
            self.source_id = None
            self.visit_group = None
            self.parallel_seq = None
            self.activity_number = None
        elif second.startswith('s'):
            self.source_id = second
            self.target_id = None
            self.visit_group = None
            self.parallel_seq = None
            self.activity_number = None
        else:
            self.visit_group = second[0:2]
            self.parallel_seq = second[2]
            self.activity_number = second[3:]
            self.target_id = None
            self.source_id = None

        third = in_chunks[2]
        if third.isdigit():
            self.exposure_number = third
            self.instrument = None
        else:
            self.instrument = third
            self.exposure_number = None

        fourth = in_chunks[3:-1]
        if self.asn_number:
            as_string = str(fourth)
            self.optical_elements = as_string[1:-1]    #Strip '[]' from string
            self.detector = "N/A"
        else:
            self.detector = fourth[0]
            self.optical_elements = None

        det = self.detector.lower()
        if det.startswith('mir'):
            self.instrument = 'miri'
        elif det.startswith('nrc'):
            self.instrument = 'nircam'
        elif det.startswith('nis'):
            self.instrument = 'niriss'
        elif det.startswith('nrs'):
            self.instrument = 'nirspec'
        else:
            self.detector = None

        self.suffix = in_chunks[-1]

        self.tuple_ = (self.filename,
                       self.asn_number,
                       self.program_id,
                       self.instrument,
                       self.detector,
                       self.optical_elements,
                       self.target_id,
                       self.source_id,
                       self.obs_number,
                       self.visit_number,
                       self.visit_group,
                       self.parallel_seq,
                       self.activity_number,
                       self.exposure_number,
                       self.suffix
                      )

    def __str__(self):
        return self.filename

#--------------------

def find_files(directory):
    """ Scan a given directory using os.walk and search for any .fits or .json
    files.  Return a dictionary of both result lists.

    :param directory:  The directory to scan for files.
    :type directory:  str
    """

    # Make sure directory is complete
    directory = os.path.abspath(directory)
    print("Scanning {0}...".format(directory))

    # Initialize lists
    fits = []
    asn = []

    # Walk directory and add files ending in '.fits' or '.json'
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.endswith('.fits'):
                fits.append(f)
            elif f.endswith('.json'):
                asn.append(f)

    # Create the results dictionary and return it
    found_files = {'fits': sorted(fits), 'asn': sorted(asn)}
    print("...done!")
    return found_files

#--------------------

def connect_to_sqlite(db_file):
    """ Establish an sqlite3 database connection and cursor object.

    :param db_file:  The database to connect to.
    :type db_file:  str
    """

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    return (c, conn)

#--------------------

def add_columns(jw_properties, db_result, stage):
    """ Add properties found in a reference database query to a list of JWST
    data product file properties.

    :param jw_properties:  A list of JWST data product file properties.
    :type jw_properties:  list

    :param db_result:  The result of the reference database query for the given
                       data product type.  Holds [suffix, description, units].
    :type db_result:  list

    :param stage:  The stage of processing to add to the product file in
                   question.
    :type stage:  str
    """

    # Only need to add the description and units entries from db_result
    jw_properties.extend(db_result[1:])
    jw_properties.append(stage)

    return jw_properties

#--------------------

def add_filenames_to_db(files, ref_db, mem_db):
    """ Take a list of filenames and create a JWSTProduct object for each.
    Add to this information by querying the reference database.  Create a
    Pandas DataFrame to hold all the information and write that to the new
    memory database in a table called 'products'.

    :param files:  List of file names to add to the 'products' database table.
    :type files:  list

    :param ref_db:  Tuple containing sqlite3 cursor and connection items for
                    the JWST data products reference database.
    :type ref_db:  tuple

    :param mem_db:  Tuple containing sqlite3 cursor and connection items for
                    the new database in memory.
    :type mem_db:  tuple
    """

    # Get column format from the global variable
    columns = PRODUCT_COLUMNS

    # Pull sqlite3 cursor and connection items out of tuple
    c = ref_db[0]
    new_conn = mem_db[1]

    # Create empty Pandas DataFrame
    df = pd.DataFrame()

    for f in files:
        jw = JWSTProduct(f)

        # Skip file if .formatted not set
        if not jw.formatted:
            continue

        # Get processed tuple of JWSTProduct properties
        jw_list = list(jw.tuple_)

        # Query each table looking for the suffix of the current file
        suffix = []
        suffix.extend(list(c.execute('SELECT * FROM detector1 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM image2 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM spec2 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM ami3 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM coron3 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM image3 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM spec3 WHERE suffix=?',
                                     [jw.suffix])))
        suffix.extend(list(c.execute('SELECT * FROM tso3 WHERE suffix=?',
                                     [jw.suffix])))

        # Take last query result found, skip if none
        if len(suffix) > 0:
            suffix = suffix[-1]
        else:
            logging.error("{0} is not a valid file type.".format(jw.filename))
            continue

        # Add database query results to the JWSTProduct properties
        jw_list = add_columns(jw_list, suffix, '1')

        # Make a new DataFrame with the new information and add it to the main
        # frame
        new_row = pd.DataFrame(columns=columns, data=[jw_list])
        df = df.append(new_row, ignore_index=True)

    # Write the DataFrame to the 'products' table
    df.to_sql("products", new_conn, if_exists="replace", dtype='string')

    return df

#--------------------

def add_associations_to_db(asn_list, mem_db):
    conn = mem_db[1]
    columns = ASN_COLUMNS

    df = pd.DataFrame()
    for a in sorted(asn_list):
        asn = JWSTAssociation(a)
        if asn.formatted:
            asn_fields = list(asn.tuple_)
            new_row = pd.DataFrame(columns=columns, data=[asn_fields])
            df = df.append(new_row, ignore_index=True)
        else:
            continue

    df.to_sql("associations", conn, if_exists="replace", dtype='string')
    return df

#--------------------

def get_exposure_programs_list(mem_db):
    c = mem_db[0]
    programs = []
    for row in c.execute('SELECT program_id FROM products'):
        programs.append(str(row[0]))
    programs = list(set(programs))
    return programs

#--------------------

def pair_associations_and_exposures(programs, mem_db):
    c = mem_db[0]
    conn = mem_db[1]
    results = {}
    for p in programs:
        association_query = c.execute('SELECT * FROM associations WHERE \
                                      program_id=?', [p])
        association_query = c.fetchall()
        associations = [aq[1:] for aq in association_query]

        exposure_query = c.execute('SELECT * FROM products WHERE \
                                   program_id=?', [p])
        exposure_query = c.fetchall()
        exposures = [eq[1:] for eq in exposure_query]

        exposures_frame = pd.DataFrame(exposures, columns=PRODUCT_COLUMNS)
        exposures_title = "{0}_exposures".format(p)
        exposures_frame.to_sql(exposures_title, conn, if_exists="replace",
                               dtype='string')

        if len(associations) > 0:
            associations_frame = pd.DataFrame(associations,
                                              columns=ASN_COLUMNS)
            associations_title = "{0}_associations".format(p)
            associations_frame.to_sql(associations_title, conn,
                                      if_exists="replace", dtype='string')
        else:
            associations_frame = []
            logging.warning("No associations found for program, {0}".format(p))

        results[p] = {'associations': associations_frame,
                      'exposures': exposures_frame}

    return results

#--------------------

def write_db_to_disk(mem_db, new_db):
    c = mem_db[0]
    conn = mem_db[1]
    try:
        os.remove(new_db)
    except OSError:
        pass

    new_conn = sqlite3.connect(new_db)

    query = "".join(line for line in conn.iterdump())

    new_conn.executescript(query)

    new_conn.close()

#--------------------

def run(directory, output):

    #Start a log file
    outdir = os.getcwd()
    logfile = os.path.join(outdir, LOG)
    logging.basicConfig(filename=logfile,
                        format='***%(levelname)s from %(module)s: %(message)s',
                        level=logging.DEBUG, filemode='w')

    #Search for files in the provided directory
    files = find_files(directory)
    fits = files['fits']
    asn = files['asn']

    # Connect to databases
    # ref_db = Reference database with JWST data product descriptions
    # mem_db = New database in memory to add results of scan into
    ref_db = connect_to_sqlite(JW_PRODUCTS)
    mem_db = connect_to_sqlite(":memory:")

    #Add scan results to the databases
    fits_frame = add_filenames_to_db(fits, ref_db, mem_db)
    asn_frame = add_associations_to_db(asn, mem_db)

    #Create new database tables
    programs = get_exposure_programs_list(mem_db)
    results = pair_associations_and_exposures(programs, mem_db)

    #Save the new database to disk
    write_db_to_disk(mem_db, output)

    #Close all database connections
    ref_db[0].close()
    ref_db[1].close()
    mem_db[0].close()
    mem_db[1].close()

#--------------------

if __name__ == "__main__":
    d = input("Enter your JWST directory: ")
    d = os.path.abspath(d)
    if not os.path.isdir(d):
    	print(d), " does not exist!"
    	quit()
    print("-----Looking for files in {0}".format(d))

    out = input("Name the resulting database: ")
    if not out.endswith(".db"):
    	out = out + ".db"

    d = "/grp/jwst/ssb/test_build7.1/examples_for_dms"
    out = "mydir_files.db"
    run(d, out)
