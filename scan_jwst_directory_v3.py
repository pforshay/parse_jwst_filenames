from astropy.io import fits
import io
import json
import logging
import os
import pandas as pd
import sqlite3
import time

# Reference database with JWST data product descriptions
JW_PRODUCTS = "jwstproducts.db"

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
                   'member_of',
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
                   'level'
                  ]

#--------------------

def find_files(directory):
    """ Scan a given directory using os.walk and search for any .fits or .json
    files.  Return a dictionary of both result lists.

    :param directory:  The directory to scan for files.
    :type directory:  str
    """

    # Make sure directory is complete
    directory = os.path.abspath(directory)
    if not os.path.isdir(directory):
        logging.critical("{0} does not exist!".format(directory))
        return None
    else:
        print("Scanning {0}...".format(directory))

    # Initialize lists
    fits = []
    asn = []

    # Walk directory and add files ending in '.fits' or '.json'
    for root, dirs, files in os.walk(directory):
        for f in files:
            fullpath = os.path.join(root, f)
            if f.endswith('.fits'):
                fits.append(fullpath)
            elif f.endswith('.json'):
                asn.append(fullpath)

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

def write_dataframe_to_sql(dataframe, table_name, mem_db):
    conn = mem_db[1]
    dataframe.to_sql(table_name, conn, if_exists="replace", dtype='string')

#--------------------

#--------------------

def get_exposure_programs_list(mem_db):
    """ Return a list of the unique program ID's found in the products table
    of the memory database.

    :param mem_db:  Tuple containing memory database cursor and connections
                    objects.
    :type mem_db:  tuple
    """

    c = mem_db[0]
    programs = []
    for row in c.execute('SELECT program_id FROM products'):
        programs.append(str(row[0]))
    programs = list(set(programs))
    return programs

#--------------------

def create_program_tables(programs, mem_db):
    """ Create new tables in the memory database on a program-by-program basis
    for both data products and association files.

    :param programs:  List of unique program ID's found in the specified
                      directory.
    :type programs:  list

    :param mem_db:  Tuple containing memory database cursor and connections
                    objects.
    :type mem_db:  tuple
    """

    # Split the memory database tuple
    c = mem_db[0]
    conn = mem_db[1]

    results = {}
    for p in programs:

        # Query the 'associations' and 'products' tables for entries with a
        # matching program ID.  Grab columns [1:] to omit the original index.
        association_query = c.execute('SELECT * FROM associations WHERE \
                                      program_id=?', [p])
        association_query = c.fetchall()
        associations = [aq[1:] for aq in association_query]

        exposure_query = c.execute('SELECT * FROM products WHERE \
                                   program_id=?', [p])
        exposure_query = c.fetchall()
        products = [eq[1:] for eq in exposure_query]

        # Create a pandas DataFrame with the results of the 'products' query
        # and write it to a new table in the memory database.
        products_frame = pd.DataFrame(products, columns=PRODUCT_COLUMNS)
        products_title = "{0}_products".format(p)
        products_frame.to_sql(products_title, conn, if_exists="replace",
                               dtype='string')

        # The results of the 'associations' query may be empty, so this will
        # conditionally create the results DataFrame and create a db table.
        if len(associations) > 0:
            associations_frame = pd.DataFrame(associations,
                                              columns=ASN_COLUMNS)
            associations_title = "{0}_associations".format(p)
            associations_frame.to_sql(associations_title, conn,
                                      if_exists="replace", dtype='string')
        else:
            associations_frame = []
            logging.warning("No associations found for program, {0}".format(p))

        # Package to two results into a dictionary.
        results[p] = {'associations': associations_frame,
                      'products': products_frame}

    return results

#--------------------

def write_db_to_disk(mem_db, new_db):
    """ Copy an existing database in memory into a new database on disk.

    :param mem_db:  Cursor and connection objects for the existing database
                    in memory.
    :type mem_db:  tuple

    :param new_db:  File name for the new database file.
    :type new_db:  str
    """

    # Split the memory database tuple.
    c = mem_db[0]
    conn = mem_db[1]

    # Overwrite the database on disk if it exists.
    try:
        os.remove(new_db)
    except OSError:
        pass

    # Create the connection to the new database.
    new_conn = sqlite3.connect(new_db)

    # Add every line in the memory database into the new database.
    query = "".join(line for line in conn.iterdump())
    new_conn.executescript(query)

    # Close the new database connection.
    new_conn.close()

#--------------------

def create_asn_dict(asnlist):
    results = {}
    keywords = ["program",
                "asn_id",
                "asn_pool",
                "products"]
    print("Examining .json files...")
    for filepath in asnlist:
        f = {}
        f["filename"] = filepath.split("/")[-1]
        products = []
        members = []
        with open(filepath, 'r') as json_file:
            try:
                json_data = json.load(json_file)
            except json.decoder.JSONDecodeError:
                logging.error("{0} cannot be parsed into json.".format(
                                                                filepath))
                continue
            except UnicodeDecodeError:
                logging.error("{0} contains an invalid character.".format(
                                                                    filepath))
                continue

            for key in keywords:
                try:
                    f[key] = json_data[key]
                except KeyError:
                    f[key] = ""
            for p in f["products"]:
                products.append(p["name"])
                for m in p['members']:
                    members.append(m["expname"])
            f["products"] = products
            f["members"] = members
            json_file.close()

        results[filepath] = f

    print("...done!")
    return results

def create_fits_dict(fitslist):
    results = {}
    keywords = ["filename",
                "date-obs",
                "time-obs",
                "obs_id",
                "visit_id",
                "program",
                "observtn",
                "visit",
                "visitgrp",
                "seq_id",
                "act_id",
                "exposure",
                "instrume",
                "detector",
                "filter",
                "grating",
                "fxd_slit",
                "coronmsk",
                ]
    print("Examining .fits headers...")
    for filepath in fitslist:
        f = {}
        with fits.open(filepath) as fitsfile:
            hdr = fitsfile[0].header
            for key in keywords:
                try:
                    f[key] = hdr[key.upper()]
                except KeyError:
                    f[key] = ""
            fitsfile.close()
        results[filepath] = f

    print("...done!")
    return results

def add_asn_info_to_dict(product_dict, asn_dict):
    for p in sorted(product_dict.keys()):
        product = product_dict[p]
        product["used_by"] = ""
        for a in sorted(asn_dict.keys()):
            asn = asn_dict[a]
            if product["filename"] in asn["members"]:
                product["used_by"] = asn["filename"]

    return product_dict

def add_suffix_info_to_dict(product_dict, ref_db):
    c = ref_db[0]
    conn = ref_db[1]

    print("Looking up .fits suffix info...")
    for k in sorted(product_dict.keys()):
        product = product_dict[k]
        filename = product['filename']
        instrument = product['instrume'].lower()
        spl = filename.split(".")[0]
        suffix = spl.split("_")[-1]
        matches = {}
        c.execute('SELECT * FROM detector1 WHERE suffix=?', [suffix])
        matches['detector1'] = c.fetchone()
        c.execute('SELECT * FROM image2 WHERE suffix=?', [suffix])
        matches['image2'] = c.fetchone()
        c.execute('SELECT * FROM spec2 WHERE suffix=?', [suffix])
        matches['spec2'] = c.fetchone()
        c.execute('SELECT * FROM image3 WHERE suffix=?', [suffix])
        matches['image3'] = c.fetchone()
        c.execute('SELECT * FROM spec3 WHERE suffix=?', [suffix])
        matches['spec3'] = c.fetchone()
        c.execute('SELECT * FROM tso3 WHERE suffix=?', [suffix])
        matches['tso3'] = c.fetchone()
        if instrument == 'niriss':
            c.execute('SELECT * FROM ami3 WHERE suffix=?', [suffix])
            matches['ami3'] = c.fetchone()
            c.execute('SELECT * FROM coron3 WHERE suffix=?', [suffix])
            matches['coron3'] = c.fetchone()
        elif instrument == 'miri' or instrument == 'nircam':
            c.execute('SELECT * FROM coron3 WHERE suffix=?', [suffix])
            matches['coron3'] = c.fetchone()

        matches = {pipe: result for pipe, result
                                in matches.items()
                                if result is not None}
        if len(matches) == 0:
            logging.error("{0} is not a valid filetype.".format(filename))
            del product_dict[k]
            continue

        pipeline = list(matches.keys())[-1]
        suffix_info = matches[pipeline]
        product["description"] = suffix_info[1]
        product["units"] = suffix_info[2]
        product["level"] = suffix_info[3]

    print("...done!")
    return product_dict

def turn_dict_into_frame(fitsdict):
    df = pd.DataFrame()
    for entry in sorted(fitsdict.keys()):
        properties = fitsdict[entry]
        cols = list(properties.keys())
        vals = list(properties.values())
        row = pd.DataFrame(columns=cols, data=[vals])
        df = df.append(row, ignore_index=True)
    return df

def run(directory, output):
    """ Run all the steps to scan a given directory to parse JWST filenames
    into a database of data product properties.

    :param directory:  The directory to scan for JWST data files.
    :type directory:  str

    :param output:  The desired filename for the resulting database.
    :type output:  str
    """

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

    a = create_asn_dict(asn)
    d = create_fits_dict(fits)

    # Connect to databases
    # ref_db = Reference database with JWST data product descriptions
    # mem_db = New database in memory to add results of scan into
    ref_db = connect_to_sqlite(JW_PRODUCTS)
    mem_db = connect_to_sqlite(":memory:")

    d = add_asn_info_to_dict(d, a)
    d = add_suffix_info_to_dict(d, ref_db)
    af = turn_dict_into_frame(a)
    df = turn_dict_into_frame(d)
    #write_dataframe_to_sql(af, "associations", mem_db)
    write_dataframe_to_sql(df, "products", mem_db)
    write_db_to_disk(mem_db, output)
    #print(df)

    #Close all database connections
    ref_db[0].close()
    ref_db[1].close()
    mem_db[0].close()
    mem_db[1].close()

#--------------------

if __name__ == "__main__":

    # Get user input for a directory to find JWST files
    d = input("Enter your JWST directory: ")
    d = os.path.abspath(d)
    if not os.path.isdir(d):
    	print(d), " does not exist!"
    	quit()
    print("-----Looking for files in {0}".format(d))

    # Get user input to name the resulting database file
    out = input("Name the resulting database: ")
    if not out.endswith(".db"):
    	out = out + ".db"

    d = "/grp/jwst/ssb/test_build7.1/examples_for_dms"
    out = "mydir_files.db"
    run(d, out)
