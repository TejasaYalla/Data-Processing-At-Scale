#!/usr/bin/python2.7
#
# Interface for the Assignment 3
# Tirumala Tejasa Yalla_ tyalla@asu.edu-1222375986

#

import psycopg2
import math

# Variables
CREATE_TABLE = 'Create TABLE '
ALTER_TABLE = 'ALTER TABLE '
RANGE_PARTITION_NAME = 'range_part'
ROUND_ROBIN_PARTITION_NAME = 'rrobin_part'
INSERT_INTO = "insert into "
RATINGS_COLUMNS_VALUES = '(userid,movieid,rating) values ('


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    dbCursor = openconnection.cursor()
    dbCursor.execute(
        CREATE_TABLE + ratingstablename + " (userid INT,temp1 VARCHAR, movieid INT,temp2 VARCHAR,rating FLOAT,temp3 VARCHAR,timestamp BIGINT)")
    openconnection.commit()
    input_Ratings_File = open(ratingsfilepath, 'r')
    dbCursor.copy_from(input_Ratings_File, ratingstablename, sep=':',
                       columns=('userid', 'temp1', 'movieid', 'temp2', 'rating', 'temp3', 'timestamp'))
    openconnection.commit()
    dbCursor.execute(
        ALTER_TABLE + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp2,DROP COLUMN temp3, DROP COLUMN timestamp")
    dbCursor.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    if math.floor(numberofpartitions) != math.ceil(numberofpartitions):
        return
    partitions_array = []
    part_name = RANGE_PARTITION_NAME
    part_values = round((5 / numberofpartitions), 2)
    b = 0
    for part_index in range(0, numberofpartitions):
        partitions_array.append((part_name + str(part_index)))
    for part_index in range(0, numberofpartitions):
        if part_index == 0:
            dbCursor = openconnection.cursor()
            dbCursor.execute(CREATE_TABLE + partitions_array[part_index] + " (userid INT,movieid INT,rating FLOAT)")
            openconnection.commit()
            dbCursor.execute(INSERT_INTO + partitions_array[
                part_index] + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(
                0) + " and rating <= " + str(part_values) + ";")
            openconnection.commit()
            b = part_values
        else:
            dbCursor = openconnection.cursor()
            dbCursor.execute(CREATE_TABLE + partitions_array[part_index] + " (userid INT,movieid INT,rating FLOAT)")
            openconnection.commit()
            dbCursor.execute(INSERT_INTO + partitions_array[
                part_index] + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(
                b) + " and rating <= " + str(b + part_values) + ";")
            openconnection.commit()
            b = b + part_values


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    if math.floor(numberofpartitions) != math.ceil(numberofpartitions):
        return
    partitions_array = []
    part_name = ROUND_ROBIN_PARTITION_NAME
    for part_index in range(0, numberofpartitions):
        partitions_array.append((part_name + str(part_index)))
    for part_index in range(0, numberofpartitions):
        dbcursor = openconnection.cursor()
        dbcursor.execute(CREATE_TABLE + partitions_array[part_index] + " (userid INT,movieid INT,rating FLOAT)")
        openconnection.commit()
        if part_index != numberofpartitions - 1:
            dbcursor.execute(INSERT_INTO + partitions_array[
                part_index] + " select userid,movieid,rating from (select row_number() over() as row_id, * from " + ratingstablename + ") as imp where row_id%" + str(
                numberofpartitions) + "=" + str(part_index + 1))
            openconnection.commit()
        else:
            dbcursor.execute(INSERT_INTO + partitions_array[
                part_index] + " select userid,movieid,rating from (select row_number() over() as row_id, * from " + ratingstablename + ") as imp where row_id%" + str(
                numberofpartitions) + "=" + str(0))
            openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating < 0 or rating > 5):
        return
    dbCursor = openconnection.cursor()
    dbCursor.execute(
        "select count(*) from (SELECT tablename FROM pg_catalog.pg_tables WHERE tablename like 'rrobin_part%') as temp")
    partition_count = int(dbCursor.fetchone()[0])
    dbCursor.execute('SELECT COUNT(*) from {}'.format(ratingstablename))
    dataset_count = int(dbCursor.fetchone()[0])
    part_name = ROUND_ROBIN_PARTITION_NAME + str((dataset_count % partition_count))
    dbCursor.execute(INSERT_INTO + ratingstablename + RATINGS_COLUMNS_VALUES + str(userid) + "," + str(
        itemid) + "," + str(rating) + ")")
    openconnection.commit()
    dbCursor.execute(
        INSERT_INTO + part_name + RATINGS_COLUMNS_VALUES + str(userid) + "," + str(itemid) + "," + str(
            rating) + ")")
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating < 0 or rating > 5):
        return
    dbCursor = openconnection.cursor()
    dbCursor.execute(
        "select count(*) from (SELECT tablename FROM pg_catalog.pg_tables WHERE tablename like 'range_part%') as temp")
    count_of_partition = int(dbCursor.fetchone()[0])
    part_values = round((5 / count_of_partition), 2)
    partition_count = int(rating / part_values)
    if rating % part_values == 0 and partition_count != 0:
        partition_count = partition_count - 1
    part_name = RANGE_PARTITION_NAME + str(partition_count)
    dbCursor.execute(
        INSERT_INTO + ratingstablename + RATINGS_COLUMNS_VALUES + str(userid) + "," + str(
            itemid) + "," + str(rating) + ")")
    openconnection.commit()
    dbCursor.execute(
        INSERT_INTO + part_name + RATINGS_COLUMNS_VALUES + str(userid) + "," + str(itemid) + "," + str(
            rating) + ")")
    openconnection.commit()


def createDB(dbname='dps_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
