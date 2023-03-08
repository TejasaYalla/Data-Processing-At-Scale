#!/usr/bin/python2.7
#
# Assignment4 - Query Processing
# Tirumala Tejasa Yalla- 1222375986- tyalla@asu.edu

import psycopg2
import os
import sys

#
#
name_of_database = 'dds_assignment'
RANGE_TABLE_PREFIX = 'RangeRatingsPart'
ROUND_ROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
WHERE_RATING_EQUAL = ' WHERE rating = '
SELECT = "SELECT '"
WHERE_RATING_GREATER_THAN = " WHERE rating >= "
SELECT_ROUND_ROBIN_PARTITIONS = "SELECT PartitionNum FROM RoundRobinRatingsMetadata"

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    dbCursors = openconnection.cursor()
    partitionList = []
    dbCursors.execute("select count(*) from RangeRatingsMetaData;")
    rangeCount = int(dbCursors.fetchone()[0])
    for part in range(rangeCount):
        partitionList.append(
            SELECT + RANGE_TABLE_PREFIX + str(
                part) + "' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(
                part) + WHERE_RATING_GREATER_THAN + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))
    dbCursors.execute(SELECT_ROUND_ROBIN_PARTITIONS)
    roundpartitions = int(dbCursors.fetchone()[0])

    for part in range(roundpartitions):
        partitionList.append(SELECT + ROUND_ROBIN_TABLE_PREFIX + str(
            part) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(part) +
                             WHERE_RATING_GREATER_THAN + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    rangequeryop = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(partitionList))
    fileop = open('RangeQueryOut.txt', 'w')

    write_range_file = "COPY (" + rangequeryop + ") TO '" + os.path.abspath(fileop.name) + "' (FORMAT text, DELIMITER ',')"
    dbCursors.execute(write_range_file)

    # dbCursors.close()
    fileop.close()
    pass


def PointQuery(ratingsTableName, ratingValue, openconnection):
    dbCursors = openconnection.cursor()
    partitionList = []

    dbCursors.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    rangeCount = int(dbCursors.fetchone()[0])

    for part in range(rangeCount):
        partitionList.append(
            SELECT + RANGE_TABLE_PREFIX + str(part) + "'AS tablename, userid, movieid, rating FROM rangeratingspart"
            + str(part) + WHERE_RATING_EQUAL + str(ratingValue))

    dbCursors.execute(SELECT_ROUND_ROBIN_PARTITIONS)
    roundn_partitions = int(dbCursors.fetchone()[0])

    for part in range(roundn_partitions):
        partitionList.append(SELECT + ROUND_ROBIN_TABLE_PREFIX + str(
            part) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
                             + str(part) + WHERE_RATING_EQUAL + str(ratingValue))

    queryop = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(partitionList))
    fileop = open('PointQueryOut.txt', 'w')

    pointqueryfile = "COPY (" + queryop + ") TO '" + os.path.abspath(fileop.name) + "' (FORMAT text, DELIMITER ',')"

    dbCursors.execute(pointqueryfile)
    # dbCursors.close()
    fileop.close()
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
