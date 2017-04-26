import re
import math
import MySQLdb

requireDBInit = True #True for first DB init
testSetK = 0   # size of table test_votes
dataFile = "u.data"
connection = MySQLdb.connect(
     host="localhost",  # your host, usually localhost
     user="root",  # your username
     passwd="test",  # your password
     db="testdb"     # your testdb
)
cursor = connection.cursor()


def main():
    if requireDBInit:
        print "rebuilding DB"
        initDB()
    else:
        print "work with existing DB"

    # test set (k) = "test_votes", training set (all - k) = "user_votes"

    #print calcCosSimilarity(144, 226, "user_votes")
    print calcRMSE("user_votes") # table

    #end db connection
    connection.commit()
    connection.close()


def initDB():
    #deletes old tables
    checkForOldTable("test_votes")
    checkForOldTable("user_votes")

    # create new table user_votes
    cursor.execute("""
        CREATE TABLE user_votes (
            vote_number MEDIUMINT PRIMARY KEY NOT NULL AUTO_INCREMENT,
            userID MEDIUMINT,
            movieID MEDIUMINT,
            rating MEDIUMINT
        );"""
    )
    print " -> created new table user_votes"

    # build data set for user_votes
    with open(dataFile) as file:
        for line in file:
            line = re.split(r'\t+', line)
            cursor.execute("""
                INSERT INTO user_votes ( userID, movieID, rating)
                VALUES ( %s , %s , %s );""",
                (line[0], line[1], line[2])
            )
    print " -> wrote data set into user_votes"

    # create table test_votes
    cursor.execute("""
        CREATE TABLE test_votes (
            vote_number MEDIUMINT PRIMARY KEY NOT NULL AUTO_INCREMENT,
            userID MEDIUMINT,
            movieID MEDIUMINT,
            rating MEDIUMINT
        );"""
    )
    print " -> created new table test_votes"

    #build data set for test_votes using K
    if testSetK!=0:
        cursor.execute("""
            SELECT DISTINCT userID
            FROM user_votes
            ;"""
        )
        row = cursor.fetchall()
        for uid in row:
            cursor.execute("""
                SELECT userID, movieID, rating, vote_number
                FROM user_votes
                WHERE userID=%s
                LIMIT %s
            """, (uid[0] , testSetK))
            result = cursor.fetchall()
            if result is not None:
                query = """INSERT INTO test_votes(userID, movieID, rating) VALUES"""
                for line in result:
                    query += " (%s, %s, %s)," % (line[0], line[1], line[2])
                    cursor.execute("""
                                DELETE
                                FROM user_votes
                                WHERE vote_number=%s
                                ;""",
                                   (line[3]))
            cursor.execute(query[:-1])


    print " -> moved data from user_votes to test_votes"


def checkForOldTable(table):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = %s
        """, (table))
    if cursor.fetchone()[0] == 1:
        query = "DROP TABLE %s" % (table)
        cursor.execute(query)
        print " -> deleted old table " + table


def calcCosSimilarity(movie1ID, movie2ID, table):
    query = """
    SELECT
        SUM(dim1*dim2) /
        (SQRT(SUM(dim1*dim1)) * SQRT(SUM(dim2*dim2)))
    FROM (
            SELECT
                mov1.movieID as movie1ID,
                mov2.movieID as movie2ID,
                mov1.rating - uAvg.avgRating as dim1,
                mov2.rating - uAvg.avgRating as dim2
            FROM
                user_votes as mov1
            INNER JOIN %(table)s as mov2
            ON mov1.userID=mov2.userID AND mov1.movieID<>mov2.movieID

            INNER JOIN
                (SELECT userID, AVG(rating) AS avgRating
                FROM user_votes
                GROUP BY userID
                ) AS uAvg
            ON mov1.userID = uAvg.userID

            WHERE mov1.movieID= %(movie2ID)s AND mov2.movieID= %(movie1ID)s
            GROUP BY mov1.userID, mov2.userID
    ) as result_table
    GROUP BY movie1ID, movie2ID
    """ % { "table": table, "movie1ID": movie1ID, "movie2ID": movie2ID}
    cursor.execute(query)
    cos = cursor.fetchone()
    if cos is not None:
        return cos[0]
    else:
        return 0

def calcPrediction(userID, movieID, table):
    query="Select movieID, rating from %s where userId=%s" %  (table, userID)
    cursor.execute(query)
    result=cursor.fetchall()
    denominator = 0
    numerator = 0
    for line in result:
        cos= calcCosSimilarity(movieID, line[0], table)
        if line[1] != None and cos != None:
            numerator += line[1] * cos
            denominator += abs(cos)

    #normalisation
    if numerator != 0 and denominator != 0:
        return round(numerator / denominator *2/5+3)
    else:
        return 0

def calcRMSE(table):
    # get overall rating count
    query = "SELECT count(*) FROM %s" % (table)
    cursor.execute(query)
    overallCount = cursor.fetchone()[0]
    print overallCount

    # calculate sum
    query = "SELECT userID, movieID, rating FROM %s" % (table)
    cursor.execute(query)
    result = cursor.fetchall()
    sum = 0.
    for line in result:
        prediction = calcPrediction(line[0],line[1],table)
        if prediction != None and prediction != 0:
            print (prediction, line[2])
            sum += math.pow(prediction - line[2] , 2)


    return math.sqrt( 1./ overallCount * sum)

main()