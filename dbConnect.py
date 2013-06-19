'''
Created on Jun 18, 2013

@author: johncallery
'''

# need to install mysql-python to get this first import to work
# and to be able to use mysql with python
# http://sourceforge.net/projects/mysql-python/ 
import MySQLdb
import yelpImporter

# use a 'try' clause because we're using a connection that can throw errors that we need to handle
# and that must be closed once we are done with it
try:
    # this is a connection to a specific database
    # at location "localhost" (this can be a URL or IP address)
    # with user "johncallery"
    # with password "mysql"
    # database name "yelp"
    yelpConnection = MySQLdb.Connect("localhost","johncallery","mysql","yelp")
    
    # a cursor is created from the connection, and it used to run queries and interact with the database tables
    yelpCursor = yelpConnection.cursor()
    
    # insert businesses records into the database
    # by passing the cursor and json file with business records
    # to the 'processBusinessJson' function in the 'yelpImporter.py' file
    # and processing the business json 
    print "Starting import of Yelp data..."
    
    print "Process Businesses"
    yelpImporter.processBusinessJson(yelpCursor,'/Users/johncallery/Documents/eclipse-workspace/yelp-test/data/yelp_training_set_business.json')
    
    # this ensures that the changes are made to the database
    yelpConnection.commit()
    print "Businesses Committed"
    
    print "Process Users"
    yelpImporter.processUserJson(yelpCursor,'/Users/johncallery/Documents/eclipse-workspace/yelp-test/data/yelp_training_set_user.json')
    yelpConnection.commit()
    print "Users Committed"
    
    print "Process Reviews"
    yelpImporter.processReviewJson(yelpCursor,'/Users/johncallery/Documents/eclipse-workspace/yelp-test/data/yelp_training_set_review.json')
    yelpConnection.commit()
    print "Reviews Committed"
    
    print "Process Checkins"
    yelpImporter.processCheckinJson(yelpCursor,'/Users/johncallery/Documents/eclipse-workspace/yelp-test/data/yelp_training_set_checkin.json')
    yelpConnection.commit()
    print "Checkins Committed"
    
    print "Completed Yelp Data Import"
    #this closes our cursor after committing to the database
    yelpCursor.close()
    '''
    # SQL query to execute
    yelpCursor.execute("show tables")
    
    # write each result to test
    for row in yelpCursor.fetchall() :
        print row[0]
    '''
# if there is an error attempting to connect to the database, print it out here
except MySQLdb.Error, e:
    # the error variable 'e' has two arguments that are part of it
    # print them out here, where e.args[0] is the error number and e.args[1] is the error description
    # %d and %s tell it what to print from the set that follows. %d is for decimal, %s is for string of characters
    print "Error %d: %s" % (e.args[0],e.args[1])

#if there was no error, execute this once the 'try' code is completed
finally:
    # check to see if the connection is still open
    if yelpConnection:
        # if it is still open, close it
        yelpConnection.close()
        