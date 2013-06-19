'''
Created on Jun 18, 2013



@author: johncallery
'''

import json

# takes in a string with the full path to the business json file
# and processes it by creating database records for all entries
def processBusinessJson(yelpCursor,filePathString):
    try:
        # open connection to the file
        yelpBusinessFile = open(filePathString)
        
        # read a line from the file
        # assumes each JSON entry is on a separate line
        yelpBusinessLine = yelpBusinessFile.readline()
        
        # if the file is empty, readline will return an empty string, or ""
        # if it is not empty, iterate through all lines in the file
        # and process json content by inserting it into the relevant tables
        while yelpBusinessLine != "":            
            # use JSONDecoder, which is part of the json package imported at the top
            # to process the string and return a python dictionary of json values
            yelpBusiness = json.loads(yelpBusinessLine)
            
            # you can get specific values by referencing the name of the desired element. 
            # for example, if you take the comment out from the following line, it will 
            # print just the business id:
            # print yelpBusiness.get('business_id',None)
            # the second parameter, None, is the value to return if 'business_id' doesn't exist
            
            # pull business_id as a standalone variable because it is reused for multiple inserts
            # using the string_escape format for everything because i don't trust the data
            # also, unsure of characters allowed in encrypted business id
            bizId = str(yelpBusiness.get('business_id',None)).encode('string_escape')
            
            # use the json get method to insert new business information into the database
            # use backslash to escape single quote, because format requires single quotes around string values.
            # do not use single quotes for integer, decimal, boolean, etc.            
            insertBusinessStatement = 'INSERT INTO business (business_id,name,full_address,city,latitude,longitude,average_stars,review_count,open) VALUES (\''+bizId+'\',\''+str(yelpBusiness.get('name',None)).encode('string_escape')+'\',\''+str(yelpBusiness.get('full_address',None)).encode('string_escape')+'\',\''+str(yelpBusiness.get('city',None)).encode('string_escape')+'\','+str(yelpBusiness.get('latitude',None)).encode('string_escape')+','+str(yelpBusiness.get('longitude',None)).encode('string_escape')+','+str(yelpBusiness.get('stars',None)).encode('string_escape')+','+str(yelpBusiness.get('review_count',None)).encode('string_escape')+','+str(yelpBusiness.get('open',None)).encode('string_escape')+')'
            yelpCursor.execute(insertBusinessStatement)
            
            # get list of categories
            bizCategoryList = yelpBusiness.get('categories')
            
            # insert each category into database with the associated business_id
            for bizCategory in bizCategoryList:
                insertCategoryStatement = 'INSERT INTO category (business_id,name) VALUES (\''+bizId+'\',\''+str(bizCategory).encode('string_escape')+'\')'
                yelpCursor.execute(insertCategoryStatement)
            # END for each bizCategory in bizCategoryList
            
            # get list of neighborhoods
            neighborhoodList = yelpBusiness.get('neighborhoods')
            
            # insert each neighborhood into database with the associated business_id
            for neighborhood in neighborhoodList:
                insertNeighborhoodStatement = 'INSERT INTO neighborhood (business_id,name) VALUES (\''+bizId+'\',\''+str(neighborhood).encode('string_escape')+'\')'
                yelpCursor.execute(insertNeighborhoodStatement)
            # END for each neighborhood in neighborhoodList
            
            # read in the next line to process inside of the while loop
            yelpBusinessLine = yelpBusinessFile.readline()
        # END while there are more entries in the file
            
        
    except IOError, e:
        print 'Error %d: %s' % (e.args[0],e.args[1])
    finally:
        # end by closing the connection to the file if still open
        if yelpBusinessFile:
            yelpBusinessFile.close()
# END function processBusinessJson

# takes in a string with the full path to the user json file
# and processes it by creating database records for all entries
def processUserJson(yelpCursor,filePathString):
    try:
        # open connection to the file
        yelpUserFile = open(filePathString)
        
        # read a line from the file
        # assumes each JSON entry is on a separate line
        yelpUserLine = yelpUserFile.readline()
        
        # if the file is empty, readline will return an empty string, or ""
        # if it is not empty, iterate through all lines in the file
        # and process json content by inserting it into the relevant tables
        while yelpUserLine != "":
            
            # use JSONDecoder, which is part of the json package imported at the top
            # to process the string and return a python dictionary of json values
            yelpUser = json.loads(yelpUserLine)
            
            # use the json get method to insert new user information into the database
            # use backslash to escape single quote, because format requires single quotes around string values.
            # do not use single quotes for integer, decimal, boolean, etc.            
            insertUserStatement = 'INSERT INTO user (user_id,name,average_stars,review_count,useful_votes,funny_votes,cool_votes) VALUES (\''+str(yelpUser.get('user_id')).encode('string_escape')+'\',\''+str(yelpUser.get('name',None)).encode('string_escape')+'\','+str(yelpUser.get('average_stars',None)).encode('string_escape')+','+str(yelpUser.get('review_count',None)).encode('string_escape')+','+str(yelpUser.get('votes',None).get('useful',None)).encode('string_escape')+','+str(yelpUser.get('votes',None).get('funny',None)).encode('string_escape')+','+str(yelpUser.get('votes',None).get('cool',None)).encode('string_escape')+')'
            yelpCursor.execute(insertUserStatement)
            
            # read in the next line to process inside of the while loop
            yelpUserLine = yelpUserFile.readline()
        # END while there are more entries in the file
            
        
    except IOError, e:
        print 'Error %d: %s' % (e.args[0],e.args[1])
    finally:
        # end by closing the connection to the file if still open
        if yelpUserFile:
            yelpUserFile.close()
# END function processUserJson

# takes in a string with the full path to the review json file
# and processes it by creating database records for all entries
def processReviewJson(yelpCursor,filePathString):
    try:
        # open connection to the file
        yelpReviewFile = open(filePathString)
        
        # read a line from the file
        # assumes each JSON entry is on a separate line
        yelpReviewLine = yelpReviewFile.readline()
        
        # if the file is empty, readline will return an empty string, or ""
        # if it is not empty, iterate through all lines in the file
        # and process json content by inserting it into the relevant tables
        while yelpReviewLine != "":
            
            # use JSONDecoder, which is part of the json package imported at the top
            # to process the string and return a python dictionary of json values
            yelpReview = json.loads(yelpReviewLine)
            
            # use the json get method to insert new user information into the database
            # use backslash to escape single quote, because format requires single quotes around string values and dates.
            # do not use single quotes for integer, decimal, boolean, etc.            
            insertReviewStatement = 'INSERT INTO review (business_id,user_id,date,stars,useful_votes,funny_votes,cool_votes,text) VALUES (\''+str(yelpReview.get('business_id')).encode('string_escape')+'\',\''+str(yelpReview.get('user_id',None)).encode('string_escape')+'\',\''+str(yelpReview.get('date',None)).encode('string_escape')+'\','+str(yelpReview.get('stars',None)).encode('string_escape')+','+str(yelpReview.get('votes',None).get('useful',None)).encode('string_escape')+','+str(yelpReview.get('votes',None).get('funny',None)).encode('string_escape')+','+str(yelpReview.get('votes',None).get('cool',None)).encode('string_escape')+',\''+str(yelpReview.get('text',None)).encode('string_escape')+'\')'
            yelpCursor.execute(insertReviewStatement)
            
            # read in the next line to process inside of the while loop
            yelpReviewLine = yelpReviewFile.readline()
        # END while there are more entries in the file
            
        
    except IOError, e:
        print 'Error %d: %s' % (e.args[0],e.args[1])
    finally:
        # end by closing the connection to the file if still open
        if yelpReviewFile:
            yelpReviewFile.close()
# END function processReviewJson

def processCheckinJson(yelpCursor,filePathString):
    try:
        # open connection to the file
        yelpCheckinFile = open(filePathString)
        
        # read a line from the file
        # assumes each JSON entry is on a separate line
        yelpCheckinLine = yelpCheckinFile.readline()
        
        # if the file is empty, readline will return an empty string, or ""
        # if it is not empty, iterate through all lines in the file
        # and process json content by inserting it into the relevant tables
        while yelpCheckinLine != "":            
            # use JSONDecoder, which is part of the json package imported at the top
            # to process the string and return a python dictionary of json values
            yelpCheckin = json.loads(yelpCheckinLine)
            
            # you can get specific values by referencing the name of the desired element. 
            # for example, if you take the comment out from the following line, it will 
            # print just the business id:
            # print yelpBusiness.get('business_id',None)
            # the second parameter, None, is the value to return if 'business_id' doesn't exist
            
            # pull business_id as a standalone variable because it is reused for multiple inserts
            # using the string_escape format for everything because i don't trust the data
            # also, unsure of characters allowed in encrypted business id
            bizId = str(yelpCheckin.get('business_id',None)).encode('string_escape')
            
            # get list of categories
            checkinTimeList = yelpCheckin.get('checkin_info')
            
            # insert each category into database with the associated business_id
            for checkinTime in checkinTimeList:
                # split the "12-4" format that represents "hour-day" into an array containing the two integers
                # this way we can insert the data in a meaningful way so that daily/hourly analysis is simpler
                hour_day = checkinTime.split("-")
                
                insertCheckinStatement = 'INSERT INTO checkin (business_id,day,hour,number) VALUES (\''+bizId+'\','+hour_day[1]+','+hour_day[0]+','+str(checkinTimeList.get(checkinTime,None)).encode('string_escape')+')'
                
                yelpCursor.execute(insertCheckinStatement)
            # END for each checkinTime in checkinTimeList
            
            # read in the next line to process inside of the while loop
            yelpCheckinLine = yelpCheckinFile.readline()
        # END while there are more entries in the file
            
        
    except IOError, e:
        print 'Error %d: %s' % (e.args[0],e.args[1])
    finally:
        # end by closing the connection to the file if still open
        if yelpCheckinFile:
            yelpCheckinFile.close()
# END function processCheckinJson