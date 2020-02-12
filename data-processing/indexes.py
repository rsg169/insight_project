# INDEXES.PY: This script contains data structures that allow file paths to be constructed per user request.

# This dictionary corresponds keys describing archive date to values describing a signature substring in the index file path
DICT = {
        'December 2016' : '2016-50',
        'January 2017' : '2017-04', 'February 2017' : '2017-09', 'March 2017' : '2017-13', 'April 2017' : '2017-17', 'May 2017' : '2017-22', 'June 2017' : '2017-26',
        'July 2017' : '2017-30', 'August 2017' : '2017-34', 'September 2017' : '2017-39', 'October 2017'   : '2017-43', 'November 2017'  : '2017-47', 'December 2017'  : '2017-51',
        'January 2018' : '2018-05', 'February 2018' : '2018-09', 'March 2018' : '2018-13', 'April 2018' : '2018-17', 'May 2018' : '2018-22', 'June 2018' : '2018-26', 
        'July 2018' : '2018-30', 'August 2018' : '2018-34', 'September 2018' : '2018-39', 'October 2018'   : '2018-43', 'November 2018'  : '2018-47', 'December 2018'  : '2018-51',
        'January 2019' : '2019-04', 'February 2019' : '2019-09', 'March 2019' : '2019-13', 'April 2019' : '2019-18', 'May 2019' : '2019-22', 'June 2019' : '2019-26',
        'July 2019' : '2019-30', 'August 2019' : '2019-35', 'September 2019' : '2019-39', 'October 2019'   : '2019-43', 'November 2019'  : '2019-47', 'December 2019'  : '2019-51',
        'January 2020' : '2020-05'
       }

# This is a list of archive dates in sequential order (without gaps), which limits the range of times that the user can request
LIST = [
        'December 2016',
        'January 2017', 'February 2017', 'March 2017', 'April 2017', 'May 2017', 'June 2017', 'July 2017', 'August 2017', 'September 2017', 'October 2017', 'November 2017', 'December 2017',
        'January 2018', 'February 2018', 'March 2018', 'April 2018', 'May 2018', 'June 2018', 'July 2018', 'August 2018', 'September 2018', 'October 2018', 'November 2018', 'December 2018',
        'January 2019', 'February 2019', 'March 2019', 'April 2019', 'May 2019', 'June 2019', 'July 2019', 'August 2019', 'September 2019', 'October 2019', 'November 2019', 'December 2019',
        'January 2020'
       ]

# Construct and return a list of paths to index files that satisfy the time constraint specified by the user
def getIndexes(start,end):

    i = LIST.index(start)

    indexList = []
    indexList.append('s3://commoncrawl/crawl-data//CC-MAIN-'+DICT[start]+'/wet.paths.gz')
    while (LIST[i] != end):
        i += 1
        indexList.append('s3://commoncrawl/crawl-data/CC-MAIN-'+DICT[LIST[i]]+'/wet.paths.gz')

    return indexList
