import csv
import datetime
import sys
import codecs

reload(sys)
sys.setdefaultencoding('utf8')

date_headers = ['Date', 'Post #', 'Ups #', 'Downs #', 'Score', '# comments']
author_headers = ['Author', 'Post #']
domain_headers = ['Domains', 'Post #', 'Ups #', 'Downs #', 'Score', '# comments']

with open('stats.csv', 'wb') as statsfile, codecs.open('reddit_sanitized.csv', 'r') as redditcsv:
    globalwriter = csv.writer(statsfile, delimiter=",",)
    reader = csv.reader(redditcsv, delimiter=",")

    dates = {}
    authors = {}
    domains = {}
    
    # [postnum, ups, downs, score, comments]

    next(reader)
    for row in reader:
        if len(row) < 6:
            for i in row:
                print i
                print "-"
            print "----"
            pass
            
        ups = int(row[6])
        downs = int(row[7])
        score = int(row[8])
        comments = int(row[14])

        date = datetime.datetime.strptime(row[4], "%Y/%m/%d %H:%M:%S").strftime("%Y/%m/%d")
        author = row[2]
        domain = row[10]
        
        if date in dates.keys():
            dates[date] = [dates[date][0] + 1, dates[date][1] + ups, dates[date][2] + downs, dates[date][3] + score, dates[date][4] + comments]
        else:
            dates[date] = [1, ups, downs, score, comments]

        if domain in domains.keys():
            domains[domain] = [domains[domain][0] + 1, domains[domain][1] + ups, domains[domain][2] + downs, domains[domain][3] + score, domains[domain][4] + comments]
        else:
            domains[domain] = [1, ups, downs, score, comments]

        if author in authors.keys():
            authors[author] = [authors[author][0] + 1, authors[author][1] + ups, authors[author][2] + downs, authors[author][3] + score, authors[author][4] + comments]
        else:
            authors[author] = [1, ups, downs, score, comments]

    globalwriter.writerow(date_headers)
    for a in dates:
        globalwriter.writerow([a] + dates[a])
    globalwriter.writerow(author_headers)
    for a in authors:
        globalwriter.writerow([a] + authors[a])
    globalwriter.writerow(domain_headers)
    for a in domains:
        globalwriter.writerow([a] + domains[a])
        
