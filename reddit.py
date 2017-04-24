import praw
import csv

import sys
reload(sys)
sys.setdefaultencoding('utf8')

r=praw.Reddit(client_id="", client_secret="",password="",username="",user_agent="")
subs = ['economics', 'girlsgonebitcoin', 'bitcoin', 'cryptocurrency']
headers = ['ID', 'Title', 'Author', 'Text', 'Created', 'Likes', 'Ups', 'Downs', 'Score', 'URL', 'Thumbnail', 'Fullname', 'Gilded', '# comments', '# reports', 'Over 18', 'Sub']
commentheaders = ['ID', 'Post ID', 'Post Parent', 'Author', 'Text', 'Created', 'Likes', 'Ups', 'Downs', 'Score', 'Sub']
epoch_jump = 1209600 



def getComments(c, p, s, writer):
    try:
        commentwriter.writerow([c.id, p.id, c.parent_id, c.author, c.body, c.created, c.likes, c.ups, c.downs, c.score, s])
        for r in c.replies:
            getComments(r, p, s, writer)
    except:
        pass



total_posts = 0
with open('reddit_global.csv', 'wb') as globalcsv, open('reddit_comments.csv', 'wb') as globalcomments, open('log.txt', 'wb') as log:
    globalwriter = csv.writer(globalcsv, delimiter=",")
    globalwriter.writerow(headers)
    commentwriter = csv.writer(globalcomments, delimiter=",")
    commentwriter.writerow(commentheaders)
    
    for s in subs:
        print "--------- Getting s/{0} data -----------".format(s)
        log.write("--------- Getting s/{0} data -----------\n".format(s))
        
        sub = r.subreddit(s)
        
        begin_epoch = 1420070400 # 1262304000 # 1/1/2010
        end_epoch = begin_epoch + epoch_jump 
        retry = False
        
        while(end_epoch < 1492537581):
            ex = "(retrying)" if retry else ""
            print "  -- From {0} to {1} {2}-- ".format(begin_epoch, end_epoch, ex)
            log.write("  -- From {0} to {1} {2}-- \n".format(begin_epoch, end_epoch, ex))
            retry = False
            
            try:
                num = 0
                for p in sub.search('timestamp:{0}..{1}'.format(begin_epoch, end_epoch),sort='new',limit=None,syntax='cloudsearch'):                
                    #print "> Post '{0}' [{1}]".format(p.title, p.id)
                    globalwriter.writerow([p.id, p.title, p.author, p.selftext, p.created, p.likes, p.ups, p.downs, p.score, p.url, p.thumbnail, p.fullname, p.gilded, p.num_comments, p.num_reports, p.over_18, s])
            
                    for c in p.comments:
                        getComments(c, p, s, commentwriter)                     
                        
                        
                    num = num + 1
                    total_posts = total_posts + 1
                
                
                print "    > Posts found: {0}".format(num)
                log.write("    > Posts found: {0}\n".format(num))
                begin_epoch = end_epoch + 1
                end_epoch = begin_epoch + epoch_jump
                
            except Exception as e: 
                print "Exception: {0}\n\t --> Epochs: {1}..{2}".format(str(e), begin_epoch, end_epoch)
                log.write("Exception: {0}\n\t --> Epochs: {1}..{2}\n".format(str(e), begin_epoch, end_epoch))
                retry = True
                    
            
              
    print "\nFinished creating files. Total posts fetched: {0}".format(total_posts)
    log.write("\nFinished creating files. Total posts fetched: {0}".format(total_posts))
