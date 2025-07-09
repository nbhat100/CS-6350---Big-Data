import sys
from threading import Timer
import time
from kafka import KafkaProducer
import praw
import config

if len(sys.argv) != 3:
    print("""
        Usage: streamRedditData.py <bootstrap-servers> <outputTopic>
        """, file=sys.stderr)
    sys.exit(-1)

bootstrapServers = [sys.argv[1]]
outputTopic = str(sys.argv[2])

reddit = praw.Reddit(
    client_id=config.reddit_client_id, 
    client_secret=config.reddit_client_secret,
    user_agent='UTD Agent')

newsSubreddit = reddit.subreddit('news')
# commentsMessage = []
# def getComments():
#     for post in newsSubreddit.hot(limit=50):
#         post.comments.replace_more(limit=0)
#         comments = list(post.comments)
#         for idx in range(len(comments)):
#             if comments[idx] and comments[idx].created_utc >= startTime and len(comments[idx].body) > 0:
#                 commentsMessage.append(comments[idx].body)
#     print(commentsMessage)
#     producer = KafkaProducer(bootstrap_servers=bootstrapServers,
#                          value_serializer=lambda x: bytes(x, 'utf-8'))
#     for comment in commentsMessage:
#         producer.send(topic=outputTopic, value=comment)
#     producer.close()
# timer = Timer(60 * 15, getComments)
# startTime = time.time()
# timer.start()
#fetch data from movies subreddit and send to topic1
producer = KafkaProducer(bootstrap_servers=bootstrapServers,
                          value_serializer=lambda x: bytes(x, 'utf-8'))

for comment in newsSubreddit.stream.comments(skip_existing=True):
    commentText = comment.body
    producer.send(outputTopic, value=commentText)
    producer.flush()