import os
import numpy as np
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.errors import HttpError
import pandas as pd
import json
import socket
import socks
import requests
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


def use_credentials(credentials,client_secrets_file):
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)    
    #  Check if the credentials are invalid or do not exist 
    if not credentials or not credentials.valid:
        # Check if the credentials have expired
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
            credentials = flow.run_console()

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)
    return credentials
    
    
def get_comments(video_Id,credentials,api_service_name,api_version,searchTerms):
    import googleapiclient.discovery
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=credentials)
    video_Id = video_Id
    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_Id,
        searchTerms=searchTerms,
        maxResults = 100,
    )
    response = request.execute()

    totalResults = 0
    totalResults = int(response['pageInfo']['totalResults'])

    count = 0
    nextPageToken = ''
    comments = []
    first = True
    further = True
    while further:
        halt = False
        if first == False:
            print('..')
            try:
                response = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_Id,
                    searchTerms=searchTerms,
                    maxResults = 100,
                    textFormat='html',
                    pageToken=nextPageToken
                            ).execute()
                totalResults = int(response['pageInfo']['totalResults'])
            except HttpError as e:
                print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
                halt = True

        if halt == False:
            count += totalResults
            for item in response["items"]:
                # 这只是一部分数据，你需要啥自己选就行，可以先打印下你能拿到那些数据信息，按需爬取。
                comment = item["snippet"]["topLevelComment"]
                author = comment["snippet"]["authorDisplayName"]
                text = comment["snippet"]["textDisplay"]
                likeCount = comment["snippet"]['likeCount']
                publishtime = comment['snippet']['publishedAt']
                comments.append([author, publishtime, likeCount, text,])

#                 if int(item['snippet']['totalReplyCount']) >0:
#                     parentID = item['id']
#                     request2 = youtube.comments().list(part="snippet",parentId= parentID,maxResults = 100)
#                     response2 = request2.execute()
#                     nextPageToken2 = ''
#                     first2 = True
#                     further2 = True   # 是否查完第一页后还往下查
#                     totalResults2 = int(len(response2['items']))
#                     while further2:
#                         halt2 = False  #是否终止
#                         if first2 == False:  #是否是循环的第一次
#                             print('..')
#                             try:
#                                 response2 = youtube.comments().list(
#                                     part="snippet",
#                                     maxResults = 100,
#                                     textFormat='plainText',
#                                     parentId = parentID,
#                                     pageToken=nextPageToken2
#                                             ).execute()
#                                 totalResults2 = int(len(response2['items']))
#                             except HttpError as e:
#                                 print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
#                                 halt2 = True

#                         if halt2 == False:
#                             for item2 in response2["items"]:
#                                 # 这只是一部分数据，你需要啥自己选就行，可以先打印下你能拿到那些数据信息，按需爬取。
#                                 author = item2["snippet"]["authorDisplayName"]
#                                 text = item2["snippet"]["textDisplay"]
#                                 likeCount = item2["snippet"]['likeCount']
#                                 publishtime = item2['snippet']['publishedAt']
#                                 comments.append([author, publishtime, likeCount, text])
#                             if totalResults2 < 100:
#                                 further2 = False     #如果这一次循环里的totalresult小于0则，不进行下一次循环，further就等于False
#                                 first2 = False
#                             else:
#                                 further2 = True
#                                 first2 = False
#                                 try:
#                                     nextPageToken2 = response2["nextPageToken"]
#                                 except KeyError as e:
#                                     print("An KeyError error occurred: %s" % (e))
#                                     further2 = False               
##############################################################################
            if totalResults < 100:
                further = False
                first = False
            else:
                further = True
                first = False
                try:
                    nextPageToken = response["nextPageToken"]
                except KeyError as e:
                    print("An KeyError error occurred: %s" % (e))
                    further = False
    print('get comment count: ', str(count))
    ### write to csv file
    data = np.array(comments)
    print('total comments and replies: ',data.shape[0])
    df = pd.DataFrame(data, columns=['author', 'publishtime', 'likeCount', 'comment',])
    return df
       
