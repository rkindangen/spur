
from itertools import count
import pymysql.cursors
import time
import queue
from multiprocessing import Queue, Process
from time import sleep
import pandas as pd
import pickle
from collections import Counter

# count occurences of a word in a string
def countOccurrences(string, word):
    # split the string by spaces in a
    a = str(string).split(" ")

    # search for pattern in a
    count = 0
    for i in range(0, len(a)):

        # if match found increase count
        if (word == a[i]):
            count = count + 1
            
    return count      

# Count occurences of top words of a topic in a review
def countTopWords(topics, topic, review):
    if(review):
        topwords = topics[topic]

        total = 0
        counter = Counter(str(review))
        for word in topwords:
            total += counter[word]
    else:
        total = None
    return total


def writeCountResult(countList, cursor):
    columns = ('ReviewID', 'Location', 'Noisy' , 'ScenicSpot' , 'Transportation' , 'RoomSize' , 'Smell' , 'RecreationFacilities' , 'Amenities' , 'Bathroom' , 'InRoomFacilities' , 'Renovation' , 'View' , 'Cleanliness' )
    sql = "INSERT INTO Airbnb VALUES {};".format(tuple(countList))
    cursor.execute(sql)
 


# Count top words of all topics each of the reviews and write the result to SQL
def writeToSQL(topics, reviewdict, task_to_accomplish):
    while True:    
        try:
            # Get the next review in queue
            innerindex = task_to_accomplish.get_nowait()
        except queue.Empty:
            # Stop if there is no more review left in queue
            break
        else:
            # count words in the review
            review = reviewdict[innerindex]
            countList = list()
            countList.append(innerindex)
            for topic in topics.keys():
                countList.append(countTopWords(topics, topic, review))
            # write the result to the SQL
            while True:
                try:
                    # Connect to the database
                    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='Rach17956.',
                             database='airbnb_reviews',
                             cursorclass=pymysql.cursors.DictCursor)
                    cursor = connection.cursor()
                    break
                except pymysql.OperationalError as e:
                    print(e)
                    time.sleep(1)
    
            try:
                writeCountResult(countList, cursor)
                connection.commit()
            except AttributeError:
                print('Writing Failed AttributeError')
            except TypeError:
                print('Writing Failed TypeError')
            except Exception as e:
                print(e)
            connection.close()
    return(True)


if __name__ == '__main__':
    ## Read review data and topic dictionary
    data = pd.read_csv("spur_review.csv")
    reviews = data.set_index('idx')['review'].to_dict()
    with open('topics.pickle', 'rb') as handle:
        topics = pickle.load(handle)
    ## time the process
    start_time=time.time()

    ## Use 4 cpu cores
    total_processes=4

    task_to_accomplish=Queue()
    processes=[]

    ## Put all review indices in queue
    for i in reviews.keys():
        task_to_accomplish.put(i)

    ## Assign task to processors
    for core in range(total_processes):
        writetosql = Process(target=writeToSQL, args=(topics, reviews, task_to_accomplish))
        processes.append(writetosql)
        writetosql.start()
    for p in processes:
        p.join()
    ## Print how much time it took to complete
    print("It took %s seconds" % (time.time()-start_time))
