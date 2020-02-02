# Author: Po-Chuan (Gary) Tseng
import sys
import re
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.types import IntegerType
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession

def readAllChap(x):
	list1=[]
	for each in x[1]:
		each_list1 = re.split("(?<=[XVI])\n\n", each)
		each_list1=[x[0]]+each_list1
		list1.append(tuple(each_list1))
	return list1


def readAllPara(x):
	list2=[]
	for index,each in enumerate(x[2]):
		each_list2 = re.split("\n\n", each)
		each_list2=[x[0]]+[x[1]]+['PARAGRAPH '+str(index+1)]+each_list2
		list2.append(tuple(each_list2))
	return list2


def readAllSentence(x):
	list3=[]
	for index,each in enumerate(x[3]):
		each_list3=[x[0]]+[x[1]]+[x[2]]+['SENTENCE '+str(index+1)]+[each]
		list3.append(tuple(each_list3))
	return list3


def readAllWord(x):
	list4=[]
	for index,each in enumerate(x[5]):
		each_list4=[x[0]]+[x[1]]+[x[2]]+[x[3]]+[x[4]]+['WORD '+str(index+1)]+[each]
		list4.append(tuple(each_list4))
	return list4

conf = SparkConf().setMaster("local").setAppName("LargeBodySemiText")
sc = SparkContext(conf = conf)
spark=SparkSession(sc)

filename = sys.argv[1]
with open(filename) as f:
    data = f.read()

rawDataDataFrame = spark.createDataFrame([
    (0, data)    
], ["id", "rawData"])

rawDataregexTokenizer = RegexTokenizer(inputCol="rawData", outputCol="bodyText", toLowercase=False, pattern="\n\n\n\n\n\n\n\n\n\n\n")
rawDataregexTokenized = rawDataregexTokenizer.transform(rawDataDataFrame)
rawData1 = rawDataregexTokenized.rdd.map(list).collect()

# Parsing body from the raw text 
BodyDataFrame = spark.createDataFrame([
    (0, rawData1[0][2][1])
], ["id", "body"])

# Tokenizing the body dataframe
BodyTokenizer = RegexTokenizer(inputCol="body", outputCol="book", toLowercase=False, pattern="\n\n\n\n\n\n(?!.*CHAPTER)(?!\n)")
BodyTokenized = BodyTokenizer.transform(BodyDataFrame)

# Parsing books from the body
books = BodyTokenized.rdd.map(lambda x: x[2]).collect()
books_list = []
for each in books[0]:
	each_list = re.split("\n\n\n\n\n\n(?=CHAPTER\sI\n)",each)
	books_list.append(tuple(each_list))

# Creating the book dataframe
BookDataFrame = spark.createDataFrame(books_list,["bookIndex","bookContent"])

# Tokenizing the book dataframe
BookTokenizer = RegexTokenizer(inputCol="bookContent", outputCol="chap", toLowercase=False, pattern="\n{5,6}(?=.*CHAPTER)")
BookTokenized = BookTokenizer.transform(BookDataFrame)

# Parsing chapters from the book
chaps = BookTokenized.rdd.map(lambda x: (x[0],x[2])).flatMap(lambda x: readAllChap(x)).collect()

# Creating the chapter dataframe
ChapDataFrame = spark.createDataFrame(chaps,["bookIndex","chapIndex","chapContent"])

# Tokenizing the chapter dataframe
ChapTokenizer = RegexTokenizer(inputCol="chapContent", outputCol="paragraph", toLowercase=False, pattern="\n\n")
ChapTokenized = ChapTokenizer.transform(ChapDataFrame)

# Parsing paragraphs from the chapter
paragraphs= ChapTokenized.rdd.map(lambda x: (x[0],x[1],x[3])).flatMap(lambda x: readAllPara(x)).collect()

# Creating the paragraph dataframe
ParagraphDataFrame = spark.createDataFrame(paragraphs,["bookIndex","chapIndex","paragraphIndex", "paragraphContent"])

# Tokenizing the paragraph dataframe
ParagraphTokenizer = RegexTokenizer(inputCol="paragraphContent", outputCol="sentence", toLowercase=False, pattern="(?<=[a-zA-z])\\.\\s|\\.\\”\\s(?=[A-Z\\(])|\\.\\)\\s|\\!\\s|\\!\\”\\s(?=[A-Z\\(])|\\!\\)\\s|\\?\\s|\\?\\”\\s(?=[A-Z\\(])|\\?\\)\\s")
ParagraphTokenized = ParagraphTokenizer.transform(ParagraphDataFrame)

# Parsing sentences from the paragraph
sentences= ParagraphTokenized.rdd.map(lambda x: (x[0],x[1],x[2],x[4])).flatMap(lambda x: readAllSentence(x)).collect()

# Creating the sentence dataframe
SentenceDataFrame = spark.createDataFrame(sentences,["bookIndex","chapIndex","paragraphIndex","sentenceIndex","sentenceContent"])

# Tokenizing the sentence dataframe
SentenceTokenizer = RegexTokenizer(inputCol="sentenceContent", outputCol="word", toLowercase=False, pattern="\\s")
SentenceTokenized = SentenceTokenizer.transform(SentenceDataFrame)

# Parsing words from the sentence
words= SentenceTokenized.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5])).flatMap(lambda x: readAllWord(x)).collect()

# Creating final dataframe
FinalDataFrame = spark.createDataFrame(words,["bookIndex","chapIndex","paragraphIndex","sentenceIndex","sentenceContent","wordIndex","wordContent"])
FinalDataFrame.show()
FinalDataFrame.coalesce(1).write.csv(sys.argv[2])