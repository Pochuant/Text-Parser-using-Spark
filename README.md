## Text-Parser-using-Spark
***Large body of semi structured text parser***

Write a scanner and parser that can separate the body of the book content into hierarchical structure.
- Detect books, chapters, paragraphs
- Structure the text into the following nested format

Structure:
    
    Book number and year
        -> Chapter Index
            -> Paragraph Index
                -> Sentence Index          
                    -> Sentence Text
                    -> Word Index
                        -> Word 
                  
## My approach : 
Using [Spark Dataframe](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html) as container to serialize the large body of text. [RegexToknizer](https://spark.apache.org/docs/latest/ml-features.html#tokenizer) are applied to detect the structure.


Final Dataframe will be looked like: 

| bookIndex     | chapIndex       | paragraphIndex  | sentenceIndex  | sentenceContent  | wordIndex  | wordContent  |
| ------------- |:---------------:| :--------------:| :-------------:| :---------------:| :---------:|-------------:|
| BOOK ONE: 1805| CHAPTER I       | PARAGRAPH 1     | SENTENCE 1     | "Well, Prince,...| WORD 1     | "Well,       |
| BOOK ONE: 1805| CHAPTER I       | PARAGRAPH 1     | SENTENCE 1     | "Well, Prince,...| WORD 2     | Prince,      |
| BOOK ONE: 1805| CHAPTER I       | PARAGRAPH 1     | SENTENCE 1     | "Well, Prince,...| WORD 3     | so           |


@ Code can be referred to parser.py

@ Text template use [Tolstoy's War and Peace](http://www.gutenberg.org/ebooks/2600)


