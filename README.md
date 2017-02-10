This example showcases a simple luigi data pipeline that downloads a book, count its words and loads the results into a sqlite3 database.

## Installation

Install requirements:

```
$ pip install -r requirements.txt
```

## Usage

Example:

```
$ python book_word_count.py LoadToDatabase --book-url="http://www.gutenberg.org/cache/epub/5200/pg5200.txt" --local-scheduler
```
