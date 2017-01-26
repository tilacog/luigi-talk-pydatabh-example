Code used in [PyData Belo Horizonte #1](https://www.meetup.com/PyData-Belo-Horizonte/events/237074854/) event.

This example showcases a simple luigi data pipeline that downloads a book, count its words and loads the results into a sqlite3 database.

## Installation

Install requirements:

```
$ pip install -r requirements.txt
```

## Usage

You can either call the script directly:

```
python task.py LoadToDatabase --book-url="http://www.gutenberg.org/cache/epub/5200/pg5200.txt" --local-scheduler
```

... or run the luigi_cmd.sh script:

```
$ chmod +x luigi_cmd
$ luigi_cmd.sh
```
