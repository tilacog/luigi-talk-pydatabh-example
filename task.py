import csv
import sqlite3
import string
from collections import Counter

import luigi
import requests


class DownloadBook(luigi.Task):
    book_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('book.txt')

    def run(self):

        request = requests.get(self.book_url)
        with self.output().open('wb') as fout:
            fout.write(request.content.decode('utf-8'))


class CountWords(luigi.Task):

    book_url = luigi.Parameter()

    def requires(self):
        return DownloadBook(book_url=self.book_url)

    def output(sellf):
        return luigi.LocalTarget('word-count.csv')

    def run(self):
        with self.input().open('r') as fin:
            book_words = [self.clean_word(i) for i in fin.read().split()]

        wordcount = Counter(book_words)

        with self.output().open('w') as fout:
            for word, count in wordcount.most_common():
                fout.write('%s,%s\n' % (word, count))

    def clean_word(self, text):
        return ''.join(i for i in text.lower() if i in string.ascii_lowercase)


class LoadToDatabase(luigi.Task):
    book_url = luigi.Parameter()

    def requires(self):
        return CountWords(book_url=self.book_url)

    def output(self):
        return luigi.LocalTarget('books-word-count.sqlite3')

    def run(self):
        with self.input().open('r') as fin:
            reader = csv.reader(fin)
            rows = list(reader)

        conn = self._create_db()
        conn.executemany(
            'insert into wordcount (word, count) values (?, ?)',
            rows
        )

        conn.commit()
        conn.close()

    def _create_db(self):
        conn = sqlite3.connect(self.output().path)

        # create table
        conn.execute('create table wordcount (word, count);')

        return conn


if __name__ == '__main__':
    luigi.run()
