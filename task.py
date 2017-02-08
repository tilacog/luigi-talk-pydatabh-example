import base64
import csv
import sqlite3
import string
from collections import Counter

import luigi
import requests


def hash_filename(text):
    as_bytes = text.encode('utf-8')
    hashed = base64.b64encode(as_bytes)
    as_string = hashed.decode('ascii')
    return as_string


class DownloadBook(luigi.Task):
    book_url = luigi.Parameter()

    def output(self):

        # suboptimal way to use url as a filename
        encoded_name = hash_filename(self.book_url) + '.txt'

        return luigi.LocalTarget(encoded_name)

    def run(self):

        request = requests.get(self.book_url)
        with self.output().open('wb') as fout:
            fout.write(request.content.decode('utf-8'))


class CountWords(luigi.Task):

    book_url = luigi.Parameter()

    def requires(self):
        return DownloadBook(book_url=self.book_url)

    def output(self):
        # suboptimal way to use url as a filename
        encoded_name = hash_filename(self.book_url) + '-wc.txt'
        return luigi.LocalTarget(encoded_name)

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
        return SqliteRowTarget(
            'db.sqlite3',
            table='wordcount',
            column='book_url',
            value=self.book_url,
        )

    def run(self):
        with self.input().open('r') as fin:
            reader = csv.reader(fin)
            rows = [(self.book_url, i, j) for (i, j) in list(reader)]

        conn = self._create_db()
        conn.executemany(
            'insert into wordcount (book_url, word, count) values (?, ?, ?)',
            rows
        )

        conn.commit()
        conn.close()

    def _create_db(self):
        conn = sqlite3.connect(self.output().path)

        # create table
        conn.execute('create table wordcount (book_url, word, count);')

        return conn


class SqliteRowTarget(luigi.Target):
    """
    Simple target for Sqlite3 databases.
    """
    def __init__(self, path, table, column, value):

        self.path = path
        self.table = table
        self.column = column
        self.value = value

    def exists(self):
        """
        Checks if a given record exists by trying to select it.
        """

        #  connect to database
        try:
            conn = sqlite3.connect(self.path)
        except:
            return False

        # query template
        q = """
            select {col} from {table}
            where {col} = ?
            limit 1;
        """.format(col=self.column, table=self.table)

        # fetch records
        try:
            cur = conn.cursor()
            cur.execute(q, (self.value,))

            if not cur.fetchone():
                return False

        except sqlite3.OperationalError:
            return False

        return True


if __name__ == '__main__':
    luigi.run()
