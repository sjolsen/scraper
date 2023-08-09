import argparse
import dataclasses
import mimetypes
import os
import sqlite3
import sys
from typing import *

import bs4
import requests
import urllib3.util
from urllib3.util import Url


@dataclasses.dataclass
class Resource:
    status: int
    headers: Dict[str, str]
    data: bytes


def resource_extension(resource: Resource) -> Optional[str]:
    for content_type in split_list(resource.headers['Content-Type'], ';'):
        ext = mimetypes.guess_extension(content_type)
        if ext is not None:
            return ext
    return None


@dataclasses.dataclass
class Database:
    path: str

    def create(self, *, recreate: bool = False):
        if os.path.exists(self.path):
            if recreate:
                os.unlink(self.path)
            else:
                return
        with sqlite3.connect(self.path) as connection:
            c = connection.cursor()
            c.executescript("""
                CREATE TABLE url(
                    id INTEGER NOT NULL PRIMARY KEY,
                    host TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    data BLOB NOT NULL
                );
                CREATE TABLE header(
                    id INTEGER NOT NULL PRIMARY KEY,
                    url_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    value TEXT NOT NULL,
                    FOREIGN KEY(url_id) REFERENCES url(id)
                );
            """)

    def get(self, url: Url) -> Optional[Resource]:
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            c.execute("""
                SELECT id, status, data FROM url
                WHERE host = ? AND path = ?
            """, (url.host, url.path))
            url_row = c.fetchone()
            if url_row is None:
                return None
            c.execute("""
                SELECT name, value FROM header
                WHERE url_id = ?
            """, (url_row['id'],))
            header_rows = c.fetchall()
        return Resource(
            status=url_row['status'],
            headers={r['name']: r['value'] for r in header_rows},
            data=url_row['data'])

    def insert(self, url: Url, resource: Resource):
        with sqlite3.connect(self.path) as connection:
            c = connection.cursor()
            c.execute("""
                INSERT INTO url(host, path, status, data)
                VALUES(?, ?, ?, ?)
            """, (url.host, url.path, resource.status, resource.data))
            url_id = c.lastrowid
            for name, value in resource.headers.items():
                c.execute("""
                    INSERT INTO header(url_id, name, value)
                    VALUES(?, ?, ?)
                """, (url_id, name, value))

    def keys(self) -> List[Url]:
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            c.execute('SELECT host, path FROM url')
            return [normalize_url(Url(host=r['host'], path=r['path']))
                    for r in c.fetchall()]


@dataclasses.dataclass
class HTTPCache:
    database: Database

    def fetch(self, url: Url) -> Resource:
        hit = self.database.get(url)
        if hit is not None:
            return hit
        response = self.fetch_uncached(url)
        resource = Resource(
            status=response.status_code,
            headers=response.headers,
            data=response.content)
        self.database.insert(url, resource)
        return resource

    def fetch_uncached(self, url: Url) -> Resource:
        print(f'Fetching {url}')
        return requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0',
        })


def split_list(text: str, sep: str) -> FrozenSet[str]:
    result = []
    for s in text.split(sep):
        s = s.strip()
        if s:
            result.append(s)
    return frozenset(result)


def normalize_url(url: Url) -> Url:
    defaults = {'scheme': 'http', 'path': '/'}
    return Url(**{
        field: getattr(url, field) or defaults.get(field)
        for field in url._fields
    })


def resolve_url(base: Url, rel: Url) -> Url:
    if not rel.path:
        path = base.path
    elif rel.path.startswith('/'):
        path = rel.path
    else:
        dirname = os.path.dirname(base.path)
        path = os.path.join(dirname, rel.path)
    result = Url(
        scheme=rel.scheme or base.scheme,
        host=rel.host or base.host,
        path=path,
        query=rel.query,
        fragment=rel.fragment)
    return normalize_url(result)


def extract_links(base: Url, resource: Resource) -> FrozenSet[Url]:
    content_type = split_list(resource.headers['Content-Type'], ';')
    if 'text/html' not in content_type:
        return []
    result = []
    soup = bs4.BeautifulSoup(resource.data, 'html.parser')
    for link in soup.find_all('a'):
        rel = urllib3.util.parse_url(link.get('href'))
        result.append(resolve_url(base, rel))
    return frozenset(result)


@dataclasses.dataclass
class ScrapePolicy:
    root: Url

    def should_scrape(self, url: Url) -> bool:
        if url.host != self.root.host:
            return False
        if url.scheme not in ('http', 'https'):
            return False
        return True
    

def write_file(path: str, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(data)


def cmd_create(ns: argparse.Namespace):
    database = Database(ns.database)
    recreate: bool = ns.recreate
    testdata: bool = ns.testdata

    database.create(recreate=recreate)
    if testdata:
        database.insert(
            urllib3.util.parse_url('example.com'),
            Resource(
                status=200,
                headers={'Content-Type': 'text/plain'},
                data=b''))


def cmd_scrape(ns: argparse.Namespace):
    database = Database(ns.database)
    root = normalize_url(urllib3.util.parse_url(ns.root))

    database.create()
    cache = HTTPCache(database)
    policy = ScrapePolicy(root)
    visited = set()
    unvisited = set((root,))

    while unvisited:
        next_unvisited = set()
        for url in unvisited:
            resource = cache.fetch(url)
            for link in extract_links(root, resource):
                if policy.should_scrape(link):
                    next_unvisited.add(link)
            visited.add(url)
        unvisited = sorted(next_unvisited - visited)


def cmd_export(ns: argparse.Namespace):
    database = Database(ns.database)
    directory: str = ns.directory

    for url in database.keys():
        resource = database.get(url)
        path = url.path
        if path.endswith('/'):
            ext = resource_extension(resource) or ''
            path = f'{path}__resource__{ext}'
        path = path.lstrip('/')
        dst = os.path.join(directory, url.host, path)
        write_file(dst, resource.data)


def main(args: Sequence[str]):
    parser = argparse.ArgumentParser(prog='scraper.py')
    parser.add_argument('--database', default='scraper.db')
    subparsers = parser.add_subparsers()

    def cmd_help(_):
        parser.print_help()

    p_help = subparsers.add_parser('help')
    p_help.set_defaults(command=cmd_help)

    p_create = subparsers.add_parser('create')
    p_create.set_defaults(command=cmd_create)
    p_create.add_argument('--recreate', action='store_true', default=False)
    p_create.add_argument('--testdata', action='store_true', default=False)

    p_create = subparsers.add_parser('scrape')
    p_create.set_defaults(command=cmd_scrape)
    p_create.add_argument('root')

    p_create = subparsers.add_parser('export')
    p_create.set_defaults(command=cmd_export)
    p_create.add_argument('directory', default='output')

    ns = parser.parse_args(args[1:])
    command = getattr(ns, 'command', cmd_help)
    command(ns)


if __name__ == '__main__':
    main(sys.argv)
