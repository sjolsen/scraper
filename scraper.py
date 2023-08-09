import argparse
import dataclasses
import mimetypes
import os
import sqlite3
import sys
from typing import *

import bs4
import requests
from requests.structures import CaseInsensitiveDict
import urllib3.util


@dataclasses.dataclass(frozen=True, order=True)
class Uri:
    """Identifier for a resource, encoded as host and path."""
    host: str
    path: str


@dataclasses.dataclass(frozen=True, order=True)
class Url:
    """Normalized variant of urllib3 Url."""
    scheme: str
    uri: Uri
    query: Optional[str]
    fragment: Optional[str]

    @classmethod
    def normalize(cls, url: urllib3.util.Url) -> 'Url':
        """Establish desirable invariants about the URL contents."""
        assert url.host is not None
        return cls(
            scheme=url.scheme or 'http',
            uri=Uri(host=url.host, path=url.path or '/'),
            query=url.query,
            fragment=url.fragment)

    @classmethod
    def parse(cls, s: str) -> 'Url':
        return cls.normalize(urllib3.util.parse_url(s))

    @property
    def url(self) -> urllib3.util.Url:
        return urllib3.util.Url(
            scheme=self.scheme,
            host=self.uri.host,
            path=self.uri.path,
            query=self.query,
            fragment=self.fragment)

    def __str__(self) -> str:
        return str(self.url)

    def relative(self, rel: str) -> 'Url':
        """Resolve a relative URL."""
        rhs = urllib3.util.parse_url(rel)
        if not rhs.path:
            path = self.uri.path
        elif rhs.path.startswith('/'):
            path = rhs.path
        else:
            dirname = os.path.dirname(self.uri.path)
            path = os.path.join(dirname, rhs.path)
        return Url(
            scheme=rhs.scheme or self.scheme,
            uri=Uri(host=rhs.host or self.uri.host, path=path),
            query=rhs.query,
            fragment=rhs.fragment)


@dataclasses.dataclass
class Resource:
    """Cached resource resulting from an HTTP GET."""
    status: int
    headers: CaseInsensitiveDict[str]
    data: bytes


def resource_extension(resource: Resource) -> Optional[str]:
    """Deduce the appropriate file extension based on Content-Type."""
    for content_type in split_list(resource.headers['Content-Type'], ';'):
        ext = mimetypes.guess_extension(content_type)
        if ext is not None:
            return ext
    return None


@dataclasses.dataclass
class Database:
    """SQLite store for Resources."""
    path: str

    def create(self, *, recreate: bool = False):
        """Initialize the database tables."""
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

    def get(self, uri: Uri) -> Optional[Resource]:
        """Look up a resource by URI."""
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            c.execute("""
                SELECT id, status, data FROM url
                WHERE host = ? AND path = ?
            """, (uri.host, uri.path))
            url_row = c.fetchone()
            if url_row is None:
                return None
            return Resource(
                status=url_row['status'],
                headers=self._get_headers(connection, url_row['id']),
                data=url_row['data'])

    def _get_headers(self, connection: sqlite3.Connection,
                     url_id: int) -> CaseInsensitiveDict[str]:
        """Get the headers for a resource with a fresh cursor."""
        c = connection.cursor()
        header_rows = c.execute("""
            SELECT name, value FROM header
            WHERE url_id = ?
        """, (url_id,))
        return CaseInsensitiveDict({r['name']: r['value'] for r in header_rows})

    def insert(self, url: Url, resource: Resource):
        """Insert a resource into the store."""
        with sqlite3.connect(self.path) as connection:
            c = connection.cursor()
            c.execute("""
                INSERT INTO url(host, path, status, data)
                VALUES(?, ?, ?, ?)
            """, (url.uri.host, url.uri.path, resource.status,
                  resource.data))
            url_id = c.lastrowid
            for name, value in resource.headers.items():
                c.execute("""
                    INSERT INTO header(url_id, name, value)
                    VALUES(?, ?, ?)
                """, (url_id, name, value))

    def items(self) -> Iterator[Tuple[Uri, Resource]]:
        """List stored resources (Uri and Resource)."""
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            for url_row in c.execute('SELECT * FROM url'):
                uri = Uri(host=url_row['host'], path=url_row['path'])
                resource = Resource(
                    status=url_row['status'],
                    headers=self._get_headers(connection, url_row['id']),
                    data=url_row['data'])
                yield uri, resource


@dataclasses.dataclass
class HTTPCache:
    """HTTP fetch API backed by a Database."""
    database: Database

    def fetch(self, url: Url) -> Resource:
        """Return a cached resource if available, or perform a GET."""
        hit = self.database.get(url.uri)
        if hit is not None:
            return hit
        resource = self.fetch_uncached(url)
        self.database.insert(url, resource)
        return resource

    def fetch_uncached(self, url: Url) -> Resource:
        """Unconditionally perform a GET."""
        print(f'Fetching {url}')
        response = requests.get(url.url.url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0',
        })
        return Resource(
            status=response.status_code,
            headers=response.headers,
            data=response.content)


def split_list(text: str, sep: str) -> FrozenSet[str]:
    """Split on sep, trimming whitespace and discarding empty entries."""
    result = []
    for s in text.split(sep):
        s = s.strip()
        if s:
            result.append(s)
    return frozenset(result)


def extract_links(base: Url, resource: Resource) -> FrozenSet[Url]:
    """Examine a resource for links to follow."""
    content_type = split_list(resource.headers['Content-Type'], ';')
    result = []
    if 'text/html' in content_type:
        soup = bs4.BeautifulSoup(resource.data, 'html.parser')
        for link in soup.find_all('a'):
            result.append(Url.relative(base, link.get('href')))
    return frozenset(result)


@dataclasses.dataclass
class ScrapePolicy:
    """Heuristics for limiting the scope of the web scraper."""
    root: Url

    def should_scrape(self, url: Url) -> bool:
        if url.uri.host != self.root.uri.host:
            return False
        if url.scheme not in ('http', 'https'):
            return False
        return True


def write_file(path: str, data: bytes):
    """Store a file to the filesystem, ensuring the directory exists."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(data)


def cmd_create(ns: argparse.Namespace):
    """Create a new database file."""
    database = Database(ns.database)
    recreate: bool = ns.recreate
    testdata: bool = ns.testdata

    database.create(recreate=recreate)
    if testdata:
        database.insert(
            Url.parse('example.com'),
            Resource(
                status=200,
                headers=CaseInsensitiveDict({'Content-Type': 'text/plain'}),
                data=b''))


def cmd_scrape(ns: argparse.Namespace):
    """Recursively fetch available HTTP resources."""
    database = Database(ns.database)
    root = Url.parse(ns.root)

    database.create()
    cache = HTTPCache(database)
    policy = ScrapePolicy(root)
    visited: Set[Url] = set()
    unvisited = {root}

    while unvisited:
        next_unvisited: Set[Url] = set()
        for url in sorted(unvisited):
            resource = cache.fetch(url)
            for link in extract_links(root, resource):
                if policy.should_scrape(link):
                    next_unvisited.add(link)
            visited.add(url)
        unvisited = next_unvisited - visited


def cmd_export(ns: argparse.Namespace):
    """Export stored resources to the filesystem."""
    database = Database(ns.database)
    directory: str = ns.directory

    for url, resource in database.items():
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

    p_create = subparsers.add_parser('create', help=cmd_create.__doc__)
    p_create.set_defaults(command=cmd_create)
    p_create.add_argument('--recreate', action='store_true', default=False)
    p_create.add_argument('--testdata', action='store_true', default=False)

    p_create = subparsers.add_parser('scrape', help=cmd_scrape.__doc__)
    p_create.set_defaults(command=cmd_scrape)
    p_create.add_argument('root')

    p_create = subparsers.add_parser('export', help=cmd_export.__doc__)
    p_create.set_defaults(command=cmd_export)
    p_create.add_argument('directory')

    ns = parser.parse_args(args[1:])
    command = getattr(ns, 'command', cmd_help)
    command(ns)


if __name__ == '__main__':
    main(sys.argv)
