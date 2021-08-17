import queue
import uuid
from typing import List, Tuple, Dict

from clickhouse_driver import connect
from dejavu.base_classes.common_database import CommonDatabase
from dejavu.config.settings import (FIELD_FILE_SHA1, FIELD_FINGERPRINTED,
                                    FIELD_HASH, FIELD_OFFSET, FIELD_SONG_ID,
                                    FIELD_SONGNAME, FIELD_TOTAL_HASHES,
                                    FINGERPRINTS_TABLENAME, SONGS_TABLENAME)


class ClickhouseDatabase(CommonDatabase):
    type = "clickhouse"

    CREATE_SONGS_TABLE = f'''
        CREATE TABLE IF NOT EXISTS {SONGS_TABLENAME}(
            {FIELD_SONG_ID} UUID,
            {FIELD_SONGNAME} String,
            {FIELD_FINGERPRINTED} Int8 DEFAULT 1,
            {FIELD_FILE_SHA1} String,
            `date_created` DateTime DEFAULT now(),
            {FIELD_TOTAL_HASHES} Int32
            
        ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date_created)
            ORDER BY tuple()
    '''

    CREATE_FINGERPRINTS_TABLE = f'''
        CREATE TABLE IF NOT EXISTS {FINGERPRINTS_TABLENAME} (
            {FIELD_HASH} String,
            {FIELD_SONG_ID} UUID,
            {FIELD_OFFSET} Int32,
            `date_created` DateTime DEFAULT now()
        ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date_created)
            ORDER BY (`date_created`, {FIELD_SONG_ID})            
    '''

    INSERT_FINGERPRINT = f'''
        INSERT INTO {FINGERPRINTS_TABLENAME} (
            {FIELD_SONG_ID},
            {FIELD_HASH},
            {FIELD_OFFSET}
        ) VALUES
    '''

    INSERT_SONG = f'''
        INSERT INTO {SONGS_TABLENAME} (
            {FIELD_SONG_ID},
            {FIELD_SONGNAME}, 
            {FIELD_FILE_SHA1},
            {FIELD_TOTAL_HASHES}
        )
        VALUES (%(0)s, %(1)s, %(2)s, %(3)s)
    '''

    SELECT = f'''
        SELECT {FIELD_SONG_ID}, {FIELD_OFFSET}
        FROM {FINGERPRINTS_TABLENAME}
        WHERE {FIELD_HASH} = (%(0)s)
    '''

    SELECT_MULTIPLE = f'''
        SELECT upper({FIELD_HASH}), {FIELD_SONG_ID}, {FIELD_OFFSET}
        FROM {FINGERPRINTS_TABLENAME}
        WHERE upper({FIELD_HASH}) IN (%(0)s)
    '''

    SELECT_ALL = f'SELECT {FIELD_SONG_ID}, {FIELD_OFFSET} FROM {FINGERPRINTS_TABLENAME}'

    SELECT_SONG = f'''
        SELECT
            {FIELD_SONGNAME},
            upper({FIELD_FILE_SHA1}) AS {FIELD_FILE_SHA1},
            {FIELD_TOTAL_HASHES}
        FROM {SONGS_TABLENAME}
        WHERE {FIELD_SONG_ID} = toUUID(%(0)s)
    '''

    SELECT_NUM_FINGERPRINTS = f'SELECT count() AS n FROM {FINGERPRINTS_TABLENAME}'

    SELECT_UNIQUE_SONG_IDS = f'''
        SELECT count({FIELD_SONG_ID}) AS n
        FROM {SONGS_TABLENAME}
        WHERE {FIELD_FINGERPRINTED} = 1;
    '''

    SELECT_SONGS = f'''
        SELECT
            {FIELD_SONG_ID},
            {FIELD_SONGNAME},
            upper({FIELD_FILE_SHA1}) AS {FIELD_FILE_SHA1},
            {FIELD_TOTAL_HASHES},
            date_created
        FROM {SONGS_TABLENAME}
        WHERE {FIELD_FINGERPRINTED} = 1;
    '''

    IN_MATCH = f'%s'

    DROP_FINGERPRINTS = f'DROP TABLE IF EXISTS {FINGERPRINTS_TABLENAME}'

    DROP_SONGS = f'DROP TABLE IF EXISTS {SONGS_TABLENAME}'

    DELETE_UNFINGERPRINTED = 'SELECT now()'

    def __init__(self, **options):
        super().__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self) -> None:
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def insert_song(self, song_name: str, file_hash: str, total_hashes: int) -> str:
        """
        Inserts a song name into the database, returns the new
        identifier of the song.

        :param song_name: The name of the song.
        :param file_hash: Hash from the fingerprinted file.
        :param total_hashes: amount of hashes to be inserted on fingerprint table.
        :return: the inserted id.
        """
        song_id = str(uuid.uuid4())
        with self.cursor() as cur:
            params = [song_id, song_name, file_hash, total_hashes]
            keys = [str(x) for x in range(len(params))]
            cur.execute(self.INSERT_SONG, dict(zip(keys, params)))
            # cur.execute(self.INSERT_SONG, [song_id, song_name, file_hash, total_hashes])
            return song_id

    def __getstate__(self):
        return self._options,

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)

    def delete_unfingerprinted_songs(self) -> None:
        raise NotSupportedException('Not supported')

    def delete_songs_by_id(self, song_ids: List[int], batch_size: int = 1000) -> None:
        raise NotSupportedException('Not supported')

    def set_song_fingerprinted(self, song_id):
        raise NotSupportedException('Not supported')

    def insert_hashes(self, song_id: int, hashes: List[Tuple[str, int]], batch_size: int = 10000) -> None:
        super().insert_hashes(song_id, hashes, batch_size)

    def return_matches(self, hashes: List[Tuple[str, int]], batch_size: int = 5000) -> Tuple[
        List[Tuple[int, int]], Dict[int, int]]:
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hsh, offset in hashes:
            if hsh.upper() in mapper.keys():
                mapper[hsh.upper()].append(offset)
            else:
                mapper[hsh.upper()] = [offset]

        values = list(mapper.keys())

        # in order to count each hash only once per db offset we use the dic below
        dedup_hashes = {}

        results = []
        with self.cursor() as cur:
            for index in range(0, len(values), batch_size):
                # Create our IN part of the query
                cur.execute(self.SELECT_MULTIPLE, {"0": values[index: index + batch_size]})

                for hsh, sid, offset in cur:
                    if sid not in dedup_hashes.keys():
                        dedup_hashes[sid] = 1
                    else:
                        dedup_hashes[sid] += 1
                    #  we now evaluate all offset for each  hash matched
                    for song_sampled_offset in mapper[hsh]:
                        results.append((sid, offset - song_sampled_offset))

            return results, dedup_hashes

    def get_song_by_id(self, song_id: int) -> Dict[str, str]:
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_SONG, {"0": song_id})
            res = cur.fetchone()
            return {
                f'{FIELD_SONGNAME}': res[0],
                f'{FIELD_FILE_SHA1}': res[1],
                f'{FIELD_TOTAL_HASHES}': res[2]
            }


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)

    return cursor


class NotSupportedException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
        ...
    """

    def __init__(self, dictionary=False, **options):
        super().__init__()

        self._cache = queue.Queue(maxsize=5)

        try:
            conn = self._cache.get_nowait()
            # Ping the connection before using it from the cache.
            conn.ping(True)
        except queue.Empty:
            conn = connect(**options)

        self.conn = conn
        self.dictionary = dictionary

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        if self.dictionary:
            self.cursor = self.conn.cursor()
        else:
            self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a PostgreSQL related error we try to rollback the cursor.

        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()
