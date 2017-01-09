"""SQLite log handler.

This module is designed to be used a log handler for the logging module in the standard library. It write log
message to a SQLite file.

Note:
    I've only tested this in Python 3.5. It might or might not work in eariler version of Python.


Example:

    import logging
    from sqliteLogHandler import SQLiteLogger

    logger = logging.getLogger("my_logger")


    # Give it a file name, In this case, I'm using the name, log.sqlite.
    sql_handler = SQLiteLogger("log.sqlite")
    
    # Add handler to logger.
    logger.addHandler(sql_handler)

    # Get the minimum threshold for what you want to capture. DEBUG will capture it all
    logger.setLevel(logging.DEBUG)

    logger.debug("My debug message for me.")
    logger.info("My info level message for the user.")
    logger.critical("Oh No! I died")
    # etc...

"""
import logging
import sqlite3
import sys


# This command builds the SQLite tables if they don't exist already
CREATE_COMMAND = """CREATE TABLE IF NOT EXISTS
                    LogLevels (LevelName TEXT UNIQUE , Value INT UNIQUE );
                   CREATE TABLE IF NOT EXISTS
                    Logs(Name TEXT, Level INT, Pathname TEXT, Msg TEXT, Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
                  """


class SQLiteLogger(logging.Handler):
    """For generating logs in a SQLite file"""
    def __init__(self, filename, level=logging.NOTSET):

        super().__init__(level)

        self.filename = filename
        self._db = sqlite3.connect(self.filename, isolation_level=None)
        self._db.executescript(CREATE_COMMAND)

        try:
            # try to get the logging names as strings with their numerical value associated with it. But it doesn't
            # raise an exception if it can't.

            # noinspection PyProtectedMember
            levels = logging._nameToLevel
            for name, value in levels.items():
                self._db.execute("INSERT OR IGNORE INTO LogLevels(LevelName, Value) VALUES (?, ?)", (name, value))
        except AttributeError:
            print("Unable to get log level names", file=sys.stderr)
            pass

    def emit(self, record):
        """
        Generates a new log into the SQLite file
        """
        self.acquire()
        try:
            cur = self._db.cursor()
            cur.execute(
                "INSERT INTO Logs(Name, Level, Pathname, Msg) VALUES (?, ?, ?, ?)",
                (record.name, record.levelno, record.pathname, record.msg))
        finally:
            self.release()

    def flush(self):
        self.acquire()
        try:
            self._db.commit()
        finally:
            self.release()

    def close(self):
        try:
            self.acquire()
            self._db.commit()
            self._db.close()
        finally:
            self.release()
