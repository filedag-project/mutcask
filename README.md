# Mutcask

Store data chunk in kv style for files and objects.


## data storing process

Mutcask has one active write process accepting key-value data. Received data chunk will be append to a log file which has a max size setting by configs. The size of the log files may not as exactly as size in setting, it may be bigger. Once a log file reached the size limit, it sealed, and a new log file will be create to accepting chunks.

As data only be appended to log files, there no rewrite to log files. So we can accept multiple read to one log file.
