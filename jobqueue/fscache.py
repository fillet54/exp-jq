"""Filesystem Cache

This library provides the functions and objects required to save filesystem trees
in a cache. The file systems are stored in git loose object store format.

Since this libary is intended to work on filesystem that can have large files,
this library attempts to use streams when possible to avoid requiring to load
files fully into memory.
"""

import io, os, hashlib, zlib
from collections import namedtuple
from contextlib import contextmanager
from itertools import chain

class GitObject(object):
    def __init__(self, objtype, length, data):
        self.type = objtype
        self.length = length
        self.data = data 

    def __len__(self):
        return self.length

    def __iter__(self):
        return self.data

def write_compressed(streams, fout, block_size=4096):
    '''Compresses streams and writes to output'''
    zlib_obj = zlib.compressobj()
    for stream in streams:
        while True:
            chunk = stream.read(block_size)
            if not chunk:
                break
            compressed_bytes = zlib_obj.compress(chunk)
            if compressed_bytes:
                fout.write(compressed_bytes)
    
    # Flush out remaining compressed bytes
    compressed_bytes = zlib_obj.flush()
    if compressed_bytes:
        fout.write(compressed_bytes)

def read_compressed(fin, block_size=4096):
    '''Generator for reading a zlib compressed file'''
    zlib_obj = zlib.decompressobj()
    while True:
        chunk = fin.read(block_size)
        if not chunk:
            break
        decompressed_chunk = zlib_obj.decompress(chunk)
        if decompressed_chunk:
            yield decompressed_chunk

    decompressed_chunk = zlib_obj.flush()
    if decompressed_chunk:
        yield decompressed_chunk

def compute_sha1_hash(*streams, block_size=4096):
    '''Computes sha1 hash of the concatinated streams'''
    sha = hashlib.sha1()
    for stream in streams:
        while True:
            data = stream.read(block_size)
            if not data:
                break    
            sha.update(data)
    return sha.hexdigest()

def object_header(objtype, length):
    '''Constructs a git object store object header'''
    return (objtype + '\x20' + str(length) + '\x00').encode()
            

def stream_length(stream):
    '''Helper to determine remaining stream length'''
    cur = stream.tell()
    stream.seek(0, os.SEEK_END)
    length = stream.tell() - cur
    stream.seek(cur)
    return length

@contextmanager
def open_stream(stream_or_path, mode='rb'):
    '''Helper method to open a stream or return the passed in stream'''
    close = False
    stream = stream_or_path
    try:
        if not isinstance(stream, io.IOBase):
            stream = open(stream_or_path, mode)
            close = True
        yield stream
    finally:
        if close:
            stream.close()

total_write_length = 0

OBJECT_TYPES = ['blob', 'tree', 'commit']
def write_object(repo, objtype, data):
    '''Writes a git object to a git object store

    returns sha, size'''
    assert objtype in OBJECT_TYPES, "objtype must be one of %r" % OBJECT_TYPES

    with open_stream(data, 'rb') as stream:
        length = stream_length(stream)
        header = io.BytesIO(object_header(objtype, length))
        sha = compute_sha1_hash(header, stream)
        dir_path = os.path.join(repo, 'objects', sha[:2])
        path = os.path.join(dir_path, sha[2:])

        # No need to write if file already exists
        if os.path.exists(path):
            return sha, length

        global total_write_length
        total_write_length += length

        os.makedirs(dir_path, exist_ok=True)

        header.seek(0)
        stream.seek(0)
        with open(path, 'wb') as fout:
            write_compressed([header, stream], fout)

        return sha, length

@contextmanager
def read_object(repo, sha):
    path = os.path.join(repo, 'objects', sha[:2], sha[2:])
    f = None
    try:
        f = open(path, 'rb')
        header_chunks = []
        chunks = read_compressed(f)

        while True:
            chunk = next(chunks)

            # Found end of header
            end_of_header = chunk.find(b'\x00')
            if end_of_header >= 0:
                header_chunks.append(chunk[:end_of_header+1])
                remaining = chunk[end_of_header+1:]

                header = b''.join(header_chunks).decode()
                parts = header.split()
                objtype = parts[0]
                length = int(parts[1][:-1])

                yield GitObject(objtype, length, chain([remaining], chunks))
                break
            else:
                header_chunks.append(chunk)
    finally:
        if f:
            f.close()

import math

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

def write_tree(repo, rootdir, paths):
    '''Writes a file system tree and associated objects'''
    
    rootdir = os.path.abspath(root).replace('\\' ,'/')
    if not rootdir.endswith('/'):
        rootdir += '/'
    
    # convert to absolute and normalized paths
    paths = [os.path.join(rootdir, path).replace('\\', '/') 
             for path in paths]
    
    assert all(path.startswith(rootdir) in paths), "All paths must be within root"

    tree = []
    for path in paths:
        rel_path = path[len(rootdir):]
        
        stat = os.stat(path)
        mode = oct(stat.st_mode)[2:]
        sha, length = write_object(repo, 'blob', path)
        tree.append((mode, sha, rel_path))
        
    # format tree file
    entries = [f"'{mode}' '{sha}' '{rel_path}'"
               for mode, sha, rel_path in tree]
    stream = io.BytesIO('\n'.join(entries))
    sha, length = write_object(repo, 'tree', stream)
    
    return sha

def ensure_refs(repo, namespace):
    refs_path = os.path.join(repo, 'refs', namespace)
    
    if os.exists(refs_path):
        return
    
    os.makedirs()
    
    
def commit_tree(repo, ref, tree_sha, namespace='heads'):
    
    ensure_refs(repo)
    

def restore_tree(repo, sha):
    pass

if __name__ == '__main__':

    # Simple example of writing a blob and reading it back

    repo = './repo'
    obj_data = io.BytesIO("SOME DATA NEW DATA".encode())
    # obj_data = open('path/to/some/file', 'rb')

    
    # Write the object to the repo and get the objects sha
    sha, _ = write_object('./repo', 'blob', obj_data)

    # Read object
    with read_object(repo, sha) as obj:
        print('TYPE:', "'%s'" % obj.type)
        print('LENGTH:', '%d bytes' % obj.length)
        print('DATA:', "'%s'" % b''.join([chunk for chunk in obj]).decode())
