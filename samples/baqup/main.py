#!/usr/bin/python
#
# Copyright 2014 Quip
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Backs up a Quip account to a local folder.

This is a sample app for the Quip API - https://quip.com/api/.
"""

import argparse
import datetime
import logging
import os.path
import re
import shutil
import sys
import urllib2
import xml.etree.cElementTree
import xml.sax.saxutils
import json

reload(sys)
sys.setdefaultencoding('utf8')

import quip

_BASE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
_STATIC_DIRECTORY = os.path.abspath(os.path.join(_BASE_DIRECTORY, 'static'))
_TEMPLATE_DIRECTORY = os.path.abspath(
    os.path.join(_BASE_DIRECTORY, 'templates'))
_OUTPUT_STATIC_DIRECTORY_NAME = '_static'
_MAXIMUM_TITLE_LENGTH = 64

class HeadersDict(dict):
    """Half-assed substitution for the `httplib.HTTPMessage` (I think?) that
    the object returned by `urllib2.urlopen().info()`. The important part is
    it's dict-like and treats keys as case-insensitive during reads.

    `httplib.HTTPMessage` has no API documentation and says "It is not directly
    instantiated by the users." [1], so us users now got this. It's used for
    the `Blob#headers` property and returned from `Blob#info()`.

    [1]: https://docs.python.org/2/library/httplib.html#httplib.HTTPMessage
    """
    
    def __getitem__(self, key):
        return super(HeadersDict, self).__getitem__(key.lower())

    def get(self, key, *args, **kwds):
        return super(HeadersDict, self).get(key.lower(), *args, **kwds)

class Blob(object):
    """
    What `BaqupClient#get_blob()` returns instead of the "file-like" 
    whatever [1] that `quip.QuipClient#get_blob()` does.
    
    This change was made to facilitate caching downloaded blobs, because it
    was painfully unclear how to instantiate the classes `urllib2` uses for
    ourselves, even after reading the source.
    
    It makes some attempt to mimic the API of the "file-like" object from 
    `urllib2.urlopen`, and seems to be sufficient enough for this script.
    """
    
    def __init__(self, id, contents, headers, url, code):
        self.id = id
        self.contents = contents
        self.headers = HeadersDict(headers)
        self.url = url
        self.code = code

    def read(self):
        return self.contents

    def geturl(self):
        return self.url

    def info(self):
        return self.headers
    
    def getcode(self):
        return self.code 

class BaqupClient(quip.QuipClient):
    """Subclass of `quip.QuipClient` that implements read caching for a poor- 
    person's resume capability: all client GET requests are cached in the
    `BaqupClient#cache_dir` directory and read from there if present instead of
    hitting the API, allowing the script to tear through the threads it's
    already backed up pretty quickly on subsequent runs. This also avoids
    incurring the associated rate limit costs.

    Combined with the rate limiting work in `quip.py`, this allowed us to get
    through backing up ~2.6K entities.
    
    To clear the cache, just delete the cache directory.
    """
    
    def __init__(self, cache_dir, *args, **kwds):
        """Construct a client.
        
        Parameters are the same as `quip.QuipClient`, with the addition of
        `cache_dir`, which is the directory to write cache files to. It will be
        created if it doesn't exist.
        """
        self.cache_dir = _normalize_path(cache_dir)
        super(BaqupClient, self).__init__(*args, **kwds)
    
    def get_blob(self, thread_id, blob_id):
        """Returns a `Blob` object with the contents of the given `blob_id`.

        It seems that `thread_id` is required to form the API URL, but it
        *appears* that blob IDs are unique (though the same blob may appear
        in more than one thread), and this class takes advantage of this 
        assumption.
        """
        if self._cache_has_blob(blob_id):
            return self._cache_get_blob(blob_id)
        
        response = super(BaqupClient, self).get_blob(thread_id, blob_id)
        
        blob = Blob(
            id = blob_id,
            contents = response.read(),
            headers = response.info().dict,
            url = response.geturl(),
            code = response.getcode(),
        )
        
        self._cache_put_blob(blob)
        
        return blob
    
    def _fetch_json(self, path, post_data=None, **args):
        """When `post_data` is `None`, we assume this is a read request and
        proxy to `#_fetch_json_with_caching()`. Otherwise, call goes 
        strait through to `quip.QuipClient#_fetch_json()`.
        """
        if post_data is None:
            return self._fetch_json_with_caching(path, **args)
        else:
            return super(BaqupClient, self)._fetch_json(path, 
                post_data=post_data, **args)
    
    def _fetch_json_with_caching(self, path, **args):
        """Pretty much what it sounds like... if it's in the cache, it's read
        from there. If it's not, the request is made and the results are put 
        into the cache.
        """
        url = self._url(path, **args)
        cache_key = re.sub(r'^https?\:\/\/', '', url)
        
        if self._cache_has(key=cache_key, format='json'):
            return self._cache_get(key=cache_key, format='json')
        
        value = super(BaqupClient, self)._fetch_json(path, post_data=None,
            **args)
        
        self._cache_put(key=cache_key, format='json', value=value)
        
        return value
    
    def _cache_filepath(self, key, format):
        """Get the fully-formed file path a cache entry with `key` and 
        `format` will be saved at.
        
        Returns a string.
        """
        return os.path.join(self.cache_dir,
            "{key}.{format}".format(key=key, format=format))
    
    def _cache_has(self, key, format):
        """Is this key/format pair in the cache?
        
        Returns a boolean.
        """
        return os.path.isfile(self._cache_filepath(key=key, format=format))
    
    def _cache_get(self, key, format):
        """Get a value out of the cache.

        This will raise if it's not there, so use `#_cache_has()` to check first
        or handle the exception.

        If `format` is 'json', returns the parse of the cache file contents.

        If `format` is 'bin', the cache file is opened in binary mode and you
        get back whatever Python 2 uses for read binary data. I think it's a
        string from what I remember.

        Otherwise, returns the result of a regular file read.
        """
        filepath = self._cache_filepath(key=key, format=format)
        
        logging.debug("CACHE GET key=%s format=%s from %s",
            key, format, filepath)
        
        mode = 'r'
        if format == 'bin':
            mode = 'rb'
        
        with open(filepath, mode=mode) as fp:
            contents = fp.read()
        
        if format == 'json':
            return json.loads(contents)
        else:
            return contents
    
    def _cache_put(self, key, format, value):
        """Put a value in the cache at a `key` and `format`.
        
        `key` is used as the relative path from `#cache_dir`, and `format`
        is used as the file extension.
        
        If format is 'json', `value` will be JSON encoded for writing.
        
        If format is 'bin', the file will be written in binary mode.
        
        Doesn't return.
        """
        filepath = self._cache_filepath(key=key, format=format)
        
        logging.debug("CACHE SET key=%s format=%s to %s",
            key, format, filepath)
        
        _ensure_path_exists(os.path.dirname(filepath))
        
        mode = 'w'
        if format == 'bin':
            mode = 'wb'
        
        if format == 'json':
            value = json.dumps(value, indent=2, sort_keys=True)
        
        with open(filepath, mode=mode) as fp:
            fp.write(value)
    
    def _cache_blob_keys(self, blob_id):
        """Returns a pair of strings as a `tuple`. The first element is the
        cache key for blob metadata; the second is for the contents.
        
        You may notice that only the blob's ID is used to form the keys;
        the thread ID is omitted.
        
        Though blobs are retrieved through threads, blob IDs alone appear to 
        uniquely identify them: from what I've seen, if you have a blob ID `B`
        and thread with IDs `X` and `Y` reference the blob via paths
        `/blob/X/B` and `/blob/Y/B`, that means the same blob is referenced in
        both `A` and `B`.
        
        Using that assumption, we cache blobs by **only their blob ID**, 
        avoiding retreiving and storing the same blob multiple times.
        """
        
        base_key = (re.sub(r'^https?\:\/\/', '', self.base_url) + 
            "/1/blob/" + blob_id)
        return (base_key + ".metadata", base_key + ".contents")
    
    def _cache_has_blob(self, blob_id):
        """Helper method to see if the cache has a blob, by checking for 
        *both* it's metadata and contents entries.
        
        Return as boolean.
        """
        metadata_key, contents_key = self._cache_blob_keys(blob_id)
        
        return (self._cache_has(key=metadata_key, format='json')
                and self._cache_has(key=contents_key, format='bin'))
    
    def _cache_get_blob(self, blob_id):
        """Helper method to get both a blob's metadata and contents entries and
        assemble them into a `Blob` instance.
        
        Like `#_cache_get()`, the entries need to be there, or it'll raise.
        
        Returns a `Blob`.
        """
        metadata_key, contents_key = self._cache_blob_keys(blob_id)

        metadata = self._cache_get(key=metadata_key, format='json')
        contents = self._cache_get(key=contents_key, format='bin')

        return Blob(
            id = blob_id,
            contents = contents,
            headers = metadata['headers'],
            url = metadata['url'],
            code = metadata['code'],
        )
    
    def _cache_put_blob(self, blob):
        """Helper method to put a `Blob` into the cache, writing both its
        metadata and contents entries.
        
        Doesn't return.
        """
        metadata_key, contents_key = self._cache_blob_keys(blob.id)
        
        self._cache_put(
            key = metadata_key,
            format = 'json',
            value = dict(
                id = blob.id,
                headers = blob.headers,
                url = blob.url,
                code = blob.code,
            )
        )
        
        self._cache_put(key=contents_key, format='bin', value=blob.contents)

def main():
    logging.getLogger().setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description="Backup of a Quip account")

    parser.add_argument("--access_token", required=True,
        help="Access token for the user whose account should be backed up")
    parser.add_argument("--root_folder_id", default=None,
        help="If provided, only the documents in the given folder will be "
             "backed up. Otherwise all folder and documents will be backed up.")
    parser.add_argument("--quip_api_base_url", default=None,
        help="Alternative base URL for the Quip API. If none is provided, "
             "https://platform.quip.com will be used")
    parser.add_argument("--output_directory", default="./",
        help="Directory where to place backup data.")
    parser.add_argument("--cache_directory", default=None,
        help="Directory where to cache downloaded thread data")
    parser.add_argument("--use_rate_limiting", action='store_true',
        help="Watch API rate limit and wait when it runs out")

    args = parser.parse_args()

    cache_dir = args.cache_directory
    
    if cache_dir is not None:
        cache_dir = _normalize_path(cache_dir)
        _ensure_path_exists(cache_dir)
        client = BaqupClient(
            cache_dir=cache_dir,
            access_token=args.access_token, base_url=args.quip_api_base_url,
            request_timeout=120, use_rate_limiting=bool(args.use_rate_limiting))
    else:
        client = quip.QuipClient(
            access_token=args.access_token, base_url=args.quip_api_base_url,
            request_timeout=120, thread_cache_dir=thread_cache_dir,
            use_rate_limiting=bool(args.use_rate_limiting))
    
    output_directory = os.path.join(
        _normalize_path(args.output_directory), "baqup")
    _ensure_path_exists(output_directory)
    shutil.rmtree(output_directory, ignore_errors=True)
    output_static_diretory = os.path.join(
        output_directory, _OUTPUT_STATIC_DIRECTORY_NAME)
    shutil.copytree(_STATIC_DIRECTORY, output_static_diretory)
    _run_backup(client, output_directory, args.root_folder_id)

def _run_backup(client, output_directory, root_folder_id):
    user = client.get_authenticated_user()
    processed_folder_ids = set()
    if root_folder_id:
        _descend_into_folder(root_folder_id, processed_folder_ids,
            client, output_directory, 0)
    else:
        _descend_into_folder(user["private_folder_id"], processed_folder_ids,
            client, output_directory, 0)
        _descend_into_folder(user["starred_folder_id"], processed_folder_ids,
            client, output_directory, 0)
    logging.info("Looking for conversations")
    conversation_threads = _get_conversation_threads(client)
    if conversation_threads:
        conversations_directory = os.path.join(output_directory, "Conversations")
        _ensure_path_exists(conversations_directory)
        for thread in conversation_threads:
            _backup_thread(thread, client, conversations_directory, 1)

def _descend_into_folder(folder_id, processed_folder_ids, client,
        output_directory, depth):
    if folder_id in processed_folder_ids:
        return
    processed_folder_ids.add(folder_id)
    try:
        folder = client.get_folder(folder_id)
    except quip.QuipError as e:
        if e.code == 403:
            logging.warning("%sSkipped over restricted folder %s.",
                "  " * depth, folder_id)
        else:
            logging.warning("%sSkipped over folder %s due to unknown error %d.",
                "  " * depth, folder_id, e.code)
        return
    except urllib2.HTTPError as e:
        logging.warning("%sSkipped over folder %s due to HTTP error %d.",
            "  " * depth, folder_id, e.code)
        return
    title = folder["folder"].get("title", "Folder %s" % folder_id)
    logging.info("%sBacking up folder %s...", "  " * depth, title)
    folder_output_path = os.path.join(output_directory, _sanitize_title(title))
    _ensure_path_exists(folder_output_path)
    for child in folder["children"]:
        if "folder_id" in child:
            _descend_into_folder(child["folder_id"], processed_folder_ids,
                client, folder_output_path, depth + 1)
        elif "thread_id" in child:
            thread = client.get_thread(child["thread_id"])
            _backup_thread(thread, client, folder_output_path, depth + 1)

def _backup_thread(thread, client, output_directory, depth):
    thread_id = thread["thread"]["id"]
    title = thread["thread"]["title"]
    logging.info("%sBacking up thread %s (%s)...",
        "  " * depth, title, thread_id)
    sanitized_title = _sanitize_title(title)
    if "html" in thread:
        # Parse the document
        try:
            tree = client.parse_document_html(thread["html"])
        except xml.etree.cElementTree.ParseError as e:
            logging.error(
                "Error parsing thread %s (%s), skipping backup: %s" % (
                    title, thread_id, e))
            return

        # Download each image and replace with the new URL
        for img in tree.iter("img"):
            src = img.get("src")
            if not src.startswith("/blob"):
                continue
            _, _, thread_id, blob_id = src.split("/")
            blob_response = client.get_blob(thread_id, blob_id)
            content_disposition = blob_response.info().get(
                "Content-Disposition")
            if content_disposition:
                image_filename = content_disposition.split('"')[-2]
            else:
                image_filename = "image.png"
            image_output_path = os.path.join(output_directory, image_filename)
            with open(image_output_path, "w") as image_file:
                image_file.write(blob_response.read())
            img.set("src", image_filename)
        html = unicode(xml.etree.cElementTree.tostring(tree))
        # Strip the <html> tags that were introduced in parse_document_html
        html = html[6:-7]

        document_file_name = sanitized_title + ".html"
        document_output_path = os.path.join(
            output_directory, document_file_name)
        document_html = _DOCUMENT_TEMPLATE % {
            "title": _escape(title),
            "stylesheet_path": ("../" * depth) +
                _OUTPUT_STATIC_DIRECTORY_NAME + "/main.css",
            "body": html,
        }
        with open(document_output_path, "w") as document_file:
            document_file.write(document_html.encode("utf-8"))
    messages = _get_thread_messages(thread_id, client)
    if messages:
        title_suffix = "messages" if "html" in thread else thread_id
        message_file_name = "%s (%s).html" % (sanitized_title, title_suffix)
        messages_output_path = os.path.join(output_directory, message_file_name)
        messages_html = _MESSAGES_TEMPLATE % {
            "title": _escape(title),
            "stylesheet_path": ("../" * depth) +
                _OUTPUT_STATIC_DIRECTORY_NAME + "/main.css",
            "body": "".join([_MESSAGE_TEMPLATE % {
                "author_name":
                    _escape(_get_user(client, message["author_id"])["name"]),
                "timestamp": _escape(_format_usec(message["created_usec"])),
                "message_text": _escape(message["text"]),
            } for message in messages])
        }
        with open(messages_output_path, "w") as messages_file:
            messages_file.write(messages_html.encode("utf-8"))

def _get_thread_messages(thread_id, client):
    max_created_usec = None
    messages = []
    while True:
        chunk = client.get_messages(
            thread_id, max_created_usec=max_created_usec, count=100)
        messages.extend(chunk)
        if chunk:
            max_created_usec = chunk[-1]["created_usec"] - 1
        else:
            break
    messages.reverse()
    return messages

def _get_conversation_threads(client):
    max_updated_usec = None
    threads = []
    thread_ids = set()
    while True:
        chunk = client.get_recent_threads(
            max_updated_usec=max_updated_usec, count=50).values()
        chunk.sort(key=lambda t:t["thread"]["updated_usec"], reverse=True)
        threads.extend([t for t in chunk
            if "html" not in t and t["thread"]["id"] not in thread_ids])
        thread_ids.update([t["thread"]["id"] for t in chunk])
        if chunk:
            chunk_max_updated_usec = chunk[-1]["thread"]["updated_usec"] - 1
            if chunk_max_updated_usec == max_updated_usec:
                logging.warning("New chunk had the same max_updated_usec (%d) "
                    "as the last one, can't get any older threads",
                    max_updated_usec)
                break
            max_updated_usec = chunk_max_updated_usec
        else:
            break
        logging.info("  Got %d threads, paged back to %s",
            len(threads), _format_usec(max_updated_usec))
    threads.reverse()
    return threads

def _ensure_path_exists(directory_path):
    if os.path.exists(directory_path):
        return
    os.makedirs(directory_path)

def _normalize_path(path):
    return os.path.abspath(os.path.expanduser(path))

def _sanitize_title(title):
    sanitized_title = re.sub(r"\s", " ", title)
    sanitized_title = re.sub(r"(?u)[^- \w.]", "", sanitized_title)
    if len(sanitized_title) > _MAXIMUM_TITLE_LENGTH:
        sanitized_title = sanitized_title[:_MAXIMUM_TITLE_LENGTH]
    return sanitized_title

_user_cache = {}
def _get_user(client, id):
    if id not in _user_cache:
        try:
            _user_cache[id] = client.get_user(id)
        except quip.QuipError:
            _user_cache[id] = {"id": id, "name": "Unknown user %s" % id}
    return _user_cache[id]

def _read_template(template_file_name):
    template_path = os.path.join(_TEMPLATE_DIRECTORY, template_file_name)
    with open(template_path, "r") as template_file:
        return "".join(template_file.readlines())

def _escape(s):
    return xml.sax.saxutils.escape(s, {'"': "&quot;"})

def _format_usec(usec):
    return datetime.datetime.utcfromtimestamp(usec / 1000000.0).isoformat()

_DOCUMENT_TEMPLATE = _read_template("document.html")
_MESSAGE_TEMPLATE = _read_template("message.html")
_MESSAGES_TEMPLATE = _read_template("messages.html")

if __name__ == '__main__':
    main()
