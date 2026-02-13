#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import http.server
import json
import logging
import os
import re


def copy_byte_range(infile, outfile, start=None, stop=None, bufsize=16 * 1024):
    """Like shutil.copyfileobj, but only copy a range of the streams.
    Both start and stop are inclusive.
    """
    if start is not None:
        infile.seek(start)
    while 1:
        to_read = min(bufsize, stop + 1 - infile.tell() if stop else bufsize)
        buf = infile.read(to_read)
        if not buf:
            break
        outfile.write(buf)


BYTE_RANGE_RE = re.compile(r"bytes=(\d+)-(\d+)?$")


def parse_byte_range(byte_range):
    """Returns the two numbers in 'bytes=123-456' or throws ValueError.
    The last number or both numbers may be None.
    """
    if byte_range.strip() == "":
        return None, None

    m = BYTE_RANGE_RE.match(byte_range)
    if not m:
        raise ValueError("Invalid byte range %s" % byte_range)

    first, last = [x and int(x) for x in m.groups()]
    if last and last < first:
        raise ValueError("Invalid byte range %s" % byte_range)
    return first, last


class HTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Adds support for HTTP 'Range' requests to SimpleHTTPRequestHandler
    The approach is to:
    - Override send_head to look for 'Range' and respond appropriately.
    - Override copyfile to only transmit a range when requested.
    """

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_GET(self):
        path = self.translate_path(self.path)
        if path.endswith("/"):
            self._set_headers()
            output = {}
            _, _, filenames = next(os.walk(path))
            for f in filenames:
                if not f.endswith(".meta"):
                    output[f] = os.path.getsize(path + f)
            self.wfile.write(json.dumps(output).encode())
            return
        else:
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

    def guess_type(self, path):
        with open(path + ".meta") as m:
            content_type = str(m.read())
        return content_type

    def send_head(self):
        isRange = True
        if "Range" not in self.headers:
            self.headers["Range"] = "bytes=0-"
            isRange = False
        try:
            self.range = parse_byte_range(self.headers["Range"])
        except ValueError:
            self.send_error(400, "Invalid byte range")
            return None
        first, last = self.range

        # Mirroring SimpleHTTPServer.py here
        path = self.translate_path(self.path)
        f = None

        try:
            f = open(path, "rb")
            ctype = self.guess_type(path)
        except Exception:
            self.send_error(404, "File not found")
            return None

        fs = os.fstat(f.fileno())
        file_len = fs[6]

        if file_len != 0 and first >= file_len:
            self.send_error(416, "Requested Range Not Satisfiable")
            return None

        if isRange:
            self.send_response(206)
        else:
            self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Accept-Ranges", "bytes")

        if last is None or last >= file_len:
            last = file_len - 1
        response_length = last - first + 1

        self.send_header("Content-Range", "bytes %s-%s/%s" % (first, last, file_len))
        self.send_header("Content-Length", str(response_length))
        self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
        self.send_header("Access-Control-Allow-Headers", "Authorization")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        return f

    def copyfile(self, source, outputfile):
        if not self.range:
            return http.server.SimpleHTTPRequestHandler.copyfile(self, source, outputfile)

        # SimpleHTTPRequestHandler uses shutil.copyfileobj, which doesn't let
        # you stop the copying before the end of the file.
        start, stop = self.range  # set in send_head()
        copy_byte_range(source, outputfile, start, stop)

    def do_OPTIONS(self):
        self.send_response(200, "ok")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
        self.send_header("Access-Control-Allow-Headers", "Authorization")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_PUT(self):
        path = self.translate_path(self.path)
        logging.info(path)
        if path.endswith("/"):
            self.send_response(405, "Method Not Allowed")
            self.wfile.write("PUT not allowed on a directory\n".encode())
            return
        else:
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError:
                pass
            length = int(self.headers["Content-Length"])
            content_type = self.headers["Content-Type"]
            with open(path, "wb") as f, open(path + ".meta", "wt") as m:
                f.write(self.rfile.read(length))
                m.write(content_type)
            self.send_response(201, "Created")
            self.end_headers()
            self.wfile.write("Successfully created file\n".encode())

    def do_DELETE(self):
        path = self.translate_path(self.path)
        if path.endswith("/"):
            self.send_response(405, "Method Not Allowed")
            self.end_headers()
            self.wfile.write("DELETE not allowed on a directory\n".encode())
            return
        else:
            try:
                os.remove(path)
                os.remove(path + ".meta")
                self.send_response(200, "Deleted")
                self.end_headers()
                self.wfile.write("Successfully deleted file\n".encode())
            except Exception:
                self.send_response(401, "Not deleted")
                self.end_headers()
                self.wfile.write("Could not delete file\n".encode())


class BasicObjectStore:
    def __init__(self, config):

        staging_config = config.get("staging", {})
        object_store_config = staging_config.get("polytope", {})
        self.host = object_store_config.get("host", "0.0.0.0")
        self.port = object_store_config.get("port", "8000")
        self.root_dir = object_store_config.get("root_dir", "/data")

    def run(self):

        if not os.path.isdir(self.root_dir):
            try:
                os.mkdir(self.root_dir)
            except Exception:
                logging.info("Could not create the basic object store root directory")
                raise

        os.chdir(self.root_dir)
        try:
            httpd = http.server.HTTPServer(("0.0.0.0", int(self.port)), HTTPRequestHandler)
            logging.info("Serving HTTP on %s port %s ..." % (self.host, self.port))
            logging.info("basic object store started.")
            httpd.serve_forever()
        except Exception as e:
            logging.exception(e)
            logging.info("Stop signal received, shutting down server")
            httpd.socket.close()
