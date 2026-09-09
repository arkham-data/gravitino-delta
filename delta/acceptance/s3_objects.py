#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Minimal SigV4 client for exact manifests of one explicitly owned S3 prefix."""

import datetime
import hashlib
import hmac
from pathlib import Path
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from harness import credentials


class OwnedObjects:
    def __init__(self, profile, prefix):
        if not prefix or prefix.startswith("/") or ".." in prefix.split("/"):
            raise ValueError("An explicit owned prefix is required")
        self.profile = profile
        self.prefix = prefix.rstrip("/") + "/"
        self.access, self.secret = credentials(profile["credentialsFile"])

    def request(self, method, key="", query=None, data=b""):
        if method not in ("GET", "PUT") or ".." in key.split("/"):
            raise ValueError("Only owned reads and explicit writes are allowed")
        if key and not key.startswith(self.prefix):
            raise ValueError("Object outside owned prefix")
        endpoint = urllib.parse.urlsplit(self.profile["endpoint"])
        path = "/" + self.profile["bucket"] + "/" + urllib.parse.quote(key, safe="/~")
        query_string = urllib.parse.urlencode(
            sorted((query or {}).items()), quote_via=urllib.parse.quote
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        date = now.strftime("%Y%m%d")
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        payload = hashlib.sha256(data).hexdigest()
        headers = {
            "host": endpoint.netloc,
            "x-amz-content-sha256": payload,
            "x-amz-date": stamp,
        }
        signed = ";".join(sorted(headers))
        canonical_headers = "".join(
            key + ":" + headers[key] + "\n" for key in sorted(headers)
        )
        canonical = "\n".join(
            (method, path, query_string, canonical_headers, signed, payload)
        )
        scope = date + "/" + self.profile["region"] + "/s3/aws4_request"
        signing_key = ("AWS4" + self.secret).encode()
        for part in (date, self.profile["region"], "s3", "aws4_request"):
            signing_key = hmac.new(signing_key, part.encode(), hashlib.sha256).digest()
        string_to_sign = "\n".join(
            (
                "AWS4-HMAC-SHA256",
                stamp,
                scope,
                hashlib.sha256(canonical.encode()).hexdigest(),
            )
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()
        headers["Authorization"] = (
            "AWS4-HMAC-SHA256 Credential="
            + self.access
            + "/"
            + scope
            + ", SignedHeaders="
            + signed
            + ", Signature="
            + signature
        )
        url = (
            endpoint.scheme
            + "://"
            + endpoint.netloc
            + path
            + ("?" + query_string if query_string else "")
        )
        request = urllib.request.Request(
            url, data=data if method == "PUT" else None, headers=headers, method=method
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()

    def manifest(self, archive=None):
        token = None
        entries = []
        while True:
            query = {"list-type": "2", "prefix": self.prefix}
            if token:
                query["continuation-token"] = token
            root = ET.fromstring(self.request("GET", query=query))
            ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
            for item in root.findall("s:Contents", ns):
                key = item.findtext("s:Key", namespaces=ns)
                content = self.request("GET", key)
                entry = {
                    "key": key,
                    "size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "etag": item.findtext("s:ETag", namespaces=ns),
                }
                entries.append(entry)
                if archive is not None:
                    path = Path(archive) / hashlib.sha256(key.encode()).hexdigest()
                    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    path.write_bytes(content)
            token = root.findtext("s:NextContinuationToken", namespaces=ns)
            if not token:
                return sorted(entries, key=lambda row: row["key"])
