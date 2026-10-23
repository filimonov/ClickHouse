"""Minimal path-style SigV4 S3 client (stdlib only) for the cost rig.

The `Host` header is signed exactly as the endpoint spells it, so a byte-exact proxy in front of the store
(see the README, "Cost rig") forwards a request the store can still verify.
"""
from __future__ import annotations

import argparse
import base64
import datetime
import hashlib
import hmac
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

EMPTY_SHA256 = hashlib.sha256(b'').hexdigest()
DELETE_BATCH = 1000
DEFAULT_ENDPOINT = 'http://127.0.0.1:19121'
ACCESS_KEY = 'clickhouse'
SECRET_KEY = 'clickhouse'


class S3Error(RuntimeError):
    def __init__(self, method: str, url: str, status: int, body: str):
        super().__init__(f'{method} {url}: HTTP {status}: {body[:300]}')
        self.status = status
        self.body = body


def _quote(text: str, safe: str = '') -> str:
    return urllib.parse.quote(text, safe='-_.~' + safe)


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign_v4(method: str, path: str, query: dict, headers: dict, access_key: str, secret_key: str,
            region: str, amz_date: str) -> str:
    """Return the `Authorization` header value. `headers` must hold `Host`, `x-amz-date` and
    `x-amz-content-sha256`; every header passed is signed. `path` is the already-encoded request path."""
    lowered = {k.lower(): ' '.join(v.split()) for k, v in headers.items()}
    names = sorted(lowered)
    canonical_query = '&'.join(f'{_quote(k)}={_quote(v)}' for k, v in sorted(query.items()))
    canonical = '\n'.join([
        method, path, canonical_query,
        ''.join(f'{n}:{lowered[n]}\n' for n in names),
        ';'.join(names),
        lowered['x-amz-content-sha256'],
    ])
    date = amz_date[:8]
    scope = f'{date}/{region}/s3/aws4_request'
    to_sign = '\n'.join(['AWS4-HMAC-SHA256', amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = _hmac(('AWS4' + secret_key).encode(), date)
    for part in (region, 's3', 'aws4_request'):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    return (f'AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={";".join(names)},'
            f'Signature={signature}')


def _lower(headers) -> dict:
    return {k.lower(): v for k, v in headers.items()}


def _local(tag: str) -> str:
    return tag.rsplit('}', 1)[-1]


def _children(node: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in node if _local(c.tag) == name]


def _text(node: ET.Element, name: str, default: str = '') -> str:
    found = _children(node, name)
    return (found[0].text or default) if found else default


class S3Client:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = 'us-east-1'):
        self.endpoint = endpoint.rstrip('/')
        self.host = urllib.parse.urlparse(self.endpoint).netloc
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def request(self, method: str, bucket: str, key: str = '', query: dict | None = None, body: bytes = b'',
                headers: dict | None = None, ok: tuple = ()) -> tuple[int, dict, bytes]:
        """Send one signed request. Statuses in `ok` are returned instead of raising S3Error."""
        query = query or {}
        path = '/' + _quote(bucket) + ('/' + _quote(key, '/') if key else '')
        amz_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        signed = {'Host': self.host, 'x-amz-date': amz_date,
                  'x-amz-content-sha256': hashlib.sha256(body).hexdigest() if body else EMPTY_SHA256}
        signed.update(headers or {})
        signed['Authorization'] = sign_v4(method, path, query, signed, self.access_key, self.secret_key,
                                          self.region, amz_date)
        url = self.endpoint + path
        if query:
            url += '?' + '&'.join(f'{_quote(k)}={_quote(v)}' for k, v in sorted(query.items()))
        request = urllib.request.Request(url, data=body if body or method in ('PUT', 'POST') else None,
                                         method=method, headers=signed)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.status, _lower(response.headers), response.read()
        except urllib.error.HTTPError as e:
            data = e.read()
            if e.code in ok:
                return e.code, _lower(e.headers), data
            raise S3Error(method, url, e.code, data.decode(errors='replace')) from None

    def ensure_bucket(self, bucket: str) -> None:
        status, _, _ = self.request('HEAD', bucket, ok=(404,))
        if status == 404:
            self.request('PUT', bucket)

    def put(self, bucket: str, key: str, data: bytes) -> str:
        _, headers, _ = self.request('PUT', bucket, key, body=data)
        return headers.get('etag', '').strip('"')

    def get(self, bucket: str, key: str) -> bytes:
        return self.request('GET', bucket, key)[2]

    def head(self, bucket: str, key: str) -> dict | None:
        """Return {size, etag}, or None when the object does not exist."""
        status, headers, _ = self.request('HEAD', bucket, key, ok=(404,))
        if status == 404:
            return None
        return {'size': int(headers.get('content-length', 0)), 'etag': headers.get('etag', '').strip('"')}

    def delete(self, bucket: str, key: str) -> None:
        self.request('DELETE', bucket, key, ok=(404,))

    def list(self, bucket: str, prefix: str = '') -> list[dict]:
        out = []
        token = None
        while True:
            query = {'list-type': '2', 'prefix': prefix}
            if token:
                query['continuation-token'] = token
            root = ET.fromstring(self.request('GET', bucket, query=query)[2])
            for item in _children(root, 'Contents'):
                out.append({'key': _text(item, 'Key'), 'size': int(_text(item, 'Size', '0')),
                            'etag': _text(item, 'ETag').strip('"')})
            if _text(root, 'IsTruncated') != 'true':
                return out
            token = _text(root, 'NextContinuationToken')
            if not token:
                raise RuntimeError(f'truncated listing of {bucket}/{prefix} without a continuation token')

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Delete every object under `prefix`; return how many the store confirmed deleted."""
        keys = [o['key'] for o in self.list(bucket, prefix)]
        deleted = 0
        for i in range(0, len(keys), DELETE_BATCH):
            body = ('<Delete>' + ''.join(f'<Object><Key>{_xml_escape(k)}</Key></Object>'
                                         for k in keys[i:i + DELETE_BATCH]) + '</Delete>').encode()
            md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
            root = ET.fromstring(self.request('POST', bucket, query={'delete': ''}, body=body,
                                              headers={'Content-MD5': md5})[2])
            errors = _children(root, 'Error')
            if errors:
                raise RuntimeError(f'DeleteObjects failed for {len(errors)} keys, first: '
                                   f'{_text(errors[0], "Key")}: {_text(errors[0], "Code")}')
            deleted += len(_children(root, 'Deleted'))
        return deleted

    def create_multipart(self, bucket: str, key: str) -> str:
        root = ET.fromstring(self.request('POST', bucket, key, query={'uploads': ''})[2])
        return _text(root, 'UploadId')

    def upload_part(self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes) -> str:
        _, headers, _ = self.request('PUT', bucket, key, query={'partNumber': str(part_number), 'uploadId': upload_id},
                                     body=data)
        return headers.get('etag', '').strip('"')

    def complete_multipart(self, bucket: str, key: str, upload_id: str, parts: list[dict]) -> None:
        body = ('<CompleteMultipartUpload>' + ''.join(
            f'<Part><PartNumber>{p["part_number"]}</PartNumber><ETag>"{p["etag"]}"</ETag></Part>' for p in parts)
            + '</CompleteMultipartUpload>').encode()
        _, _, data = self.request('POST', bucket, key, query={'uploadId': upload_id}, body=body)
        # A 200 reply can still carry an <Error> element.
        if b'<Error>' in data:
            raise S3Error('POST', f'{bucket}/{key}?uploadId={upload_id}', 200, data.decode(errors='replace'))

    def abort_multipart(self, bucket: str, key: str, upload_id: str) -> None:
        self.request('DELETE', bucket, key, query={'uploadId': upload_id})

    def list_multipart_uploads(self, bucket: str, prefix: str = '') -> list[dict]:
        """RustFS 1.0.0-rc.3 returns nothing for a directory-style prefix such as `dir/`; pass the full key."""
        root = ET.fromstring(self.request('GET', bucket, query={'uploads': '', 'prefix': prefix})[2])
        return [{'key': _text(u, 'Key'), 'upload_id': _text(u, 'UploadId')} for u in _children(root, 'Upload')]

    def list_parts(self, bucket: str, key: str, upload_id: str) -> list[dict]:
        """Raise S3Error (NoSuchUpload) when the upload is gone."""
        root = ET.fromstring(self.request('GET', bucket, key, query={'uploadId': upload_id})[2])
        return [{'part_number': int(_text(p, 'PartNumber')), 'size': int(_text(p, 'Size', '0')),
                 'etag': _text(p, 'ETag').strip('"')} for p in _children(root, 'Part')]


def _xml_escape(text: str) -> str:
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def main() -> None:
    parser = argparse.ArgumentParser(prog='python3 -m yardstick.cost.s3client')
    sub = parser.add_subparsers(dest='cmd', required=True)
    ensure = sub.add_parser('ensure-bucket')
    ensure.add_argument('bucket')
    ensure.add_argument('--endpoint', default=DEFAULT_ENDPOINT)
    args = parser.parse_args()
    S3Client(args.endpoint, ACCESS_KEY, SECRET_KEY).ensure_bucket(args.bucket)


if __name__ == '__main__':
    main()
