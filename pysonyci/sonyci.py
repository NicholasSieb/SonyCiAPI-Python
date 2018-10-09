import os
import sys
import logging
import requests
import json
import simplejson
from requests.auth import HTTPBasicAuth
from requests_toolbelt import MultipartEncoder


try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0


SONYCI_URI = "https://api.cimediacloud.com"
SINGLEPART_URI = 'https://io.cimediacloud.com/upload'
MULTIPART_URI = 'https://io.cimediacloud.com/upload/multipart'
CHUNK_SIZE = 10 * 1024 * 1024
USE_THREADS = True


logging.basicConfig(filename='sony_ci.log', level=logging.DEBUG)


class SonyCiException(Exception):
    def __init__(self, error_code, error_msg):
        self.error_code = error_code
        self.error_msg = error_msg
        Exception.__init__(self, error_code, error_msg)

    def __str__(self):
        return '%s -> %s' % (self.error_code, self.error_msg)


class SonyCi(object):

    def __init__(self, config_path=None):
        if os.path.exists(config_path):
            cfg = ConfigParser()
            cfg.read(config_path)
        else:
            print("Config file not found.")
            sys.exit(1)
        self._authenticate(cfg)

    def _authenticate(self, cfg):
        url = SONYCI_URI + '/oauth2/token'
        data = {'grant_type': 'password',
                'client_id': cfg.get('general', 'client_id'),
                'client_secret': cfg.get('general', 'client_secret')}
        auth = HTTPBasicAuth(cfg.get('general', 'username'),
                             cfg.get('general', 'password'))
        req = requests.post(url, data=data, auth=auth)

        json_resp = req.json()
        logging.debug("auth: \n%s" % json.dumps(json_resp, indent=4))

        if req.status_code != requests.codes.ok:
            raise SonyCiException(json_resp['error'],
                                  json_resp['error_description'])

        self.access_token = json_resp['access_token']
        self.header_auth = {'Authorization': 'Bearer %s' % self.access_token}

        if cfg.get('general', 'workspace_id'):
            self.workspace_id = cfg.get('general', 'workspace_id')
        else:
            for w in self.workspaces(fields='name,class'):
                if 'Personal' in w['class']:
                    self.workspace_id = w['id']

    def workspaces(self, limit=50, offset=0, fields='class'):
        url = SONYCI_URI + '/workspaces'
        params = {'limit': limit,
                  'offset': offset,
                  'fields': fields}

        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        logging.debug("workspaces: \n%s" % json.dumps(json_resp, indent=4))

        if json_resp['count'] >= 1:
            for el in json_resp['items']:
                yield el

    # def list(self, kind='all', limit=50, offset=0,
    #          fields='description, parentId, folder'):
    def list(self, kind='all', limit=50, offset=0, fields='metadata'):
        if self.workspace_id:
            url = SONYCI_URI + '/workspaces/%s/contents' % self.workspace_id
        else:
            url = SONYCI_URI + '/workspaces'
        params = {'limit': limit,
                  'offset': offset,
                  'kind': kind,
                  'fields': fields}
        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        logging.debug("list: \n%s" % json.dumps(json_resp, indent=4))

        return json_resp


    def list_folder(self, kind='all', limit=50, offset=0, fields='metadata', folder_id=None):
        if self.workspace_id:
            url = SONYCI_URI + '/folders/%s/contents' % folder_id
        else:
            url = SONYCI_URI + '/workspaces'
        params = {'limit': limit,
                  'offset': offset,
                  'kind': kind,
                  'fields': fields}
        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        logging.debug("list: \n%s" % json.dumps(json_resp, indent=4))

        return json_resp

    def items(self):
        curr_offset = 0
        page_size = 50
        items = []

        while True:
            elts_curr_page = self.list(limit=page_size, offset=curr_offset)
            curr_offset+=page_size
            if not elts_curr_page['items']:
                break
            items.extend(elts_curr_page['items'])
        if len(items) >= 1:
            for el in items:
                logging.debug("items: \n%s" % json.dumps(el, indent=4))
            return items

    def assets(self):
        curr_offset = 0
        page_size = 50
        assets = []
        while True:
            elts_curr_page = self.list(kind='asset', limit=page_size, offset=curr_offset)
            curr_offset+=page_size
            if not elts_curr_page['items']:
                break
            assets.extend(elts_curr_page['items'])
        if len(assets) >= 1:
            for el in assets:
                logging.debug("assets: \n%s" % json.dumps(el, indent=4))
            return assets

    def folder_contents(self, folder_id):
        curr_offset = 0
        page_size = 50
        assets = []
        while True:
            elts_curr_page = self.list_folder(kind='asset', limit=page_size, offset=curr_offset, folder_id = folder_id)
            curr_offset+=page_size
            if not elts_curr_page['items']:
                break
            assets.extend(elts_curr_page['items'])
        if len(assets) >= 1:
            for el in assets:
                logging.debug("assets: \n%s" % json.dumps(el, indent=4))
            return assets

    def folders(self):
        curr_offset = 0
        page_size = 50
        folders = []
        while True:
            elts_curr_page = self.list(kind='folder', fields='parentId', limit=page_size, offset=curr_offset)
            curr_offset+=page_size
            logging.debug(elts_curr_page)
            if not elts_curr_page['items']:
                break
            folders.extend(elts_curr_page['items'])
        if len(folders) >= 1:
            for el in folders:
                logging.debug("folders: \n%s" % json.dumps(el, indent=4))
            return folders

    def search(self, name, limit=50, offset=0, kind="all", workspace_id=None):
        if not workspace_id:
            workspace_id = self.workspace_id

        url = SONYCI_URI + '/workspaces/%s/search' % workspace_id
        params = {'kind': kind,
                  'limit': limit,
                  'offset': offset,
                  'query': name}
        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        logging.debug(json_resp)
        return json_resp

    def upload(self, file_path, folder_id=None, workspace_id=None, metadata={}):
        if os.path.getsize(file_path) >= 5 * 1024 * 1024:
            print('Start multipart upload')
            asset_id = self._initiate_multipart_upload(file_path,
                                                       folder_id,
                                                       workspace_id,
                                                       metadata)
            if USE_THREADS:
                self._do_multipart_upload_part_parallel(file_path, asset_id)
            else:
                self._do_multipart_upload_part(file_path, asset_id)
            return self._complete_multipart_upload(asset_id)
        else:
            return self._singlepart_upload(file_path, folder_id, workspace_id, metadata)

    def _initiate_multipart_upload(self, file_path, folder_id=None,
                                   workspace_id=None,
                                   metadata={}):
        metadata = {'name': os.path.basename(file_path),
                'size': os.path.getsize(file_path),
                'workspaceId': self.workspace_id,
                'folderId': folder_id,
                'metadata': {'Resolution': '1080p', 'Language': 'English'}}

        #m = MultipartEncoder([('metadata', json.dumps(metadata))])
        m = json.dumps(metadata)

        req = requests.post(MULTIPART_URI, data=m, headers={'Content-Type': 'application/json', 'Authorization': 'Bearer %s' % self.access_token})
        json_resp = req.json()
        logging.debug("upload: init: %s" % json_resp)
        return json_resp['assetId']

    def _do_multipart_upload_part(self, file_path, asset_id):
        headers = {'Authorization': 'Bearer %s' % self.access_token,
                   'Content-Type': 'application/octet-stream'}
        s = requests.Session()
        part = 0
        with open(file_path, 'rb') as fp:
            while True:
                part = part + 1
                url = MULTIPART_URI + '/%s/%s' % (asset_id, part)
                buf = fp.read(CHUNK_SIZE)
                if not buf:
                    break
                # req = requests.put(url, data=buf, headers=headers)
                req = s.put(url, data=buf, headers=headers)
                #resp = req.text
                print('upload: part: %s' % part)

    def _do_multipart_upload_part_parallel(self, file_path, asset_id):
        from Queue import Queue
        from threading import Thread

        q = Queue()

        def worker():
            while True:
                headers = {'Authorization': 'Bearer %s' % self.access_token,
                           'Content-Type': 'application/octet-stream'}
                data = q.get()
                req = requests.put(data[0], data=data[1], headers=headers)
                resp = req.text
                print('upload: part: %s' % resp)
                q.task_done()

        for i in range(4):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

        part = 0
        with open(file_path, 'rb') as fp:
            while True:
                part = part + 1
                url = MULTIPART_URI + '/%s/%s' % (asset_id, part)
                buf = fp.read(CHUNK_SIZE)
                if not buf:
                    break
                data = [url, buf]
                q.put(data)
        q.join()

    def _complete_multipart_upload(self, asset_id):
        url = MULTIPART_URI + '/%s/complete' % asset_id
        req = requests.post(url, headers=self.header_auth)
        resp = req.text
        print("upload: complete: %s " % resp)

    def _singlepart_upload(self, file_path, folder_id=None, workspace_id=None, metadata={}):
        #import httplib as http_client
        #http_client.HTTPConnection.debuglevel = 1
        metadata = {
        'name': os.path.basename(file_path),
        'size:': os.path.getsize(file_path),
        'workspaceId': self.workspace_id,
        'folderId': folder_id,
        'metadata': {'Resolution': '1080p', 'Language': 'English'}
        }

        m = MultipartEncoder([('filename', (os.path.basename(file_path), open(file_path, 'rb'))), 
                      ('metadata', json.dumps(metadata))])

        req = requests.post(SINGLEPART_URI, data=m, headers={'Content-Type': m.content_type, 'Authorization': 'Bearer %s' % self.access_token})

        json_resp = req.json()
        logging.debug(json_resp)
        return json_resp['assetId']

    

    def create_mediabox(self, name, asset_ids, type, allow_download=False,
                        recipients=[], message=None, password=None,
                        expiration_days=None, expiration_date=None,
                        send_notifications=False, notify_on_open=False):
        data = {'name': name,
                'assetIds': asset_ids,
                'type': type,
                'recipients': recipients}

        if message:
            data['message'] = message
        if password:
            data['password'] = password
        if expiration_days:
            data['expirationDays'] = expiration_days
        if expiration_date:
            data['expirationDate'] = expiration_date
        if send_notifications:
            data['sendNotifications'] = 'true'
        if notify_on_open:
            data['notifyOnOpen'] = 'true'

        url = SONYCI_URI + '/mediaboxes'
        req = requests.post(url, json=data, headers=self.header_auth)

        json_resp = req.json()
        logging.debug('create_mediabox: %s' % json_resp)
        return json_resp['mediaboxId'], json_resp['link']

    def create_folder(self, name, parent_folder_id=None, workspace_id=None):
        url = SONYCI_URI + '/folders'
        data = {'name': name}
        if parent_folder_id:
            data['parentFolderId'] = parent_folder_id

        if workspace_id:
            data['workspaceId'] = workspace_id
        else:
            data['workspaceId'] = self.workspace_id

        req = requests.post(url, json=data, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('create_folder: %s' % json_resp)
        return json_resp['folderId']

    def move_assets(self, assets, folder_id=None, workspace_id=None):
        url = SONYCI_URI + '/assets/move'
        data = {'assetIds': assets}
        if folder_id:
            data['folderId'] = folder_id

        req = requests.post(url, json=data, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('move_assets: %s' % json_resp)
        return json_resp

    def detail_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s' % folder_id
        req = requests.get(url, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('detail_folder: %s' % json_resp)
        return json_resp

    def delete_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s' % folder_id
        req = requests.delete(url, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('delete_folder: %s' % json_resp)

        if json_resp['message'] == 'Folder was deleted.':
            return True
        else:
            return False

    def trash_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s/trash' % folder_id
        req = requests.post(url, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('trash_folder: %s' % json_resp)

        if json_resp['message'] == 'Folder was trashed.':
            return True
        else:
            return False

    def archive(self, asset_id):
        url = SONYCI_URI + '/assets/%s/archive' % asset_id
        req = requests.post(url, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('archive: %s' % json_resp)

        if json_resp['message'] == 'Asset archive has started.':
            return True
        else:
            return False

    def download(self, asset_id):
        for a in self.assets():
            if asset_id in a['id']:
                name = a['name']

        url = SONYCI_URI + '/assets/%s/download' % asset_id

        req = requests.get(url, headers=self.header_auth)
        json_resp = req.json()
        #logging.debug('download: %s' % json_resp)
        if 'location' not in json_resp:
            return False
	
        if json_resp['location']:
            req = requests.get(url=json_resp['location'], stream=True)
            with open(name, 'wb') as fp:
                for chunk in req.iter_content(chunk_size=8192):
                    if chunk:
                        fp.write(chunk)

    def delete_asset(self, asset_id):
        url = SONYCI_URI + '/assets/%s'
        req = requests.delete(url, headers=self.header_auth)
        json_resp = req.json()
        logging.debug('delete_asset: %s' % json_resp)

        if json_resp['message'] == 'Asset was deleted.':
            return True
        else:
            return False

if __name__ == "__main__":

    cfg_file = "/home/nick/Documents/ai/pysonyci/config/ci.cfg"
    ci = SonyCi(cfg_file)
    print ' token %s' % ci.access_token

    #TESTING
    #print(ci.search("cat"))
    #ci.download("6f72b05954dc42ffb874df9e55520285")
    #ci.download("8c15ea8d19ad4cd7b3e850e354c8107d")
    #print(ci.folders())
    #print(ci.items())
    #print(ci.create_folder("Folder_Creation_Test"))
    #print(ci.detail_folder(folder_id))
    #print(type(ci.folder_contents(folder_id)))
    #print(ci.folder_contents(folder_id))
    #ci.upload('/Users/Nick/Documents/Python/test_ci/GH010146.MP4', folder_id=folder_id, metadata=meta)
    ######

    #folder_id = "e18d9b3dbf974fe58f5e9a7882a95517"
    folder_id = "0b98a0135e82440ea9537b269df9e91c" #set folder id for download
    meta = {'custom_metadata': 'data'}
    '''
    Here is an example of usage for data gathering
    You can set a folder_id above to choose where to download assets from
    The move_assets command can be used to take assets and move to another folder
    You should probably move assets after a successful download so you know the object has been downloaded
    '''
    #assets = ["6f72b05954dc42ffb874df9e55520285", "8c15ea8d19ad4cd7b3e850e354c8107d"]
    #print(ci.move_assets(assets=assets, folder_id="e18d9b3dbf974fe58f5e9a7882a95517"))
    for x in ci.folder_contents(folder_id):
        ci.download(x["id"])
        print(x["id"])


