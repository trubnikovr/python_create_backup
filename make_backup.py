from email import header
import re
from dotenv import dotenv_values
import time
import os
import pexpect
import sys
import glob
import tarfile
#import yadisk
import base64
import requests
import hashlib
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

#import shutil

class Yd(): 
    y = False
    appKey = '' # add login:app password
    def __init__(self) -> None:
        pass
 
    def getHost(self):
        return "https://webdav.yandex.ru/";    
    def upload(self, filename):
        try:
            Headers = {
                "Authorization": "Basic " + base64.b64encode(self.appKey.encode('ascii')).decode("utf-8"),#dHJ1Ym5pa292LnJ1c3RhbTpocWRtemZkeXFyd25qemln
            }

            print(filename)
            with open(filename, 'rb') as file_to_check:
                 
                # read contents of the file
                data: bytes = file_to_check.read()    
                # pipe contents of the file through
                md5_returned = hashlib.md5(data).hexdigest()
                readable_hash = hashlib.sha256(data).hexdigest();
                headers = {
                        # "Transfer-Encoding":"chunked",
                        "Etag": md5_returned,
                        "Sha256": readable_hash,
                        "Content-Length": str(len(data))
                    }
                headers.update(Headers)
                file_to_check.seek(0, 0)
                with tqdm(total=int(len(data)), unit="B", unit_scale=True, unit_divisor=1024 * 10) as t:
                    wrapped_file = CallbackIOWrapper(t.update, file_to_check, "read")
                    response = requests.request('PUT', self.getHost() + 'backups/' + os.path.basename(filename), headers=headers, data=wrapped_file, stream = True)

            # with open(file_path, "rb") as f:
            #     with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
            #         wrapped_file = CallbackIOWrapper(t.update, f, "read")
            #         result = requests.request(request_type, self.getHost() + uri, headers=headers, data=wrapped_file, stream = True)

 
    
            if(response.status_code != 201):
                print(response.reason)
                raise Exception("status_code is wrong")

            return True
        except FileNotFoundError:
            print(filename + "is not found")
        

    def reguest(self, uri, request_type = "GET", data = '', **kwargs):
      
        headers = {
            "Authorization": "Basic " + base64.b64encode(self.appKey.encode('ascii')).decode("utf-8"),#dHJ1Ym5pa292LnJ1c3RhbTpocWRtemZkeXFyd25qemln
        
        }
        argHeader = kwargs.get('headers', {})

        headers.update(kwargs.get('headers', {}))
        file_path = "/Users/rustamtrubnikov/Sites/auksion.live/storage/1.gz"
        file_size = os.stat(file_path).st_size
        
        with open(file_path, "rb") as f:
            with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                wrapped_file = CallbackIOWrapper(t.update, f, "read")
                result = requests.request(request_type, self.getHost() + uri, headers=headers, data=wrapped_file, stream = True)



        return result
    
    def getData(self,  data):
        for i in [data[i:i+20] for i in range(0, len(data), 20)]:
            print(i)
            yield i
        #pass

class Backup():
    cloud = False
    def __init__(self) -> None:
        self.env = dotenv_values(".env")
        
    def getCloud(self):
        if(self.cloud is False):
            self.cloud = Yd()
        
        return self.cloud

    def create_essentials(self):
        pass
         
    def remove_old_files(self):
        path=os.getcwd()+"/storage/backups/"
        now = time.time()

        for f in os.listdir(path):
            # filter only files older than 25 days
            if os.stat(os.path.join(path, f)).st_mtime < now - 25 * 86400: # 25 days
                if os.path.isfile(f):
                    os.remove(os.path.join(path, f))
          
    def remove_old_filesOLD(self):
        path=os.getcwd()+"/storage/*.tar*"
 
        # It returns an iterator which will
        # be printed simultaneously.
        print("\nUsing glob.iglob()")
         
        # Prints all types of txt files present in a Path
        for file in glob.iglob(path, recursive=False):
            os.remove(file)
            print("{file} was removed".format(file = file))

    def create_archive(self, filename):

        with tarfile.open(filename + '.gz',"w:gz") as tar:
            tar.add(filename)
        
        # shutil.copy(src, dst)
        print("archive was created")
        return filename + '.gz'
        
    def moveToYandexDisc(self, filename):
        print('uploading file')
        self.getCloud().upload(filename)
        

    def create_backup_database(self, table_names=None):
        
        db_name = self.env["DB_DATABASE"]
        db_user = self.env["DB_USERNAME"] 
        db_password = self.env["DB_PASSWORD"]
        db_host = self.env["DB_HOST"]

        filename = os.getcwd() + "/storage/backups/name_of_backup_file" + "-" + time.strftime("%Y%m%d") + ".tar"
        
        if(os.path.exists(filename)):
            os.remove(filename)

        self.remove_old_files()

        child = pexpect.spawn(str(
            "pg_dump {db_name} -U {db_user} -h localhost -F t -f {filename}".format(
                filename = filename,
                db_user = db_user,
                db_name = db_name
                )
        ))
        child.expect("Password:", timeout = -1 )
        child.sendline(db_password) 
        print("password was entered")
        child.expect(pexpect.EOF, timeout = -1)
        print("backup file was created")
        archive_filename = self.create_archive(filename=filename)

        self.moveToYandexDisc(filename=archive_filename)
        # self.moveToYandexDisc(filename='/Users/rustamtrubnikov/Sites/auksion.live/storage/1.gz')
        print("done!!!")
        sys.exit()
        return False

        if table_names is not None:
            for x in table_names:
                command_str = command_str +" -t "+x

        command_str = command_str + " -F c -b -v -f '"+backup_path+"/"+filename+"'"
        try:
            os.system(command_str)
            print ("Backup completed")
        except Exception as e:
            print ("!!Problem occured!!")
            print (e)
     

if __name__ == "__main__":
  bk = Backup().create_backup_database()
