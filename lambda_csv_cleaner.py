import os, sys, psutil
import io, csv, time
import boto3
from multiprocessing import Process, Pipe
from multiprocessing import Manager, Value
import zlib
import gzip
import collections
from collections import defaultdict
from os.path import isfile
from pprint import pprint as pp
from io import StringIO
from contextlib import closing
import itertools as it,operator as op
import multiprocessing, logging
logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.INFO)
logger.warning('doomed')
from cli_layer.fmt import  pfmt, pfmtv, pfmtd, psql
from io import  BytesIO as cStringIO
import lz4.frame as LF


e=sys.exit   
dec={}
dec[0] = zlib.decompressobj(32 + zlib.MAX_WBITS)  # offset 32 to skip the header



udec = [zlib.decompressobj(32 + zlib.MAX_WBITS), zlib.decompressobj(32 + zlib.MAX_WBITS)]

s:dict    = {}
empty:int = 0
hlen:int  = 46

is_partial= False
pacnt     = 0
grid:int  = 0
delim:str = ','
header:str= 'test_id,test_status,test_status_detail,test_origin,test_type,test_id,test_id,uploader,test_id,test_display_name,test_title'.\
            split(delim)

rcnt      = defaultdict(int)
bid       = defaultdict(int)
partcnt   = defaultdict(int)
#partCount = defaultdict(int)
on, off   = 1, 0

colids_to_clean:list=[]

s3r = boto3.resource('s3')
null_cnt=0

class CsvStreamer:

    def __init__(self, buffer):
        self.cln=self.__class__.__name__
        self.stream=buffer
        print('Created %s' % self.cln)
        self.partial={}
        #self.limit=lame_duck

    def seek(self, pos):
        self.stream.seek(pos)
    def tell(self):
        return self.stream.tell()
    def readlines(self):
        global  null_cnt
        while True:
            #print(rcnt[0])
            #if self.limit and  rcnt[0]>self.limit: break 
            line:bytes=self.readline()

            if line:

                if b'\x00' in line:
                    line=line.replace(b'\x00', b'')
                    null_cnt +=1                    
                self.line:bytes=line
                try:
                
                    yield line.decode()
                except UnicodeDecodeError:
                    #elapsed = time.perf_counter() - s
                    
                    #if off: print(f'{grid}, {line[:20]}, mem[{process.memory_info().rss/1024:7,.2f}], sec[{elapsed:0.2f}]')
                    yield b'dummy'.decode('utf-8') #b','.join(line.split(b',')[:-1]).decode('utf-8') #ignored
            else:
                break
                
    def readline(self):

        return self.stream.readline()

    def write(self, data, *args, **kwargs):
        pass
    def reset(self):
        self.cnt=0
    def __enter__(self):
        return self
    def close(self):
        self.fh.close()
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass



def uploader(evt, direct_pipes, reversed_pipe):
    plen = len(direct_pipes)

    process = psutil.Process(os.getpid())
    s3 = boto3.client('s3')
    bucket_name:str = evt['bucket_name']
    out_key:str     = evt['file']['out_key']
    
    mpu = s3.create_multipart_upload(Bucket=bucket_name, Key=out_key, StorageClass='STANDARD')
    part_info = {'Parts':[]}
    out_gz_stream = cStringIO()
    compressor:gzip.GzipFile = gzip.GzipFile(fileobj=out_gz_stream, mode='wb',compresslevel=2)
    ss={}
    def uploadPart(wid,partCount=[0]):

        partCount[0] += 1
        out_gz_stream.seek(0)
        ss[wid]=time.perf_counter() 
        print(f'f{wid}: Started upload: cleaner[{wid}],   part[{partCount[0]}], row[{rcnt[wid]}], mem[{process.memory_info().rss/1024:7,.2f}], gz[{out_gz_stream.tell():7,.2f}]]')
        part=s3.upload_part(Bucket=bucket_name
                       , Key=out_key
                       , PartNumber=partCount[0]
                       , UploadId=mpu['UploadId']
                       , Body=out_gz_stream)
        part_info['Parts'].append(
                {
                    'PartNumber': partCount[0],
                    'ETag': part['ETag']
                } )

        out_gz_stream.seek(0)
        out_gz_stream.truncate(0)
        elapsed = time.perf_counter() - ss[wid]
        print(f'f{wid}: Finished upload: part[{partCount[0]}], row[{rcnt[wid]}], mem[{process.memory_info().rss/1024:7,.2f}], elapsed[{elapsed:0.2f}]')
    done=[1 for _ in range(len(direct_pipes))]

    while True:
        #print('uploader...')
        #time.sleep(0.1)
        for cwid, con in direct_pipes.items():
            #print('uploader...2')
            if con[0].poll():
                print(cwid, 'uploader...3')
                data = con[0].recv()
                if data[-1]=='DONE': 
                    print(cwid, 'uploader...DONE')
                    wid, _=data
                    if cwid == wid: 
                        done[wid] = 0
                        break
                        
                else:
                    wid, bid, chunk= data
                    if cwid==wid:
                        compressor.write(LF.decompress(chunk))
                        print(f'{cwid}: uploader..., got data gz[{out_gz_stream.tell():7,.2f}]')

                        if out_gz_stream.tell()>10<<20:
                            uploadPart(cwid)
                        print(cwid,'uploader...4')
                        reversed_pipe[cwid][1].send([cwid, bid, 'UPLOADED'])
                        print(cwid, 'uploader...5')
            if sum(done) == 0: break
        if sum(done) == 0: break
    #for wid, file_part_info in enumerate(part_info):
    compressor.close()
    uploadPart(cwid)
    s3.complete_multipart_upload(Bucket=bucket_name
        , Key=out_key
        , UploadId=mpu['UploadId']
        , MultipartUpload=part_info
        )
    print('uploader...exit')
def cleaner( wid,  evt, data_pipe_in, direct_pipe_out, reversed_pipe_in, result_pipe_child):
    global is_partial, empty, pacnt, header, colids_to_clean,  null_cnt, dec

    
    s[wid]= time.perf_counter()
    file_evt= evt['file']
    process = psutil.Process(os.getpid())
    columns_to_clean=evt['columns_to_clean']
    #assert columns_to_clean
    lame_duck = int(file_evt['lame_duck'])
    bucket_name:str = evt['bucket_name']
    in_key:str      = file_evt['in_key']
    obj = s3r.Object(
        bucket_name=bucket_name,
        key=in_key
    )

    #out_gz_stream:cStringIO = cStringIO()
    #compressor:gzip.GzipFile = gzip.GzipFile(fileobj=out_gz_stream, mode='wb',compresslevel=5)

    in_csv_stream:cStringIO = cStringIO()
    
    
    out_csv_stream:StringIO = StringIO()
    create:csv.writer=csv.writer(out_csv_stream,quoting=csv.QUOTE_ALL,   quotechar='"', lineterminator='\n') #quoting=csv.QUOTE_NONE, QUOTE_MINIMAL
        

    spos:int=0
    rcnt[wid]=0
    bid[wid]=0
    partcnt[wid]=0
    
    if 1:
        print(f'f{wid}: getting header: {bid[wid]}')
        try:
            colids_to_clean = [header.index(col) for col in columns_to_clean]
        except ValueError:
            pp(header)
            pp(columns_to_clean)
            raise
        #assert colids_to_clean
        if len(colids_to_clean)!=len(columns_to_clean):
            pp(colids_to_clean)
            pp(columns_to_clean)
            raise Exception(f'Column name mismatch in "columns_to_clean": {columns_to_clean}, "colids_to_clean": {colids_to_clean}')
        pp(colids_to_clean)

                        
    def push_batch(wid, bid=bid, rcnt=rcnt):
        global is_partial
        out_csv_stream.seek(0)
        out_data=out_csv_stream.getvalue()
        print(f'sending to uploader: cleaner[{wid}], batch[{bid[wid]}], gz[{len(out_data):7,.0f}]')
        direct_pipe_out.send([wid, bid[wid], LF.compress(out_data.encode())])
        out_csv_stream.seek(0)
        out_csv_stream.truncate(0)
        bid[wid] +=1  
        

                        
    with closing(in_csv_stream) as rr:
        r:CsvStreamer=CsvStreamer(rr)
        #dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
        
        while True:
            #print('CLEANER:waiting for data')
            #time.sleep(1)
            if data_pipe_in.poll():
                #print('got data')
                sdata = data_pipe_in.recv()
                if sdata[-1]=='DONE': 
                    break
                
                cwid, cid, data = sdata
                assert cwid==wid, f'{cwid}!={wid}'
                #print('CLEANER: got data: ', len(s3_chunk))
                #time.sleep(1)
                #continue
                
                spos:int= in_csv_stream.tell()
                if is_partial:
                    #in_csv_stream.write(r.line)
                    pass
                    


                in_csv_stream.write(LF.decompress(data))
                in_csv_stream.seek(spos)
                is_partial = False
                
                if on:
                    f = (line for line in r.readlines())
                    reader:csv.reader = csv.reader(f, delimiter=',',  quotechar='"', lineterminator='\n')


                        
                    try:
                        gwid=wid
                        gcid=cid
                        for rid, row in enumerate(reader):
                            #clean row
                            if row and len(row)>0:
                                if len(row)<hlen: #partial row
                                    pp(row)
                                    print(len(row))
                                    print('Found partial', gwid,gcid, rid, len(row))
                                    print(r.line)
                                    raise Exception(f'Partial row of length "{len(row)}", expecting ({hlen}')
                                for colid in colids_to_clean:
                                    row[colid] = row[colid].replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                
                                create.writerow(row)
                                elapsed = time.perf_counter() - s[wid]        
                                if rcnt[wid] and rcnt[wid]%25_000 == 0: print(f'f{wid}: Cleaned rows[{rcnt[wid]:7,.0f}], mem[{process.memory_info().rss/1024:7,.2f}], sec[{elapsed:0.2f}]')
                                rcnt[wid] +=1
                                #if rcnt[wid]%10000==0:
                                #    print(f'[{wid}]: writerow[{rcnt[wid]}], ustatus:{ustatus}')
                            else:
                                print('Found empty line')
                        
                    except csv.Error as ex:
                        print('CSV error: rcnt%s, reader_line %d,\n ex: %s' % (rcnt[wid], reader.line_num, str(ex)))
                        print(r.line)
                        pp(r.line)
                        raise
                        
                        
                if on:
                    if 0:
                        out_csv_stream.seek(0)
                        compressor.write(out_csv_stream.getvalue().encode())
                        out_csv_stream.truncate(0)
                    #print('cleaner...')
                    if out_csv_stream.tell() > 10<<16:
                        if not bid[wid]:
                            push_batch(wid)
                            partcnt[wid] +=1
                        else:
                            
                            if reversed_pipe_in.poll():
                                uwid, ubid, upload_status= reversed_pipe_in.recv()
                                if uwid == wid:
                                    partcnt[wid] +=1
                                    push_batch(wid)
                            else:
                                #slow down
                                #time.sleep(0.1)
                                pass
                            
                            #test
            #if lame_duck and  rcnt[wid]>lame_duck: break
        print('CLEANER:end of loop')
        #compressor.close()
        
        push_batch(wid) #leftovers
        
        direct_pipe_out.send([wid, 'DONE'])
        out={'Row count:': rcnt[wid], \
            'Empty:'    : empty, \
            'Partials:' : pacnt, \
            
            'batches'   : bid[wid], \
            'parts'     : partcnt[wid], \
            'NULs'      : null_cnt, \
            'in_file'   : file_evt['in_key'], \
            'out_file'  : file_evt['out_key'],
            'memory'    : f'{process.memory_info().rss/1024:7,.2f}', \
            'elapsed'   : f'{time.perf_counter() - s[wid]:0.2f}'
        }
        pfmtv(out,cols=[f'File:{wid}',''])
        for k, v in out.items():
            result_pipe_child.send([wid, k,v])
        result_pipe_child.send([wid,'DONE'])
        direct_pipe_out.close()
        print('CLEANER:exit')
def streamer( event, data_pipes, cleaners):
    num_of_cleaners = len(cleaners)
    
    if 1:
        
        file_evt= event['file']
        columns_to_clean=event['columns_to_clean']
        assert columns_to_clean
        lame_duck = int(file_evt['lame_duck'])
        bucket_name:str = event['bucket_name']
        in_key:str      = file_evt['in_key']
        obj = s3r.Object(
            bucket_name=bucket_name,
            key=in_key
        )
        total_gz=0
    process = psutil.Process(os.getpid())
    from itertools import cycle
    iter_wids = cycle(list(range(num_of_cleaners)))
    mem=[0 for x in range(num_of_cleaners+2)]
    partial=None
    for cid,s3_chunk in enumerate(obj.get()["Body"].iter_chunks(1<<16)):
        wid=next(iter_wids)
        if 1: 
            csv_chunk=dec[0].decompress(s3_chunk)
            pos=csv_chunk.rfind(b',\n')
            #pcnt= 100-pos/len(csv_chunk)*100
            #assert pcnt<1
            if not cid:
                partial=csv_chunk[pos+2:]
                data_pipes[wid][1].send((wid, cid, LF.compress(csv_chunk[:pos+2])))
            else:
                if csv_chunk:

                    data_pipes[wid][1].send((wid, cid, LF.compress(partial+csv_chunk[:pos+2])))
                    partial=csv_chunk[pos+2:]
                    #sc_count[wid] +=1
                else:
                    #print('s3_chunk:  ',s3_chunk)
                    #print('csv_chunk:  ',csv_chunk)
                    print(f'Chunk [{cid}] is empty.')
                    break
                    
        else:
            print('streamer: Cleaner is dead. breaking...')
            break
        if cid%10==0:
            mem[num_of_cleaners+1]=process.memory_info().rss
            #if cid>100: break
            if sum(mem)>10_000_000_000:
                raise Exception('Reached memory limit.')
        
    for wid in range(num_of_cleaners): #cleaner_p.is_alive():
        data_pipes[wid][1].send((wid,'DONE'))


def  get_wid():
    flag=True
    while True:
        yield 1 if flag else 0
        flag = not flag
#import dill


def lambda_handler(event, context):
    out=defaultdict(dict)
    cleaners:list = []
    processes:list = []
    data_pipes:dict={}
    direct_pipes:dict={}
    reversed_pipes:dict={}
    result_pipe_parent, result_pipe_child = Pipe()
    num_of_cleaners=3
    #manager = Manager()
    #x = manager.Value('i', 0)
    #x = Value('dict', dec)
    #y = Value('i', 0)

    
    for wid in range(num_of_cleaners): 
        data_pipes[wid]=[*Pipe()]
        direct_pipes[wid]=[*Pipe()]
        reversed_pipes[wid]=[*Pipe()]

    if 1:
        for wid in range(num_of_cleaners): 
            #conn, reversed_pipe, result_pipe_child
            s[wid]= time.perf_counter()
            process:Process = Process(target=cleaner, args=( wid, event, data_pipes[wid][0],  direct_pipes[wid][1], reversed_pipes[wid][0], result_pipe_child))
            cleaners.append(process)
    if 1:
        process:Process = Process(target=uploader, args=(event,direct_pipes,reversed_pipes))
        processes.append(process)        

    # start all processes
    for process in cleaners:
        process.daemon = True
        process.start()    
    for process in processes:
        process.daemon = True
        process.start()
    print(123)
    streamer(event, data_pipes, cleaners)
    # make sure that all processes have finished
    if 1:
        for pid, process in enumerate(cleaners):
            print(f'joining cleaner {pid}')
            process.join()    
        for pid, process in enumerate(processes):
            print(f'joining uploader {pid}')
            process.join()
        
    if 1:
        #get resulting counts
        done=[1 for _ in range(num_of_cleaners)]
        for i in range(10):
            if result_pipe_parent.poll():
                print('reading job results', i)
                data= result_pipe_parent.recv()
                if data[-1]=='DONE':
                    wid, _=data
                    done[wid]=0
                    if sum(done) == 0:
                        break
                else:
                    wid, k, v = data
                    out[wid][k]=v
            else:
                print('Waiting for job results...',i)
                time.sleep(1)
                
            if sum(done)==0: break
        process = psutil.Process(os.getpid())
        for wid in range(num_of_cleaners): 
            elapsed = time.perf_counter() - s[wid]
            print(f"f{wid}: elapsed {elapsed:0.2f} sec.")
    

    return out
            
def  get_wid():
    flag=True
    while True:
        yield 1 if flag else 0
        flag = not flag
#import dill

if __name__ == '__main__':
    
    ss= time.perf_counter()
    evt={"bucket_name":"test-data","columns_to_clean":["display_name", "test_title", "test_title_2"], 
        # "columns_to_clean":["channel_display_name", "video_title", "asset_title", "claim_id"],
        "file":
        {
         "in_key":"conversion_test/CLEAN/test_CLEAN.csv.gz",
         "out_key":"conversion_test/compressed_clean/compressed_clean_test.csv.gz",
         "lame_duck":0
        }
    }
    evt={"bucket_name":"test-data","columns_to_clean":["channel_display_name", "video_title", "asset_title"], 
        # "columns_to_clean":["channel_display_name", "video_title", "asset_title", "claim_id"],
        "file":
        {
         "in_key":"testz.csv.gz",
         "out_key":"conversion_test/compressed_clean/compressed_clean_test.csv.gz",
         "lame_duck":0
        }
    }
    if 1:
        result = lambda_handler(evt, {})
        pp(dict(result))
    process = psutil.Process(os.getpid())
    elapsed = time.perf_counter() - ss
    print(f"Main shell memory[{process.memory_info().rss/1024:7,.2f}], elapsed {elapsed:0.2f} sec")
  
