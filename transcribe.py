#python3
import boto3
import botocore
import json
import requests
import time
import os.path
import sys
import getopt
import argparse
from pathlib import Path
# pip3 install pydub --user
from pydub import AudioSegment
from datetime import datetime

#transcribe_client = boto3.Session().client(service_name='transcribe')
transcribe_client = boto3.client('transcribe', 'ap-southeast-2')

region = 'ap-southeast-2'
s3bucket = 'djenny-appservices-demos'
s3directory = 'transcribe'
content_dir = 'content'
speakers_max = 2
custom_vocab = None
language_code = 'en-AU'

help_msg = "This program uses Amazon Transcribe to convert an audio file to text"
parser = argparse.ArgumentParser(description = help_msg)
parser.add_argument("input_file", type=str, help="audio input filename")
parser.add_argument("-V", "--verbose", help="run in verbose output mode", action="store_true")
args=parser.parse_args()

input_file = args.input_file

if args.verbose:
    print("File to scan: ",input_file)

base_dir = os.path.dirname(input_file)
base_filename = Path(input_file).stem
file_ext = os.path.splitext(input_file)[1]
file_ext_final = file_ext.replace('.', '')
new_filename=os.path.join(base_dir,base_filename) + '.wav'
key_t=os.path.join(s3directory, new_filename)

if args.verbose:
    print("base_dir: ",base_dir)
    print("base_filename: ",base_filename)
    print("file_ext_final: ",file_ext_final)
    print("new_filename: ",new_filename)
    print("key_t: ",key_t)

# If not wav, convert now
if (file_ext_final != 'wav'):
   print("Converting " + input_file + " to wav format")
   try:
      track = AudioSegment.from_file(input_file, file_ext_final)
      file_handle = track.export(new_filename, format='wav')
   except:
      print("ERROR: Could not convert " + str(filepath) + " to wav format")
else:
   print("Input file is already wav format. Proceeding directly to transcription")

# Cut out 3 second segments from audio file

# Define job parameters
now=datetime.now()
job_name = "transcribe_"+base_filename+"_"+now.strftime("%Y%m%d_%H%M%S")
job_uri = "https://s3-ap-southeast-2.amazonaws.com/"+s3bucket+"/"+s3directory+"/"+new_filename

if args.verbose:
    print("Amazon Transcribe job name: " + job_name)
    print("Uploading input file to: " + job_uri)

# Ensure the video is uploaded to the S3 bucket
s3_resource = boto3.resource('s3','ap-southeast-2')
try:
    s3_resource.Object(s3bucket, key_t).load()
    if args.verbose:
        print("INFO: Source file already exists in S3 bucket, delete it first if you wish to replace the file")
except botocore.exceptions.ClientError:
    if args.verbose:
        print("Uploading source file to S3 bucket")
    s3_resource.Bucket(s3bucket).upload_file(Filename=new_filename, Key=key_t)

# Start transcription job
if args.verbose:
    print("Starting transcription job")

settings = {}
if (speakers_max > 1):
    settings["ShowSpeakerLabels"]=True
    settings["MaxSpeakerLabels"]=speakers_max

if (custom_vocab != None):
    settings["VocabularyName"]=custom_vocab

if args.verbose:
    print("Settings: ", settings)

transcribe_client.start_transcription_job(
    TranscriptionJobName=job_name,
    Media={'MediaFileUri': job_uri},
    MediaFormat='wav',
    LanguageCode=language_code,
    Settings=settings
)

# Poll for job completion
while True:
    if args.verbose:
        sys.stdout.write("."); sys.stdout.flush()
    status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
        break
    time.sleep(5)

# Retrieve the output from the signed URL returned

if args.verbose:
    print("Retrieving transcription output")
s3_url=status['TranscriptionJob']['Transcript']['TranscriptFileUri']
response=requests.get(s3_url)
if args.verbose:
    print(response)
response_json=json.loads(response.content)
if args.verbose:
    print(response_json)
with open('data.txt', 'w') as outfile:
    json.dump(response_json, outfile)

# Extract raw transcription
transcription=response_json['results']['transcripts'][0]['transcript']
print("***************************")
print("*    Raw Transcription    *")
print("***************************")
print(transcription)
print()

if (speakers_max > 1):
    speaker_labels=response_json['results']['speaker_labels']
    items=response_json['results']['items']

    if args.verbose:
        print("***************************")
        print("*     Speaker Labels      *")
        print("***************************")
        print(speaker_labels)
        print()
        print("***************************")
        print("*          Items          *")
        print("***************************")
        print(items)
        print()

    segments = [segment for segment in speaker_labels['segments']]

    timestamps=[]
    current_speaker=""
    item_count=0
    items_length=len(items)

    speaker_transcription=""
    for segment in segments:
        end_of_segment=False
        speaker_label = segment['speaker_label']
        speaker_transcription = speaker_transcription + speaker_label + ":  "
        start_time = float(segment['start_time'])
        end_time = float(segment['end_time'])
        sub_segments = [sub_segment for sub_segment in segment['items']]
        while not(end_of_segment) and (item_count < items_length):
            current_item=items[item_count]
            if (current_item['type'] == 'pronunciation'):
                if (float(current_item['end_time']) <= end_time):
                    speaker_transcription = speaker_transcription + current_item['alternatives'][0]['content'] + " "
                else:
                    end_of_segment=True
            else:
                speaker_transcription = speaker_transcription + "\b" + current_item['alternatives'][0]['content'] + " "
            if not(end_of_segment):
                item_count = item_count + 1
        speaker_transcription = speaker_transcription + "\n"

    print(speaker_transcription)
