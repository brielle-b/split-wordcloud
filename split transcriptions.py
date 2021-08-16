# Databricks notebook source
# Get all files from folder/S3 and take transcription and job name only
import os
import json
os.chdir("/dbfs/FileStore/import-stage/transcripts/")
text_dict = {}
for filename in os.listdir(os.getcwd()):
   with open(os.path.join(os.getcwd(), filename), 'r') as f:
      transcript_result = json.loads(f.read())
      job_name=transcript_result["jobName"][6:10] # unique name for each call conversation
      transcriptions = transcript_result["results"]["transcripts"]
      for t in transcriptions:
         text_dict[job_name]=t["transcript"]


# COMMAND ----------

# Remove all conversations shorter than 100 characters because we can't learn anything from those
clean_dict = {}
for i in text_dict.keys():
  if len(text_dict[i]) >100:
    clean_dict[i]= text_dict[i]
#print(len(clean_dict))

# COMMAND ----------

# split conversations by length that Amazon Comprehend can succeed
short = {}
for i in text_dict.keys():
  chunk,chunk_size = len(text_dict[i]),4900
  short[i]=[text_dict[i][n:n+chunk_size] for n in range(0, chunk, chunk_size) ]
  #print(len(short[i]))

# COMMAND ----------

# create individual text files by slices of conversations
save_path ="/dbfs/FileStore/import-stage/split_audio_texts/"
for i in short.keys():
  x = range(len(short[i]))
  for j in x:
    filename = i +"_"+str(j+1)
    text = short[i][j]
    completeName = os.path.join(save_path, filename+".txt") 
    file = open(completeName, 'w')
    file.write(text)
    file.close()

    

# COMMAND ----------

#download files or send directly back to S3 bucket
import urllib
import urllib.request
import webbrowser
os.chdir("/dbfs/FileStore/import-stage/split_audio_texts/")
base = 'https://tfsedp.cloud.databricks.com'

def build_url(base_url, path):
    # Returns a list in the structure of urlparse.ParseResult
    url_parts = list(urllib.parse.urlparse(base_url))
    url_parts[2] = path
    #url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)
  
for filename in os.listdir(os.getcwd()):
    path ="/files/import-stage/split_audio_texts/"+filename
   # open url in browser to download files

