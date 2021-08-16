# Databricks notebook source
# take transcriptions of conversations from S3
import os
import json
os.chdir("/dbfs/FileStore/import-stage/transcripts/")
text_list = [] 
for filename in os.listdir(os.getcwd()):
   with open(os.path.join(os.getcwd(), filename), 'r') as f:
      transcript_result = json.loads(f.read())
      transcriptions = transcript_result["results"]["transcripts"]
      for t in transcriptions:
        if len(t['transcript']) >100:
          text_list.append(t["transcript"])



# COMMAND ----------

# import libraries used to create wordcloud graphics and pre-process text and remove irrelevent words from conversations
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from text_preprocessing import preprocess_text
from text_preprocessing import to_lower, remove_email, remove_url, remove_punctuation, lemmatize_word
from gensim.parsing.preprocessing import remove_stopwords
# random words that appear frequently
OTHERS=['okay','case','email','send','unit','storage','sent','talking','service','support','display', 'week','thermal','friday','maybe','quote','serial','going','know','yeah','no','alright','thing','thats','gonna','need','want','thank','look','number','mean','said','sure','think','right','people','dispatch','second','phone','looking','silence','list','pull','check','price','thursday','refrigerator','teeth','information','level','speak','warranty','thought','company','showing','party','time','order','hold','come','ahead','board','actually','calling','moment']


# COMMAND ----------

# create wordcloud for each conversation from S3 folder individually
for i in text_list:
  preprocessed_text = preprocess_text(i.lower())
  processed = ''.join((x for x in preprocessed_text if not x.isdigit()))
  filtered_sentence = remove_stopwords(processed)
  filtered = filtered_sentence.split()
  final = [word for word in filtered if (len(word)>3 and word not in OTHERS)]
  new= " ".join(final)
  word_cloud=WordCloud(collocations=False).generate(new)
  plt.imshow(word_cloud, interpolation='bilinear')
  plt.axis("off")
  plt.show()

# COMMAND ----------

# Wordcloud for all conversations combined
whole_str = " ".join(text_list)
preprocessed_text = preprocess_text(whole_str.lower())
processed = ''.join((x for x in preprocessed_text if not x.isdigit()))
filtered_sentence = remove_stopwords(processed)
filtered = filtered_sentence.split()
final = [word for word in filtered if (len(word)>3 and word not in OTHERS)]
new= " ".join(final)
word_cloud=WordCloud(collocations=False).generate(new)
plt.imshow(word_cloud, interpolation='bilinear')
plt.axis("off")
plt.show()
