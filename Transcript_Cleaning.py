import json, re, num2words, sys, os
from datetime import time, timedelta, datetime

yearMap = {
    "1900":"nineteen hundred","2000":"two thousand","2001":"two thousand and one","2002":"two thousand and two","2003":"two thousand and three","2004":"two thousand and four","2005":"two thousand and five","2006":"two thousand and six","2007":"two thousand and seven","2008":"two thousand and eight","2009":"two thousand and nine"
}

contractionMap = { 
    "ain't": "am not / are not / is not / has not / have not","aren't": "are not / am not","can't": "cannot","can't've": "cannot have","'cause": "because","could've": "could have","couldn't": "could not","couldn't've": "could not have","didn't": "did not","doesn't": "does not","don't": "do not","hadn't": "had not","hadn't've": "had not have","hasn't": "has not","haven't": "have not","he'd": "he had / he would","he'd've": "he would have","he'll": "he shall / he will","he'll've": "he shall have / he will have","he's": "he has / he is","how'd": "how did","how'd'y": "how do you","how'll": "how will","how's": "how has / how is / how does","I'd": "I had / I would","I'd've": "I would have","I'll": "I shall / I will","I'll've": "I shall have / I will have","I'm": "I am","I've": "I have","isn't": "is not","it'd": "it had / it would","it'd've": "it would have","it'll": "it shall / it will","it'll've": "it shall have / it will have","it's": "it has / it is","let's": "let us","ma'am": "madam","mayn't": "may not","might've": "might have","mightn't": "might not","mightn't've": "might not have","must've": "must have","mustn't": "must not","mustn't've": "must not have","needn't": "need not","needn't've": "need not have","o'clock": "of the clock","oughtn't": "ought not","oughtn't've": "ought not have","shan't": "shall not","sha'n't": "shall not","shan't've": "shall not have","she'd": "she had / she would","she'd've": "she would have","she'll": "she shall / she will","she'll've": "she shall have / she will have","she's": "she has / she is","should've": "should have","shouldn't": "should not","shouldn't've": "should not have","so've": "so have","so's": "so as / so is","that'd": "that would / that had","that'd've": "that would have","that's": "that has / that is","there'd": "there had / there would","there'd've": "there would have","there's": "there has / there is","they'd": "they had / they would","they'd've": "they would have","they'll": "they shall / they will","they'll've": "they shall have / they will have","they're": "they are","they've": "they have","to've": "to have","wasn't": "was not","we'd": "we had / we would","we'd've": "we would have","we'll": "we will","we'll've": "we will have","we're": "we are","we've": "we have","weren't": "were not","what'll": "what shall / what will","what'll've": "what shall have / what will have","what're": "what are","what's": "what has / what is","what've": "what have","when's": "when has / when is","when've": "when have","where'd": "where did","where's": "where has / where is","where've": "where have","who'll": "who shall / who will","who'll've": "who shall have / who will have","who's": "who has / who is","who've": "who have","why's": "why has / why is","why've": "why have","will've": "will have","won't": "will not","won't've": "will not have","would've": "would have","wouldn't": "would not","wouldn't've": "would not have","y'all": "you all","y'all'd": "you all would","y'all'd've": "you all would have","y'all're": "you all are","y'all've": "you all have","you'd": "you had / you would","you'd've": "you would have","you'll": "you shall / you will","you'll've": "you shall have / you will have","you're": "you are","you've": "you have"
}
tensMap = {
    "10":"tens","20":"twenties","30":"thirties","40":"forties","50":"fifties","60":"sixties","70":"seventies","80":"eighties","90":"nineties"
}

#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
# Constants for IBM COS values
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
#COS_API_KEY = ""
#COS_CRN_RID = ""
#COS_ENDPOINT = ""
DEFAULT_PREFIX_TRANSCRIBE = 'tranresult/' # incoming transcripts
DEFAULT_PREFIX_TEST = 'cleansed/test/' # clean transcripts for test sets
DEFAULT_PREFIX_TRAIN = 'cleansed/train/' # clean transcripts for training sets
DEFAULT_PREFIX_AUDIO_EXPORT = 'transcribe/' # not used, but here for clarity
DEFAULT_SPLIT_INTERVAL_MINUTES = 5 # todo read this from parameter

PREFIX_TEST = os.getenv("PREFIX_TEST",DEFAULT_PREFIX_TEST)
PREFIX_TRAIN = os.getenv("PREFIX_TRAIN",DEFAULT_PREFIX_TRAIN)
PREFIX_TRANSCRIBE = os.getenv("PREFIX_TRANSCRIBE",DEFAULT_PREFIX_TRANSCRIBE)
PREFIX_EXPORT = os.getenv("PREFIX_EXPORT",DEFAULT_PREFIX_AUDIO_EXPORT)

SPLIT_INTERVAL_MINUTES = int(os.getenv("SPLIT_INTERVAL_MINUTES",DEFAULT_SPLIT_INTERVAL_MINUTES))

def main(event_trigger, fio):
    print("=======Main======")

    try:
        arr = parseTrigger(event_trigger)
    except:
        print("Failed to parse params " + str(sys.exc_info()[1]))
        return json.loads('{ "statusCode": 400, "body": "Unable to parse trigger parameters."}')

    if len(arr) < 4:
        print("Ignoring this trigger.")
        return json.loads('{"statusCode": 200, "body": "Cleaner was not run." }')

#COS_API_KEY = arr['key']
#COS_CRN_RID = arr['rid']
#COS_ENDPOINT = arr['url']
#print("Resources ID: " + arr['rid'])

    try:
        fio.prepareStore(arr['key'], arr['rid'], COS_AUTH_ENDPOINT, arr['url'])
    except:
        print("Failed to create cos " + str(sys.exc_info()[1]))
        return json.loads('{ "statusCode": 400, "body": "Unable to create COS."}')

# file list is a dictionary, 
    content = fio.readTranscription(arr['bkt'], arr['fnt'])
    fileList = splitTranscription(content)
    splitList = []

    i=1
    for a_file in fileList.items():
        if i < len(fileList):
            splitList.append(float(a_file[0]))
        fn = arr['fnp'] + "-" + str(i) + ".txt"
        print("file " + fn)
        i = i + 1
        fTrain = cleanTranscriptionForTraining(a_file[1])
        fTest = cleanTranscriptionForTestSet(fTrain)
        fio.writeTranscription(arr['bkt'], PREFIX_TRAIN, fn, fTrain)
        fio.writeTranscription(arr['bkt'], PREFIX_TEST, fn, fTest)

    if len(splitList) > 0:
        splitsFn = arr['fnp'] + ".json"
        fio.writeSplitFile(arr['bkt'], PREFIX_EXPORT, splitsFn, json.dumps(dict({"splits":splitList})))

# might re-purpose to cleanup.
#cleanUpS3Buckets(arr[0])

    return json.loads('{ "statusCode": 200, "body": "cleaner was run." }')

def parseTrigger(event):
    print("=======Parse the Trigger to get File Names======")

#print("event " + json.dumps(event))
#print(list(event))

    arr = dict()

    if (not event['key'].startswith(PREFIX_TRANSCRIBE)) or event['key'].endswith("/") or event['operation'] != 'Object:Write':
        return arr

    creds = event['__cos_creds']
    arr['key'] = creds['apikey']
    arr['rid'] = creds['resource_instance_id']

    endpoint = 'https://' + event['endpoint']
    print("Endpoint: " + endpoint)
    arr['url'] = endpoint

    bucket_name = event['bucket']
    clip = len(PREFIX_TRANSCRIBE)
#print(PREFIX_TRANSCRIBE + " clip start: " + str(clip))
    file_name = event['key'][clip:-4]

    print("Bucket Name: " + bucket_name)
    print("Filename: " + file_name)

    arr['bkt'] = bucket_name
    arr['fnp'] = file_name
    arr['fnt'] = event['key']

    return arr

def splitTranscription(content):
    lines = content.split("\n")
    lineBuffer = []
    fileDict = dict()

    index = 1 # number of splits, starting with 1
    tick = timedelta(minutes=SPLIT_INTERVAL_MINUTES)
    d = datetime.now()
    mark = d.combine(d, time())
    startTs = d.combine(d, time())
    mark = mark + tick
#pattern = '\[' + mark.time().isoformat() + '\]'
    pattern = '\[[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\]'
#print("trn " + pattern)
    for line in lines:
        start = 0 # start position of match (if any)
        end = 0 # end position of match (if any)
        match = re.search(pattern, line)
        if match:
#print(line)
            timestamp = line[match.start()+1:match.end()-1]
#print("ts " +timestamp)
            ts = d.combine(d, time.fromisoformat(timestamp))
            if ts.time() >= mark.time():
                splitSecond = (ts - startTs).total_seconds()
#print("Seconds: " + str(splitSecond))
                strdex = str(splitSecond)
#setup for next loop
                mark = ts + tick
# split the file
                lineBuffer.append(line[:match.start()])
                fileDict[strdex] = lineBuffer.copy()
                lineBuffer.clear()
                lineBuffer.append(line[(match.end() + 1):] + "\n")
#print("Split Transcription file " + strdex + "added.")
                index += 1
        else:
            lineBuffer.append(line)

    if len(lineBuffer) > 0:
#print("Last segment")
        fileDict[str(index)] = lineBuffer.copy()
    if len(fileDict) == 0:
        print("No time slices found!")
        fileDict[str(index)] = lines

    return fileDict

def cleanTranscriptionForTraining(content):
    
    speakerList = set() # todo, somehting with this
    parsedStr = ""

    for line in content:

        speaker = line.split(":   ")
        if len(speaker) > 1:
            person = speaker[0]
            speakerList.add(person)
            line = line.replace(person+":", " ")

        numtonum = re.findall(r'\d+-\d+', line)    
        for num in numtonum:
            line = line.replace(num, num.split("-")[0] + " to " + num.split("-")[1])

        line = re.sub(r'\[(.*?)\]', '', line)
        line = re.sub(r'\((.*?)\)', '', line)
        line = re.sub(r'[A-Za-z0-9]*-\n+', '', line)
        line = re.sub(r'[A-Za-z0-9]*-\W*', '', line)

        line = line.strip()
        line = line.replace("...","")

        line = re.sub(r' {2,}', ' ', line)

        parsedStr = parsedStr + timeClean(apostropheClean(tensClean(yearClean(percentClean(ordinalClean(line)))))) + "\n"

    parsedStr = parsedStr.replace("\n\n", "\n")
    return parsedStr

def cleanTranscriptionForTestSet(content_ref):
    print("=======Clean up Transcription for TestSet======")
    content_ref = content_ref.replace("\n", " ")
    lineSplit_ref = content_ref.split("\n")
    trim_content_ref = ""
    for lines in lineSplit_ref:
        trim_content_ref = trim_content_ref + re.sub(r' {2,}', ' ', lines)
    return trim_content_ref
    
def cleanUpS3Buckets(bucket_name, audio_name):
    print("=======Transitioning to different S3 for the final product======")
    copyAudio = audio_name.replace("+", " ")
    print("About to copy over: " + copyAudio)
    try:
        copy_source = {
            'Bucket': 'ce-rev-media',
            'Key': copyAudio
        }
        cos.meta.client.copy(copy_source, 'ce-rev-completed-media', audio_name)
        cos.Object('ce-rev-media', copyAudio).delete()
        cos.Object('ce-rev-pending-media', copyAudio).delete()
        print("Copying/Deleting " + copyAudio + " from ce-rev-media AND ce-rev-pending-media cos buckets to ce-rev-completed-media.")
    except:
        print("Unknown error has occured while deleting and copying.")

def ordinalClean(words):
    ordinalList = re.findall(r"\d+th|\d+st|\d+nd|\d+rd",words)
    for i in ordinalList:
        numList = re.findall(r"\d+",i)
        words = words.replace(i,num2words.num2words(numList[0], ordinal=True))
    return words

def percentClean(words):
    percentList = re.findall(r"\d+%",words)
    for i in percentList:
        numList = re.findall(r"\d+",i)
        words = words.replace(i,num2words.num2words(numList[0]) + " percent")
    return words
    
def yearClean(words):
    yearList = re.findall(r"\d{4}",words)
    for i in yearList:
        if int(i) > 1909 and int(i) < 2100:
            try:
                words = words.replace(i,yearMap[i])
            except:
                words = words.replace(i,num2words.num2words(i[0:2]) + " " + num2words.num2words(i[2:4]))
    return words

def tensClean(words):
    tensList = re.findall(r"[1-9]0's|[1-9]0s",words)
    for i in tensList:
        numList = re.findall(r"\d+",i)
        words = words.replace(i,tensMap[numList[0]])
    return words

def apostropheClean(words):
    apostropheList = re.findall(r"\d+'\w+|\w+'\w+",words)
    for i in apostropheList:
        z=""
        try:
            z = contractionMap[i]
        except:
            z = i.replace("\'s", "")
            words = words.replace(i,z)
    return words

def timeClean(words):
    timeList = re.findall(r"\d{1,2}:\d{2}",words)
    for i in timeList:
        split = i.split(":")
        if (split[1] == "00"):
            words = words.replace(i,num2words.num2words(split[0])+ " o'clock")
        else:
            if (split[0] == "00" or split[0] == "0"):
                split[0] = "12"
            words = words.replace(i,num2words.num2words(split[0])+ " " + num2words.num2words(split[1]))
    return words