#!/usr/bin/env python3

import re, os, sys, num2words


tensMap = {
    "10":"tens","20":"twenties","30":"thirties","40":"forties","50":"fifties","60":"sixties","70":"seventies","80":"eighties","90":"nineties"
}
yearMap = {
    "1900":"nineteen hundred","2000":"two thousand","2001":"two thousand and one","2002":"two thousand and two","2003":"two thousand and three","2004":"two thousand and four","2005":"two thousand and five","2006":"two thousand and six","2007":"two thousand and seven","2008":"two thousand and eight","2009":"two thousand and nine"
}
abbrevDict = {
	"Mr.":"Mr","Dr.":"Dr","Ms.":"Ms","Mrs.":"Mrs","Jr.":"Jr","Sr.":"Sr","St.":"St","Ft.":"Ft","Mt.":"Mt"
}
wordlist = ["uh", "um", "hm", "mmm", "mm", "eh", "hmm"]


filename = sys.argv[1]
with open(filename, "r") as f:
  text = f.read()
  
#test on individual file
"""
filename = "cleantest.txt"
with open(filename, "r") as f:
  text = f.read()
"""

#abbreviation and acronym transformations
for i in abbrevDict:
	text = text.replace(i, abbrevDict[i])
text = re.sub(r"(?<!\w)([A-Za-z])\.", r"\1", text, flags=re.IGNORECASE)


#whole word disfluency removal
remove = "|".join(wordlist)
regex = re.compile(r"\b("+remove+r")\b", flags=re.IGNORECASE)
text = re.sub(regex, "", text)


#date handling
yearList = re.findall(r"\d{4}", text)
for i in yearList:
    if int(i) > 1909 and int(i) < 2100:
        try:
            text = text.replace(i,yearMap[i])
        except:
            text = text.replace(i,num2words.num2words(i[0:2]) + " " + num2words.num2words(i[2:4]))


#tens handling
tensList = re.findall(r"[1-9]0's|[1-9]0s",text)
for i in tensList:
    numList = re.findall(r"\d+",i)
    text = text.replace(i,tensMap[numList[0]]) 

#punctuation and line conditioners
text = text.replace(".", " . \n")
text = text.replace("?", " ? \n")
text = text.replace("!", " ! \n")
text = re.sub(r" {1,}"," ", text)
text = text.replace("\n ","\n")
text = re.sub(r"(\n)+","\n", text)
text = re.sub(r"\n\.( )*\n","", text)
text = re.sub(r" \. \ncom", ".com", text)
text = re.sub(r"( )+", " ", text)
text = re.sub(r"[,\"]\n", r" . \n", text)


with open(filename, "w") as f:
  	f.write(text)
