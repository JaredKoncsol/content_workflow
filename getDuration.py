#!/usr/bin/env python3

import librosa, sys, re, json

filename = sys.argv[1]

newDuration = 0
newDuration += librosa.get_duration(filename=filename)

newFilename = filename.replace(".wav", ".json")
	
with open(newFilename, "r") as jsonFile:
  data = json.load(jsonFile)
  
tmp = data["audioLength"]
data["audioLength"] = newDuration

with open(newFilename, "w") as jsonFile:
	json.dump(data, jsonFile)