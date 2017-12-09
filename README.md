# Project 3: Deliverable II

https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-3.html

# Collaboration plan
## Team Members:
Mathieu Clement,
Anjani Bajaj,
Bhargavi Kommineni,
Surada Lerkpatomsak,
Neha Bandal

# Dataset 
## GDELT(The Global Database of Events, Language and Tone)
## Goal of the Project: Visualize, explore, and export the GDELT Event Database.
By quantitatively codifying human society’s events, dreams and fears,  we are planning to map happiness and conflict, provide insight to vulnerable populations, and even potentially forecast global conflict in ways that allow us as a society to come together to deescalate tensions, counter extremism.

**Instructor Comment**: This sounds pretty amazing, but is there something a bit more concrete you'll be doing? :-) Also, since you requested to have your group name changed, let me know what you'd like it changed to (although I think it's awesome as-is!!).

# Collecting and cleaning the data

To speed up the download process we grabbed ZIP files from the GDELT website.
There is one for every day of the years 2015 and 2016 which we are interested in.
ZIP archives, however, are not supported natively by Spark.
So to load those files but still save space on our hard drives we unzipped them and gzipped them instead, because GZIP is supported natively by Spark.

No preprocessing was otherwise necessary as the GDELT dataset is pretty clean.

The GDELT website also provides the header of the CSV file (i.e. the name of the columns separated by tab characters). We converted that to newline-separated values (though not really necessary), and suffixed every column but character-type column with a color and the type it should be parsed at, e.g. IntegerType.
We then created a StructType with StructFields.

Here is the code:

```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType, BooleanType

types = {
    'Float': lambda: FloatType(),
    'Integer': lambda: LongType(), # the spec does not distinguish between integer and long, so long it is
    'Long': lambda: LongType(),
    'Bool': lambda: IntegerType() # like in the NAM dataset, booleans are expressed as numbers (0 and 1)
}

feats = []
with open('CSV.header.txt') as header_file:
    for lineno, line in enumerate(header_file):
        line = line.strip()
        if ':' in line:
            feat_name, type_name = line.split(':')
            feats.append(StructField(feat_name, types[type_name](), True))
        else:
            feats.append(StructField(line, StringType(), True))
            
schema = StructType(feats)
```

# Info about features

The [GDELT file format](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf) (it's really a tab-separated text file) contains records related to events around the world. It uses the [CAMEO](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf) taxonomy to classify the organisms (government and non-government), actors (such as persons, multinational companies, governments), and actions (warn, appeal, decline to comment, accuse, protest, impose,  etc.)

Each event can be qualified using the following groups of attributes:

  - Event 
    * Date: actually the date of publication in the media, but 97 % of the news relate to the current day
    * Location: where an event took place, even when it involves two actors associated with other countries, e.g. an American citizen appealing against a British citizen in the court of Human Rights in Strasbourg, France.
    * Type, code, etc.: event classification
    * Statistics: # of mentions, # of sources, average tone \[positive or negative\], impact of the news on the stability of the country, 
  - Actor 1: the main party or one of the two main parties involved
  - Actor 2: the second party involved. In some cases, there is no second actor.
  - Source URL: Link to the article a record is extracted from
 
Most attribute contain character information such as country codes and actor type codes.
A few are related to dates (year, month-year, SQL date...)
And then there are the numeric ones:

  - Event location geocoordinates
  - AvgTone, as explained above, which is defined between -100 for an extremely negative event to +100 for an extremely positive event. A small riot will have a slightly low average tone, where as the end of a war would get an extremely positive tone.
  - Number of articles, sources, and mentions: one or more
  - Goldstein Scale: expresses whether the event will have a good impact (+10) or a bad impact (-10) to the country in question, with every value inbetween.
  - QuadClass: which of the 4 main classification groups applies (Verbal/Material Cooperation/Conflict)

# Analysis

We played the role of the professor a little bit and imagined a couple of questions:

## What are the most discussed topics implicating both the United States and Switzerland?

To answer this question, we simply filtered records by looking at the attributes *Actor1CountryCode* and *Actor2CountryCode*, and then sorted by "number of mentions":

```python
def either_country_code(row, country1, country2):
    return (row.Actor1CountryCode == country1 and row.Actor2CountryCode == country2) or\
           (row.Actor1CountryCode == country2 and row.Actor2CountryCode == country1)

df\
    .rdd\
    .filter(lambda row: either_country_code(row, 'CHE', 'USA'))\
    .takeOrdered(10, key= lambda row: -row.NumMentions)
```

We had to run this job multiple times because we used the wrong code for countries. For some strange reasons the GDELT data format uses two character country codes in some cases, and three character country codes (which they define in the CAMEO standard) in others. Defining a new standard for country codes seems a bit weird, but they must have their reasons...

`takeOrdered` takes two arguments:

  - number of records to "take"
  - the key used for sorting. In this we use "NumMentions", but because we want reverse ordering we negate the attribute. 

The results:

|Number of mentions|Date|Event location|Event description|Source|
|---|---|---|---|---|
|565|August 14, 2016|Sennwald, Switzerland|Swiss train attack suspect, female victim die of wounds|[Source](https://www.seattletimes.com/nation-world/swiss-police-no-indication-of-terrorism-in-train-attack/)|


Wait, but we asked for 10 results!
And we got those. Unfortunately the first 9 resolved to a "Page Not Found." It seems that news agencies don't keep stuff online or they changed their website architecture since then.
In an application where inaccessible sources render records useless, it would be a good idea to periodically check that links do resolve to articles that are still up. **This 10 % rate that we observed is a significant concern.**

The web archive helped a bit. For instance there was one article published on SFGate talking about both the United Nations (a SF founded institution with headquarters in NYC) and a program of UNICEF in Switzerland.

Another article explains how surfers found some awesome waves after a hurricane hit in Hawaii. There doesn't seem to be any link to Switzerland. Further more the US "location" in this article is Big Island, New York. This shows that the dataset is not 100 % accurate. => We exclude the possibility that the web archive saved the wrong content for that URL because there is come correlation with the article and other attributes.

## Compare the news coverage of Donald J. Trump vs Hillary Clinton

## 2015 was the year of [The Dress](https://en.wikipedia.org/wiki/The_dress). Find out how that affected the stability of countries.
