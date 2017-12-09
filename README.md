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
ZIP archives, however, are not supported natively by Spark.
So to load those files but still save space on our hard drives we unzipped them and gzipped them instead, because GZIP is supported natively by Spark.

No preprocessing was necessary as the GDELT dataset is pretty clean

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
  - # articles, # sources, # mentions: one or more
  - Goldstein Scale: expresses whether the event will have a good impact (+10) or a bad impact (-10) to the country in question, with every value inbetween.
