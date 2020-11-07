# Big-Data

Assignment-2: Manipulating data from MongoDB using Spark SQL


In this assignment, you need to follow the above steps to perform the following tasks using scala:

    read the json-formated tweets in the attached file and use MongoSpark library to insert them into mongoDB database in a collection called 'tweets'
    The timestamp associated with each tweet is to be stored as a Date object, where the timestamp field is to be indexed.
    Also, the geo-coordinates of tweets should be indexed properly to ensure a fast spatial-based retrieval
    calculate the number of occurrences of word w published within a circular region of raduis (r), having a central point of (lon, lat), mentioned in tweets published during the time interval (start, end). Perform this operation by two ways:
        using MongoSpark, by collecting tweets and filtering them spatio-temporally using dataframe apis.
        using mongodb library by sending a normal mongoDB query to filter by time and space.
        Text indexing is optional
    Run the application as follows:
