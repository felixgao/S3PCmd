# S3P Command

S3 Parametrized Command is a subset of S3 command that supports basic date parametrization.

Many use of S3 is to produce/archieve data in the following format
S3://BUCKET_NAME/PATH/TO/DATA/YYYY-MM-DD/
or
S3://BUCKET_NAME/PATH/TO/YYYY-MM-DD_HH-MM-SS/DATA/

To operate on top these patterns this utility allows you to easily copy, move, remove those dataset by replacing the YYYY-MM-DD with {DATEID} or YYYY-MM-DD_HH-MM-SS with {DATETIMEID}.  

Additional simple arithemtics are allowed such as {DATEID-1} for previoues day or {DATEID+1} for next day.

Similiarly, the {DATETIMEID} also operate the same for previous day or next day computation.


