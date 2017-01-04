-- computes histogram of subject frequency

REGISTER s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader AS (line:chararray); 

ntriples = FOREACH raw GENERATE FLATTEN(myudfs.RDFSplit3(line)) AS (subject:chararray,predicate:chararray,object:chararray);

subjects = GROUP ntriples BY (subject) PARALLEL 50;

count_by_subject = FOREACH subjects GENERATE flatten($0), COUNT($1) AS count PARALLEL 50;

histogram = GROUP count_by_subject BY (count) PARALLEL 50;


STORE histogram INTO 's3n://path/to/desired/location';