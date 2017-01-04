-- finds all chains of length 2 in graph or subgraph

REGISTER s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader AS (line:chararray); 

ntriples = FOREACH raw GENERATE FLATTEN(myudfs.RDFSplit3(line)) AS (subject:chararray,predicate:chararray,object:chararray);

filtered_ntriples = FILTER ntriples BY (subject matches '.*rdfabout\\.com.*');

filtered_ntriples2 = FOREACH filtered_ntriples GENERATE * AS (subject2:chararray,predicate2:chararray,object2:chararray);

joined_ntriples = JOIN filtered_ntriples BY object , filtered_ntriples2 BY subject2 PARALLEL 50;

result = DISTINCT joined_ntriples PARALLEL 50;

STORE result INTO 's3n://path/to/desired/location';