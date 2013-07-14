register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- this file is used to plot the histogram of the data
-- load the test file into Pig
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);

raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- filter by subject matching a particular string
filtered_ntriples = FILTER ntriples by subject matches '.*rdfabout\\.com.*' PARALLEL 50;


-- creating a copy of these filtered records
copy_filtered_ntriples = foreach filtered_ntriples generate (subject, predicate, object) as (subject2:chararray, predicate2:chararray, object2:chararray);

-- joining the two copies on object - subject2
joined_result = JOIN filtered_ntriples by object, copy_filtered_ntriples by subject2 PARALLEL 50;

-- let us remove teh duplicates from the data
distinct_joined_result = DISTINCT joined_result;

-- store the results in the folder /user/hadoop/example-results
store distinct_joined_result into '/user/hadoop/example-results' using PigStorage();
-- Alternatively, you can store the results in S3, see instructions:
-- store count_by_object_ordered into 's3n://superman/example-results';
