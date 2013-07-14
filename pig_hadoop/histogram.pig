register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- this file is used to plot the histogram of the data
-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);

--raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- flatten the subjets out (because group by produces a tuple of each subject
-- in the first column, and we want each subject to be a string, not a tuple),
-- and count the number of tuples associated with each subject
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count PARALLEL 50;

-- we now have each subject and the count of tuples it appears in
-- we now group by this count and count the number of subjects that have the same count
-- the first count will be the x -coordinate and teh second count will be the y coordinate 
-- that is, there will be y subjects that have x tuples associated with them

group_by_count_of_subject = group count_by_subject by (count) PARALLEL 50;

-- flatten the counts out (because group by produces a tuple of each count
-- in the first column, and we want each count to be a number, not a tuple),
-- and count the number of tuples associated with each count
histo_data = foreach group_by_count_of_subject generate flatten($0), COUNT($1) as y-coordinate PARALLEL 50;

-- we nopw have the histo data ready that can be plotted in matlab or R or Tableau 

-- store the results in the folder /user/hadoop/example-results
store histo_data into '/user/hadoop/example-results' using PigStorage();
-- Alternatively, you can store the results in S3, see instructions:
-- store count_by_object_ordered into 's3n://superman/example-results';
