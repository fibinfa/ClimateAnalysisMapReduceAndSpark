

				
1 Climate Analysis in MapReduce:

Files:
Source Code Mapreduce program without combiner:
	src -> main-> java -> MR -> HW2 -> ClimateAnalysisV1.java

Source Code Mapreduce program with combiner:
	src -> main-> java -> MR -> HW2 -> ClimateAnalysisV2.java

Source Code Mapreduce program with InMapperCombiner:
	src -> main-> java -> MR -> HW2 -> ClimateAnalysisV3.java

Source Code Mapreduce program for SecondarySorting:
	src -> main-> java -> MR -> HW2 -> SecondarySorting.java


_________________________________________________________________________________________________________________________________

2 Local Execution using MakeFile

Steps to run java

   1. Go to makefiles folder and each version has a separate make file with proper name for each make file
	ClimateAnalysisV1, v2 and v3
   2. Create input directory and download 1991.csv file for Task1 and 1880.csv - 1889.csv for Task2
   3. Go to the directory and in the terminal Run the command
	> make alone 
   4. Check the output in the output directory

_________________________________________________________________________________________________________________________________

 Local Execution using Eclipse

Steps to run java

   1. Create a maven project.
   2. Create input directory and download 1991.csv file for Q1 and 1880.csv - 1889.csv for Q2
   4. Update the pom.xml files with dependencies and rebuild the maven project.
   5. Go to Run Configuration and provide the input file path and output filepath as argument.
   6. Run the program
   7. Check the output in the output directory
_________________________________________________________________________________________________________________________________


3 Climate Data Analysis AWS Execution

Steps to Run Program in cloud

   1. Update the make file with changes to input, output, s3bucketname, subnetid and job.name
   2. Mention the number of nodes required as 6
   3. Run these commands in the terminal :
		make-bucket
		upload-input-aws
		make cloud
   5. Check the output in the output directory mentioned in aws
   6. Check the log files in log folder of s3 bucket mentioned


_________________________________________________________________________________________________________________________________

4 Climate Analysis SCALA programs

Steps to execute:

	1. Source files are under the directory Scala Programs
	2. Run each version of .scala file as scala application in eclipse with proper command line argument for input path.
	3. The output will be stored in output path.

_________________________________________________________________________________________________________________________________


