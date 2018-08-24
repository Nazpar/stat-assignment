 Where to start
----------------
The code runs on Ubuntu 16.04.5. For Mac, please find the equivalent commands. Two files are provided. One is a python file  and another one is a Jupyter Notebook file for running code blocks and it can be opened in Jypter Notebook. Both files need Spark and python 2.7+ to be installed.  Due to the volume of input data Spark has been chosen to handle data processing and analysis.
Dependecies and installations are provided below.

### Python installation

```
sudo apt-get install python
pip install jupyter
```

### Installing required python packages
```
sudo pip install numpy
sudo pip install pandas
sudo pip install pyspark
sudo pip install matplotlib
```

### Spark installation
* Download Spark from https://spark.apache.org/downloads.html: spark-2.3.1-bin-hadoop2.7.tgz
* Run `tar xvf spark-2.3.1-bin-hadoop2.7.tgz`
* Go to installation folder and run `mv spark-1.3.1-bin-hadoop2.6 /usr/local/spark`
* Add this line to ~/.bashrc `export PATH=$PATH:/usr/local/spark/bin`
* Run`source ~/.bashrc`
* Verify Spark installation by running `spark-shell`

How to run the code
---------------------
### Method 1
To run the python file, first run `python assignment.py etl`. <br />
Use 'etl' as the argument. This steps simply downloads the S3 file and convert it to Parquet file. The step is required to run before any other steps.<br />
To see the solution for each problem: <br />
Run `python assignment q1` or `python assignment q2` or `python assignment q3` <br />
To see the solutions to all questions: <br />
Run `python assignment q123` <br />
Note: question 3 will generate a png file in the current directory.

### Method 2
To run the code in interactive mode:
* Run Jupyter Notebook
* Open the assignment.ipynb
* Run the blocks from beginning

Assumptions
------------------------
* Assignment code downloads the file from S3 bucket. This downlownd can be avoided using AWS service like Glue to automatically convert the file to Parquet files and store them in S3.
* Due to the data volume mentioned, data lake can be in S3 and different partitioning is needed. Either based on keyword or timestamp or location. For this assigment one month data was already provided so I did not partition data any further.
* Depending on some factors like query performance, application of data and cost, different solutions can be used. For instance if this data needs to be fed to a reporting tool, database can be used to host some portition of data. For this assignment, data analysis was the main focus. That is why the files were only converted to Parquet files. 
* For question 3, to calculate the similarity score and understand if mobile and desktop search results diverge or converge over the one month period, I simply calculated the absolute difference of rank differences between mobile and desktop ranking for the common URLs. I got an average over the one month period. However, better similarity measures could be used to provide more accurate result such as Spearmanr or Pearson methods. The result indicates that at the beginning of month desktop and mobile ranking were not similar as the difference is high. Then the results converges and again by the end of the month it diverges.













