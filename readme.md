### Overview

1. Built a data lake
2. Wrote a job which submits files to the processor
3. Performed data quality checks, aggregations and stored the results

#### Instructions to run the pipeline

1. Use a multicore machine to take advantage of multiprocessing
2. Setup a virtual environment in python
3. Set up the data lake using `setup_data_lake.py`. It should take a couple of hours
4. After it is done, run the `submit_jobs.py`. This should take an hour or two. 
5. Run the `analysis.ipynb` notebook to view the results

#### Pipeline Design

I modelled the data lake like how I would design it in a real world.
I wrote a script which simulates the directory structure of a real data lake
It is mentioned that the data is ingested every 10 minutes.Therefore, I created a directory structure 
of `day/hour/window/`. This will also help partition the data easily

Now, that the data lake is ready. I wrote a script which simulates the next stage of the 
pipeline which is submitting the events/triggering a lambda to process the data. I chose to batch the raw files 
into batches of 6 files each and passed them to the processor for transformation.

The final step is done by the processor. The processor picks the files of the batch from the data lake, 
performs some data quality checks and starts processing parallely across multiple CPU cores with multiprocessing.
This will help us scale the pipeline across multiple devices and events. The core work of the processor is to
clean the data and and perform aggregations. In practice, the cleaned data files will be 
loaded into a data-warehouse. This will be the Enterprise Dataset.

The aggregations performed here are mean, standard deviation, last value, minimum, maximum etc.
The aggregations are performed for multiple variables present in the data. 
And since we do not know how many variables there can be, I have decided to use parquet files to 
store each file data and consolidate them at the end joining all the individual variable column metrics
The results are stored in a csv. In real-world, this would be a table.

Since I am very comfortable with lambdas and containers, I have packaged the whole
pipeline into a docker container. The entry point of the lambda will be a file_url

#### Data Quality Checks

All the data quality checks are performed before processing the data. 
Some of the key metrics I measured are volume_of_the_data(to identify when , percentage of bad data, measuring and fixing invalid data types, 
removing rows with must-have identifiers like DeviceId, Variable Name and sxo on
I stored all of them in a csv format to be able to query on them for later analysis to fix the pipeline or source data.

#### Data Type Validations
I have type-casted all the values in the Value column to float and removed all the rows with NaN's. In this I can be sure that all the values in Value Column are of numeric data.
Similarly, I have type-casted the timestamp column to pd.Timestamp and removed all the NaT values. 
I have added a check on Device and Variable columns making them mandatory for any row.
Also, The device value has to be of the six Devices, else the row will be dropped

#### Handling nulls
I chose not to remove rows with values missing because they can be a good indicator for data-analysis.
For example, we can find out patterns where data is missing.
This is a time series sensor data, so for this type of data, i am interpolating basing on the mean of above and below values (rolling average)
In occasional places, I have also removed null rows as well since the sensor won't fire every second.

The raw results of the transformations are stored in analysed_data folder. 
But you can see them loaded in the analysis.ipynb notebook along with the plots

#### Potential Enhancements (not picked due to Time Constraint)
1. Perform analysis on data-quality checks table
2. Perform analysis on aggregated results
3. Store the clean layer in a db/filesystem
4. Research advanced techniques for filling NaN values
5. Optimise the code further and use Dask inplace of pandas to increase performance