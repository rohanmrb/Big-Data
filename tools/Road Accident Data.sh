# A file can be loaded into  HDFS  using the following command.
hdfs dfs -put path_to_file /hdfs_directory_path

# Count_Per_Hour
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
-mapper "'src/Road Accident Data/Count_Per_Hour/mapper.py'" \
-reducer "'src/Road Accident Data/Count_Per_Hour/reducer.py'" \
-input path_to_input_folder_on_hdfs \
-output path_to_output_folder_on_hdfs 


# Count_Per_City_State
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
-mapper "'src/Road Accident Data/Count_Per_City_State/mapper.py'" \
-reducer "'src/Road Accident Data/Count_Per_City_State/reducer.py'" \
-input path_to_input_folder_on_hdfs \
-output path_to_output_folder_on_hdfs 