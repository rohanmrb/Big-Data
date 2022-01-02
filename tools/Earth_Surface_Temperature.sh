# To run City_avg_temp.py
# path to python file , command like arguement country , path to csv
sudo /opt/spark/bin/spark-submit\
/home/pes1ug19cs171/Spark-Streaming-for-Machine-Learning/src/Earth_Surface_Temperature/City_Avg_Temp.py \
India \
/home/pes1ug19cs171/Spark-Streaming-for-Machine-Learning/data/Earth_Surface_Temperature/city_sample_5percent.csv


# To run Country_Max_temp.py
# path to python file , path to city sample csv, path to Global.csv
sudo /opt/spark/bin/spark-submit \
/home/pes1ug19cs171/Spark-Streaming-for-Machine-Learning/src/Earth_Surface_Temperature/Country_Max_Temp.py \
/home/pes1ug19cs171/Spark-Streaming-for-Machine-Learning/data/Earth_Surface_Temperature/city_sample_5percent.csv \
/home/pes1ug19cs171/Spark-Streaming-for-Machine-Learning/data/Earth_Surface_Temperature/global.csv