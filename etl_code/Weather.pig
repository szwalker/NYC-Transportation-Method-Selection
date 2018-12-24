Weather_input = LOAD '/user/yy1316/Project/Weather.csv' USING PigStorage(',') AS (Station:chararray, Name1:chararray, Name2:chararray, Date:chararray, AWND:float, PRCP:float, SNOW:float, SNWD:float, TMAX:float, TMIN:float);
Cleaned_input = FOREACH Weather_input GENERATE Date,PRCP,SNOW;
Final_input = FILTER Cleaned_input BY Date IS NOT NULL AND PRCP IS NOT NULL AND SNOW IS NOT NULL;
STORE Final_input INTO '/user/yy1316/Project/Result/cleaned_weather';
--author: Yunhan Yang

