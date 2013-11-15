rm -r dump
mkdir dump
mkdir dump/level1
mkdir dump/level2
mkdir dump/level3

WORKTAR=$1
FILENAME=$(echo $WORKTAR | sed 's/.*\///g')
FOLDER=$(echo $FILENAME | sed 's/\..*//g')

wget $WORKTAR
tar xvf $FILENAME

hadoop fs -mkdir /user/hadoop/
hadoop fs -put $FOLDER .

# LEVEL 1
mahout canopy -i $FOLDER/vectors -o $FOLDER/canopy -t1 20 -t2 10 -xm mapreduce -ow -cl
mahout kmeans -i $FOLDER/vectors -o $FOLDER/level1 -c $FOLDER/canopy/clusters-0-final -x 5 -xm mapreduce -ow -cl
mahout clusterdump -s $FOLDER/level1/clusters-*-final/ -p $FOLDER/canopy/clusteredPoints -o dump/level1/root

# LEVEL 2
hadoop fs -mkdir $FOLDER/level2
mahout clusterpp -i $FOLDER/level1 -o $FOLDER/level2/data -xm sequential

rm -r data
hadoop fs -get $FOLDER/level2/data .
for x in `ls data | grep -v SUCCESS`; do
  echo
	mahout canopy -i $FOLDER/level2/data/$x -o $FOLDER/level2/canopy/$x -t1 10 -t2 5 -xm mapreduce -ow -cl
  mahout kmeans -i $FOLDER/level2/data/$x -o $FOLDER/level2/$x -c $FOLDER/level2/canopy/$x/clusters-0-final -x 1 -xm mapreduce -ow -cl
done
rm -r data

# LEVEL 3
hadoop fs -mkdir $FOLDER/level3
rm -r level2
hadoop fs -get $FOLDER/level2 .
for x in `ls level2/ | grep -v data | grep -v canopy`; do 
  echo

  mahout clusterdump -s $FOLDER/level2/$x/clusters-*-final/ -p $FOLDER/level2/canopy/$x/clusteredPoints -o dump/level2/$x
  mahout clusterpp -i $FOLDER/level2/$x -o $FOLDER/level3/data/$x -xm sequential

done
rm -r level2

rm -r data
hadoop fs -get $FOLDER/level3/data .
for x in `ls data`; do
  echo
	for y in `ls data/$x | grep -v SUCCESS`; do

	  mahout canopy -i $FOLDER/level3/data/$x/$y -o $FOLDER/level3/canopy/$x-$y -t1 5 -t2 2 -xm mapreduce -ow -cl
	  mahout kmeans -i $FOLDER/level3/data/$x/$y -o $FOLDER/level3/$x-$y -c $FOLDER/level3/canopy/$x-$y/clusters-0-final -x 1 -xm mapreduce -ow -cl;
  done
done
rm -r data

rm -r level3
hadoop fs -get $FOLDER/level3/ .
for x in `ls level3/ | grep -v data | grep -v canopy`; do

  mahout clusterdump -s $FOLDER/level3/$x/clusters-*-final/ -p $FOLDER/level3/canopy/$x/clusteredPoints -o dump/level3/$x

done
rm -r level3
